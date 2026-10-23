"""Persistent user-scoped session affinity and account selection for the queue."""
import copy
import hashlib
import json
import re
import time

DEFAULT_CONFIG = {'enabled': False, 'session_ttl_hours': 12, 'overflow_threshold': 3,
                  'accounts': [], 'preferences': {}}


def session_identity(headers, body):
    """Only use explicit client session identity; never bind an entire API key."""
    headers = {k.lower(): v for k, v in headers.items()}
    for name in ('session_id', 'session-id', 'x-session-id', 'x-codex-session-id',
                 'x-claude-code-session-id', 'x-opencode-session-id', 'x-opencode-session', 'x-session-key'):
        value = headers.get(name)
        if isinstance(value, str) and value.strip():
            return hashlib.sha256(value.strip().encode()).hexdigest()
    try:
        value = json.loads(body) if body else {}
        if not isinstance(value, dict):
            return ''
        metadata = value.get('metadata') if isinstance(value.get('metadata'), dict) else {}
        conversation = value.get('conversation')
        if isinstance(conversation, dict):
            conversation = conversation.get('id')
        claude = metadata.get('user_id')
        claude_session = ''
        if isinstance(claude, str):
            try:
                identity = json.loads(claude)
                if isinstance(identity, dict): claude_session = identity.get('session_id', '')
            except ValueError:
                match = re.search(r'_session_([^\s]+)$', claude)
                if match: claude_session = match.group(1)
        for item in (value.get('prompt_cache_key'), conversation, value.get('session_id'), value.get('sessionId'), metadata.get('session_id'), metadata.get('sessionId'), claude_session):
            if isinstance(item, str) and item.strip():
                return hashlib.sha256(item.strip().encode()).hexdigest()
    except (ValueError, TypeError):
        pass
    return ''


def validate_config(value):
    if not isinstance(value, dict):
        raise ValueError('配置必须是对象')
    result = copy.deepcopy(DEFAULT_CONFIG)
    result.update({k: value[k] for k in result if k in value})
    if type(result['enabled']) is not bool:
        raise ValueError('账号调度开关必须是布尔值')
    ttl = result['session_ttl_hours']
    if type(ttl) not in (int, float) or not 0 < ttl <= 720:
        raise ValueError('会话保持时间须大于 0 且不超过 720 小时')
    threshold = result['overflow_threshold']
    if type(threshold) is not int or not 0 <= threshold <= 100000:
        raise ValueError('溢出阈值须为非负整数')
    if not isinstance(result['accounts'], list) or len(result['accounts']) > 1000:
        raise ValueError('账号列表无效')
    seen = set()
    accounts = []
    for item in result['accounts']:
        if not isinstance(item, dict):
            raise ValueError('账号格式无效')
        identity = item.get('id')
        if not isinstance(identity, str) or not identity or len(identity) > 512 or identity in seen:
            raise ValueError('账号 ID 为空或重复')
        seen.add(identity)
        concurrency = item.get('concurrency', 1)
        if type(concurrency) is not int or not 1 <= concurrency <= 100000:
            raise ValueError('账号并发须为正整数')
        enabled = item.get('enabled', True)
        if type(enabled) is not bool:
            raise ValueError('账号启用状态无效')
        models = item.get('models', [])
        if not isinstance(models, list) or not all(isinstance(x, str) and 0 < len(x) <= 256 for x in models):
            raise ValueError('账号模型列表无效')
        name = item.get('name', identity)
        if not isinstance(name, str) or len(name) > 512:
            raise ValueError('账号名称无效')
        accounts.append({'id': identity, 'name': name, 'concurrency': concurrency,
                         'enabled': enabled, 'models': models})
    preferences = result['preferences']
    if not isinstance(preferences, dict) or len(preferences) > 10000:
        raise ValueError('用户优先账号格式无效')
    for user, ids in preferences.items():
        if not isinstance(user, str) or len(user) != 64 or not isinstance(ids, list) or any(not isinstance(i, str) or i not in seen for i in ids) or len(set(ids)) != len(ids):
            raise ValueError('用户优先账号不存在或重复')
    if result['enabled'] and not any(a['enabled'] for a in accounts):
        raise ValueError('启用调度前须配置可用账号')
    result['accounts'] = accounts
    return result


class RoutingUnavailable(Exception):
    pass


class AccountRouter:
    """All scheduling methods run under ConcurrencyQueue.condition."""
    def __init__(self, db_factory, clock=time.time):
        self.db = db_factory
        self.clock = clock
        self.unavailable = set()
        c = self.db()
        try:
            c.execute('CREATE TABLE IF NOT EXISTS routing_sessions (user_hash TEXT NOT NULL, session_hash TEXT NOT NULL, account_id TEXT NOT NULL, touched_at REAL NOT NULL, PRIMARY KEY(user_hash,session_hash))')
            c.execute('CREATE INDEX IF NOT EXISTS routing_sessions_expiry ON routing_sessions(touched_at)')
            row = c.execute("SELECT value FROM settings WHERE key='account_routing'").fetchone()
            self.config = validate_config(json.loads(row['value'])) if row else copy.deepcopy(DEFAULT_CONFIG)
        finally:
            c.close()

    def save(self, config):
        config = validate_config(config)
        c = self.db()
        try:
            c.execute("INSERT OR REPLACE INTO settings VALUES('account_routing',?)", (json.dumps(config),))
        finally:
            c.close()
        self.config = config

    def account(self, identity):
        return next((a for a in self.config['accounts'] if a['id'] == identity), None)

    def ready(self, identity, active):
        if not identity:
            return True
        account = self.account(identity)
        return bool(account and account['enabled'] and identity not in self.unavailable and active.get(identity, 0) < account['concurrency'])

    def choose(self, user, session, model, active, waiting):
        if not self.config['enabled']:
            return ''
        candidates = [a for a in self.config['accounts'] if a['enabled'] and a['id'] not in self.unavailable and (not a['models'] or model in a['models'] or model.split('(')[0] in a['models'])]
        if not candidates:
            raise RoutingUnavailable('没有可用且支持此模型的认证账号')
        ids = {a['id'] for a in candidates}
        c = self.db()
        try:
            now = self.clock()
            c.execute('DELETE FROM routing_sessions WHERE touched_at<=?', (now-self.config['session_ttl_hours']*3600,))
            bound = c.execute('SELECT account_id FROM routing_sessions WHERE user_hash=? AND session_hash=?', (user, session)).fetchone() if session else None
            # A live binding never spills merely because its account is busy.
            if bound and bound['account_id'] in ids:
                chosen = bound['account_id']
            else:
                preferred = [i for i in self.config['preferences'].get(user, []) if i in ids]
                rank = lambda a: (active.get(a['id'], 0) + waiting.get(a['id'], 0), a['id'])
                chosen = preferred[0] if preferred else min(candidates, key=rank)['id']
                account = next(a for a in candidates if a['id'] == chosen)
                # Spill only on backlog, and only to an account with spare capacity.
                if active.get(chosen, 0) >= account['concurrency'] and waiting.get(chosen, 0) >= self.config['overflow_threshold']:
                    spare = [a for a in candidates if a['id'] != chosen and active.get(a['id'], 0) < a['concurrency'] and not waiting.get(a['id'], 0)]
                    if spare:
                        chosen = min(spare, key=rank)['id']
            if session:
                c.execute('INSERT OR REPLACE INTO routing_sessions VALUES(?,?,?,?)', (user, session, chosen, now))
            return chosen
        finally:
            c.close()

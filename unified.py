"""Same-origin Keeper console and Gateway administration bridge."""
import copy
import retention
import hashlib
import json
import mimetypes
import threading
import time
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

from routing import RoutingUnavailable, validate_config

_refresh_lock = threading.Lock()
_checked = (None, 0.0)


def namespace(values):
    return SimpleNamespace(**values)


def enabled(g):
    return g.env('UNIFIED_CONSOLE', '0') == '1'


def keeper_headers(h):
    allowed = ('cookie', 'content-type', 'x-cpa-usage-keeper-request', 'x-cpa-usage-keeper-session', 'origin')
    result = {k: v for k, v in h.headers.items() if k.lower() in allowed}
    peer = h.client_address[0]
    result['X-Forwarded-For'] = h.headers.get('X-Forwarded-For', peer) if peer in ('127.0.0.1', '::1') else peer
    if peer in ('127.0.0.1', '::1') and h.headers.get('X-Forwarded-Proto') == 'https':
        result['X-Forwarded-Proto'] = 'https'
    return result


def session_admin(h, g):
    if not enabled(g) or not h.headers.get('Cookie'):
        return False
    try:
        r = g.requests.get(g.KEEPER_URL+'/api/v1/auth/session', headers=keeper_headers(h), timeout=5, allow_redirects=False)
        data = r.json()
        return r.status_code == 200 and data.get('authenticated') is True and data.get('role') == 'admin'
    except Exception:
        return False


def public_route(h, g):
    if not enabled(g):
        return False
    path = urlsplit(h.path).path
    if path.startswith('/api/v1/'):
        # Removed features cannot be called through the integrated entry point.
        if (path.startswith('/api/v1/ranking/') and not path.startswith('/api/v1/ranking/local/')) or path.startswith('/api/v1/auth/sessions') or path.startswith('/api/v1/update'):
            g.json_response(h, 404, {'error': 'not found'})
            return True
        try:
            n = int(h.headers.get('Content-Length', '0'))
            if not 0 <= n <= 4*1024*1024 or h.headers.get('Transfer-Encoding'):
                raise ValueError('invalid body size')
            body = h.rfile.read(n) if n else None
            r = g.requests.request(h.command, g.KEEPER_URL+h.path, data=body,
                                   headers=keeper_headers(h), timeout=60, allow_redirects=False)
            h.send_response(r.status_code)
            for key, val in r.headers.items():
                if key.lower() not in ('connection', 'transfer-encoding', 'content-length', 'content-encoding', 'set-cookie'):
                    h.send_header(key, val)
            for cookie in r.raw.headers.getlist('Set-Cookie'):
                h.send_header('Set-Cookie', cookie)
            h.send_header('Content-Length', str(len(r.content)))
            h.send_header('Cache-Control', 'no-store')
            h.end_headers()
            h.wfile.write(r.content)
        except ValueError:
            g.json_response(h, 400, {'error': 'invalid body size'})
        except Exception:
            g.json_response(h, 502, {'error': 'Keeper 服务暂时不可用'})
        return True
    if h.command == 'GET' and (path in ('/', '/admin', '/admin/', '/key-overview') or path.startswith('/assets/')):
        root = Path(g.env('CONSOLE_DIST', str(g.BASE_DIR/'console'))).resolve()
        file = (root/unquote(path).lstrip('/')).resolve() if path.startswith('/assets/') else root/'index.html'
        if not file.is_relative_to(root) or not file.is_file():
            g.json_response(h, 404, {'error': 'console asset not found'})
            return True
        content = file.read_bytes()
        if file.name == 'index.html':
            content = content.replace(b'"__APP_BASE_PATH__"', b'""').replace(b'./assets/', b'/assets/')
        h.send_response(200)
        h.send_header('Content-Type', mimetypes.guess_type(str(file))[0] or 'application/octet-stream')
        h.send_header('Content-Length', str(len(content)))
        h.send_header('Cache-Control', 'no-cache')
        h.end_headers()
        h.wfile.write(content)
        return True
    return False


def cpa_accounts(g):
    r = g.requests.get(g.UPSTREAM+'/v0/management/auth-files',
                       headers={'Authorization': 'Bearer '+g.CPA_MANAGEMENT_KEY}, timeout=10)
    r.raise_for_status()
    result = []
    for item in r.json().get('files', []):
        identity = str(item.get('id') or '')
        if not identity:
            continue
        result.append({'id': identity, 'name': str(item.get('name') or item.get('label') or identity),
                       'provider': str(item.get('provider') or item.get('type') or ''),
                       'available': not bool(item.get('disabled') or item.get('unavailable'))})
    return result


def capability(g):
    token = g.env('QUOTA_ROUTING_TOKEN')
    if not token:
        raise ValueError('请先配置 CPA 与 Gateway 共用的 QUOTA_ROUTING_TOKEN')
    r = g.requests.get(g.UPSTREAM+'/quota-gateway/capabilities',
                       headers={'X-Quota-Routing-Token': token}, timeout=5)
    if r.status_code != 200 or r.json().get('credential_pinning') is not True:
        raise ValueError('CPA 尚未启用可信账号指定接口，不能开启账号调度')


def ensure_routing_ready(g):
    global _checked
    router = g.UPSTREAM_QUEUE.router
    if not router or not router.config['enabled']:
        return
    with _refresh_lock:
        if _checked[0] is router and time.monotonic()-_checked[1] < 10:
            return
        try:
            capability(g)
            accounts = cpa_accounts(g)
            available = {a['id'] for a in accounts if a['available']}
            with g.UPSTREAM_QUEUE.condition:
                router.unavailable = {a['id'] for a in router.config['accounts']} - available
                g.UPSTREAM_QUEUE.condition.notify_all()
            _checked = (router, time.monotonic())
        except Exception as exc:
            raise RoutingUnavailable('认证账号状态检查失败，未向未知账号转发请求') from exc


def payload(h):
    size = int(h.headers.get('Content-Length', '0'))
    if not 0 < size <= 1024*1024:
        raise ValueError('invalid body size')
    value = json.loads(h.rfile.read(size))
    if not isinstance(value, dict):
        raise ValueError('expected an object')
    return value


def admin_route(h, g, path):
    if path not in ('/admin/queue', '/admin/routing', '/admin/accounts', '/admin/key-links', '/admin/preference', '/admin/history-retention'):
        return False
    if h.command not in ('GET', 'PUT') or (h.command == 'PUT' and path not in ('/admin/routing', '/admin/preference', '/admin/history-retention')):
        g.json_response(h, 405, {'error': 'method not allowed'})
        return True
    router = g.UPSTREAM_QUEUE.router
    try:
        if path == '/admin/history-retention':
            with closing(g.db()) as conn:
                if h.command == 'PUT':
                    retention.save_days(conn, payload(h).get('days'))
                g.json_response(h, 200, retention.status(conn))
        elif path == '/admin/queue':
            g.json_response(h, 200, g.UPSTREAM_QUEUE.detailed_snapshot())
        elif path == '/admin/accounts':
            g.json_response(h, 200, {'accounts': cpa_accounts(g)})
        elif path == '/admin/key-links':
            # Only admins receive the ID->hash association; no API key is returned.
            headers = keeper_headers(h)
            r = g.requests.get(g.KEEPER_URL+'/api/v1/usage/api-keys/settings', headers=headers, timeout=10)
            r.raise_for_status()
            g.json_response(h, 200, {'items': [{'id': item['id'], 'key_hash': hashlib.sha256(item['apiKey'].encode()).hexdigest()} for item in r.json().get('items', [])]})
        elif path == '/admin/preference':
            if h.command != 'PUT':
                g.json_response(h, 405, {'error': 'method not allowed'})
                return True
            value = payload(h)
            c = g.db()
            try:
                if not c.execute('SELECT 1 FROM policies WHERE key_hash=?', (value.get('key_hash'),)).fetchone():
                    raise ValueError('用户不存在')
            finally:
                c.close()
            with g.UPSTREAM_QUEUE.condition:
                config = copy.deepcopy(router.config)
                config['preferences'][value['key_hash']] = value.get('accounts')
                router.save(config)
                g.UPSTREAM_QUEUE.condition.notify_all()
            g.json_response(h, 200, {'ok': True})
        elif h.command == 'GET':
            with g.UPSTREAM_QUEUE.condition:
                config = copy.deepcopy(router.config)
            g.json_response(h, 200, config)
        else:
            config = validate_config(payload(h))
            if config['enabled']:
                capability(g)
                accounts = {a['id'] for a in cpa_accounts(g)}
                if any(a['id'] not in accounts for a in config['accounts'] if a['enabled']):
                    raise ValueError('配置中的认证文件已不存在，请刷新认证文件列表')
            with g.UPSTREAM_QUEUE.condition:
                # Preferences are edited separately; a stale account form must not overwrite them.
                account_ids = {a['id'] for a in config['accounts']}
                config['preferences'] = {user: [i for i in ids if i in account_ids] for user, ids in router.config['preferences'].items()}
                router.save(config)
                g.UPSTREAM_QUEUE.condition.notify_all()
            g.json_response(h, 200, config)
    except (ValueError, TypeError, KeyError) as exc:
        g.json_response(h, 400, {'error': str(exc)})
    except Exception:
        g.json_response(h, 502, {'error': '关联服务暂时不可用，设置未完成'})
    return True

"""Only user-authored text is retained; full request bodies are never persisted.

Prompt text is gzip-compressed and content-addressed across conversation history.
Requests store ordered references, while token accounting stays in usage_events.
"""
import gzip
import hashlib
import json
import uuid
from contextlib import closing
from datetime import datetime, timezone

from routing import session_identity

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_prompts (
 id TEXT PRIMARY KEY, text_gzip BLOB NOT NULL, original_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS request_payloads (
 id TEXT PRIMARY KEY, body_gzip BLOB NOT NULL, original_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS request_records (
 id TEXT PRIMARY KEY, key_hash TEXT NOT NULL, user_alias TEXT NOT NULL,
 received_at TEXT NOT NULL, model TEXT NOT NULL, endpoint TEXT NOT NULL,
 payload_id TEXT NOT NULL, http_status INTEGER, outcome TEXT NOT NULL DEFAULT 'pending',
 finished_at TEXT
);
CREATE INDEX IF NOT EXISTS request_records_time ON request_records(received_at);
CREATE TABLE IF NOT EXISTS request_attempts (
 request_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, started_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS request_attempts_record ON request_attempts(record_id);
"""

# The view deliberately leaves token counts NULL until CPA has reported usage.
HISTORY_VIEW = """
CREATE VIEW IF NOT EXISTS request_history AS
SELECT r.id, r.received_at, r.key_hash, r.user_alias, r.model, r.endpoint,
 r.conversation_hash, r.payload_id, r.http_status, r.outcome, r.finished_at,
 (SELECT count(*) FROM request_attempts a WHERE a.record_id=r.id) AS attempts,
 sum(e.input_tokens) AS input_tokens, sum(e.output_tokens) AS output_tokens,
 sum(e.reasoning_tokens) AS reasoning_tokens,
 sum(e.cache_read_tokens) AS cache_read_tokens,
 sum(e.cache_creation_tokens) AS cache_creation_tokens,
 sum(e.total_tokens) AS total_tokens
FROM request_records r
LEFT JOIN request_attempts a ON a.record_id=r.id
LEFT JOIN request_usage e ON e.request_id=a.request_id
GROUP BY r.id;
"""


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def user_prompts(body):
    """Extract explicit user text, excluding images, system/assistant/tool content."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return []
    if not isinstance(data, dict):
        return []

    def text(content):
        if isinstance(content, str):
            return content
        if not isinstance(content, list):
            return ''
        return '\n'.join(part['text'] for part in content if isinstance(part, dict)
                         and part.get('type') in ('text', 'input_text')
                         and isinstance(part.get('text'), str))

    result = []
    messages = data.get('messages')
    if isinstance(messages, list):
        result = [text(m.get('content')) for m in messages if isinstance(m, dict) and m.get('role') == 'user']
    elif isinstance(data.get('input'), str):
        result = [data['input']]
    elif isinstance(data.get('input'), list):
        result = [text(m.get('content')) for m in data['input'] if isinstance(m, dict) and m.get('role') == 'user']
    elif isinstance(data.get('contents'), list):
        for message in data['contents']:
            if not isinstance(message, dict) or message.get('role') != 'user':
                continue
            parts = message.get('parts')
            if isinstance(parts, list):
                result.append('\n'.join(p['text'] for p in parts if isinstance(p, dict)
                                        and isinstance(p.get('text'), str) and not p.get('thought')))
    elif isinstance(data.get('prompt'), str):
        result = [data['prompt']]
    elif isinstance(data.get('prompt'), list):
        result = [p for p in data['prompt'] if isinstance(p, str)]
    return [prompt for prompt in result if prompt.strip()]


def store_prompts(conn, prompts):
    references = []
    for prompt in prompts:
        raw = prompt.encode('utf-8')
        identity = hashlib.sha256(raw).hexdigest()
        conn.execute('INSERT OR IGNORE INTO user_prompts VALUES(?,?,?)',
                     (identity, gzip.compress(raw, compresslevel=6, mtime=0), len(raw)))
        references.append(identity)
    raw = json.dumps({'user_prompt_ids': references}, separators=(',', ':')).encode()
    identity = hashlib.sha256(b'prompt_refs_v1\0' + raw).hexdigest()
    conn.execute('INSERT OR IGNORE INTO request_payloads(id,body_gzip,original_bytes,payload_kind) VALUES(?,?,?,?)',
                 (identity, gzip.compress(raw, compresslevel=6, mtime=0), len(raw), 'prompt_refs_v1'))
    return identity


class RequestStore:
    def __init__(self, connect):
        self.connect = connect

    def initialize(self):
        with closing(self.connect()) as conn:
            conn.executescript(SCHEMA)
            if 'payload_kind' not in [r[1] for r in conn.execute('PRAGMA table_info(request_payloads)')]:
                conn.execute("ALTER TABLE request_payloads ADD COLUMN payload_kind TEXT NOT NULL DEFAULT 'request_body'")
            if 'conversation_hash' not in [r[1] for r in conn.execute('PRAGMA table_info(request_records)')]:
                conn.execute("ALTER TABLE request_records ADD COLUMN conversation_hash TEXT NOT NULL DEFAULT ''")
            conn.execute('CREATE INDEX IF NOT EXISTS request_records_conversation ON request_records(conversation_hash)')
            # Conversion is idempotent and can resume after interruption. No full body remains.
            legacy = [r[0] for r in conn.execute("SELECT id FROM request_payloads WHERE payload_kind='request_body'")]
            for old_id in legacy:
                compressed = conn.execute('SELECT body_gzip FROM request_payloads WHERE id=?', (old_id,)).fetchone()[0]
                raw = gzip.decompress(compressed)
                prompts = user_prompts(raw)
                session = session_identity({}, raw)
                conn.execute('BEGIN IMMEDIATE')
                try:
                    new_id = store_prompts(conn, prompts)
                    for record_id, key_hash in conn.execute("SELECT id,key_hash FROM request_records WHERE payload_id=? AND conversation_hash=''", (old_id,)).fetchall():
                        conversation = hashlib.sha256((key_hash + '|' + (session or record_id)).encode()).hexdigest()
                        conn.execute('UPDATE request_records SET conversation_hash=? WHERE id=?', (conversation, record_id))
                    conn.execute('UPDATE request_records SET payload_id=? WHERE payload_id=?', (new_id, old_id))
                    conn.execute('DELETE FROM request_payloads WHERE id=?', (old_id,))
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='usage_events'").fetchone():
                conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_events_request_id ON usage_events(request_id)')
                columns = 'request_id,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_creation_tokens,total_tokens'
                usage = 'SELECT ' + columns + ' FROM usage_events'
                if conn.execute("SELECT 1 FROM sqlite_master WHERE name='usage_events_archive'").fetchone():
                    conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_events_archive_request_id ON usage_events_archive(request_id)')
                    usage += ' UNION ALL SELECT ' + columns + ' FROM usage_events_archive'
                conn.execute('CREATE VIEW IF NOT EXISTS request_usage AS ' + usage)
                conn.execute('DROP VIEW IF EXISTS request_history')
                conn.executescript(HISTORY_VIEW)

    def begin(self, key_hash, alias, model, endpoint, body, session_hash=''):
        identity = str(uuid.uuid4())
        conversation = hashlib.sha256((key_hash + '|' + (session_hash or identity)).encode()).hexdigest()
        prompts = user_prompts(body)
        with closing(self.connect()) as conn:
            conn.execute('BEGIN IMMEDIATE')
            try:
                payload_id = store_prompts(conn, prompts)
                conn.execute('INSERT INTO request_records(id,key_hash,user_alias,received_at,model,endpoint,payload_id,conversation_hash) VALUES(?,?,?,?,?,?,?,?)',
                             (identity, key_hash, alias, stamp(), model, endpoint, payload_id, conversation))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return identity

    def attempt(self, identity):
        request_id = str(uuid.uuid4())
        with closing(self.connect()) as conn:
            conn.execute('INSERT INTO request_attempts VALUES(?,?,?)', (request_id, identity, stamp()))
        return request_id

    def finish(self, identity, status, outcome):
        with closing(self.connect()) as conn:
            conn.execute('UPDATE request_records SET http_status=?,outcome=?,finished_at=? WHERE id=?',
                         (status, outcome, stamp(), identity))

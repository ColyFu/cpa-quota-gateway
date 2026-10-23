"""Only candidate user text is retained; full request bodies are never persisted.

Prompt text is gzip-compressed and content-addressed across conversation history.
Requests reference only newly observed input, with explicit inference status.
Token accounting stays in usage_events.
"""
import gzip
import hashlib
import json
import uuid
from contextlib import closing
from datetime import datetime, timezone

from routing import session_identity
from prompt_detection import clean_user_text, detect_input, inspect_input, prompt_ids

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
CREATE TABLE IF NOT EXISTS prompt_conversation_state (
 conversation_hash TEXT PRIMARY KEY, state_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS request_attempts (
 request_id TEXT PRIMARY KEY, record_id TEXT NOT NULL, started_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS request_attempts_record ON request_attempts(record_id);
"""

# The view deliberately leaves token counts NULL until CPA has reported usage.
HISTORY_VIEW = """
CREATE VIEW IF NOT EXISTS request_history AS
SELECT r.id, r.received_at, r.key_hash, r.user_alias, r.model, r.endpoint,
 r.conversation_hash, r.input_state, r.input_detail, r.new_input_count, r.payload_id, r.http_status, r.outcome, r.finished_at,
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
    return inspect_input(body)['prompts']


def store_texts(conn, prompts):
    references = prompt_ids(prompts)
    for identity, prompt in zip(references, prompts):
        raw = prompt.encode('utf-8')
        conn.execute('INSERT OR IGNORE INTO user_prompts VALUES(?,?,?)',
                     (identity, gzip.compress(raw, compresslevel=6, mtime=0), len(raw)))
    return references


def store_references(conn, references):
    raw = json.dumps({'user_prompt_ids': references}, separators=(',', ':')).encode()
    identity = hashlib.sha256(b'prompt_refs_v2\0' + raw).hexdigest()
    conn.execute('INSERT OR IGNORE INTO request_payloads(id,body_gzip,original_bytes,payload_kind) VALUES(?,?,?,?)',
                 (identity, gzip.compress(raw, compresslevel=6, mtime=0), len(raw), 'prompt_refs_v2'))
    return identity


def load_state(conn, conversation):
    row = conn.execute('SELECT state_json FROM prompt_conversation_state WHERE conversation_hash=?', (conversation,)).fetchone()
    return json.loads(row[0]) if row else None


def save_state(conn, conversation, state):
    conn.execute('INSERT INTO prompt_conversation_state VALUES(?,?) ON CONFLICT(conversation_hash) DO UPDATE SET state_json=excluded.state_json',
                 (conversation, json.dumps(state, separators=(',', ':'))))


def migrate_inputs(conn):
    """Reclassify legacy history without claiming to know lost turn boundaries."""
    rows = conn.execute("""SELECT r.id,r.key_hash,r.conversation_hash,p.body_gzip,p.payload_kind
        FROM request_records r JOIN request_payloads p ON p.id=r.payload_id
        WHERE r.input_state='legacy' OR p.payload_kind!='prompt_refs_v2'
        ORDER BY r.received_at,r.id""").fetchall()
    if not rows:
        return
    conn.execute('BEGIN IMMEDIATE')
    try:
        for record_id, key_hash, conversation, compressed, kind in rows:
            raw = gzip.decompress(compressed)
            session = ''
            if kind == 'request_body':
                prompts = user_prompts(raw)
                session = session_identity({}, raw)
            else:
                prompts = []
                for identity in json.loads(raw)['user_prompt_ids']:
                    stored = conn.execute('SELECT text_gzip FROM user_prompts WHERE id=?', (identity,)).fetchone()
                    if stored is None:
                        raise ValueError('Legacy prompt reference is missing')
                    prompts.extend(clean_user_text(gzip.decompress(stored[0]).decode('utf-8')))
            conversation = conversation or hashlib.sha256((key_hash + '|' + (session or record_id)).encode()).hexdigest()
            ids = store_texts(conn, prompts)
            refs, status, detail, state = detect_input(load_state(conn, conversation), ids, {}, True, historical=True)
            payload_id = store_references(conn, refs)
            save_state(conn, conversation, state)
            conn.execute('UPDATE request_records SET payload_id=?,conversation_hash=?,input_state=?,input_detail=?,new_input_count=0 WHERE id=?',
                         (payload_id, conversation, status, detail, record_id))
        conn.execute('DELETE FROM request_payloads WHERE id NOT IN (SELECT payload_id FROM request_records)')
        # Baseline text remains part of conversation history; injected material is
        # removed. Only hashes, not extra text copies, are kept in comparison state.
        retained = set()
        for (raw,) in conn.execute('SELECT state_json FROM prompt_conversation_state'):
            retained.update(json.loads(raw)['seen'])
        for (compressed,) in conn.execute('SELECT body_gzip FROM request_payloads'):
            retained.update(json.loads(gzip.decompress(compressed))['user_prompt_ids'])
        conn.executemany('DELETE FROM user_prompts WHERE id=?',
                         [(row[0],) for row in conn.execute('SELECT id FROM user_prompts') if row[0] not in retained])
        conn.commit()
    except Exception:
        conn.rollback()
        raise


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
            columns = {r[1] for r in conn.execute('PRAGMA table_info(request_records)')}
            for name, definition in [('input_state', "TEXT NOT NULL DEFAULT 'legacy'"),
                                     ('input_detail', "TEXT NOT NULL DEFAULT ''"),
                                     ('new_input_count', 'INTEGER NOT NULL DEFAULT 0')]:
                if name not in columns:
                    conn.execute(f'ALTER TABLE request_records ADD COLUMN {name} {definition}')
            migrate_inputs(conn)
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
        facts = inspect_input(body)
        with closing(self.connect()) as conn:
            conn.execute('BEGIN IMMEDIATE')
            try:
                ids = prompt_ids(facts['prompts'])
                refs, status, detail, state = detect_input(load_state(conn, conversation), ids, facts, bool(session_hash))
                retained = set(refs)
                store_texts(conn, [text for identity, text in zip(ids, facts['prompts']) if identity in retained])
                payload_id = store_references(conn, refs)
                save_state(conn, conversation, state)
                conn.execute('INSERT INTO request_records(id,key_hash,user_alias,received_at,model,endpoint,payload_id,conversation_hash,input_state,input_detail,new_input_count) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                             (identity, key_hash, alias, stamp(), model, endpoint, payload_id, conversation,
                              status, detail, len(refs) if status == 'inferred_input' else 0))
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

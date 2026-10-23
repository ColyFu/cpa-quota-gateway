"""Bounded request history; daily statistics replace fully processed old details."""
from contextlib import closing
from datetime import datetime, timedelta
import gzip
import json
import logging
import os
import threading
from zoneinfo import ZoneInfo

DEFAULT_DAYS = 60
SETTING = 'request_history_retention_days'
WAKE = threading.Event()
LOCK = threading.Lock()
METRICS = ('request_count', 'success_count', 'failure_count', 'input_tokens', 'output_tokens',
           'reasoning_tokens', 'cache_read_tokens', 'cache_creation_tokens', 'total_tokens',
           'latency_sum_ms', 'latency_samples', 'ttft_sum_ms', 'ttft_samples',
           'speed_output_tokens', 'speed_elapsed_ms', 'speed_samples', 'cache_rate_sum', 'cache_rate_samples')
SCHEMA = '''
CREATE TABLE IF NOT EXISTS request_daily_stats (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 day TEXT NOT NULL, bucket_start TEXT NOT NULL, api_group_key TEXT NOT NULL, model TEXT NOT NULL,
 request_count INTEGER NOT NULL DEFAULT 0, success_count INTEGER NOT NULL DEFAULT 0,
 failure_count INTEGER NOT NULL DEFAULT 0, input_tokens INTEGER NOT NULL DEFAULT 0,
 output_tokens INTEGER NOT NULL DEFAULT 0, reasoning_tokens INTEGER NOT NULL DEFAULT 0,
 cache_read_tokens INTEGER NOT NULL DEFAULT 0, cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
 total_tokens INTEGER NOT NULL DEFAULT 0, latency_sum_ms INTEGER NOT NULL DEFAULT 0,
 latency_samples INTEGER NOT NULL DEFAULT 0, ttft_sum_ms INTEGER NOT NULL DEFAULT 0,
 ttft_samples INTEGER NOT NULL DEFAULT 0, speed_output_tokens INTEGER NOT NULL DEFAULT 0,
 speed_elapsed_ms INTEGER NOT NULL DEFAULT 0, speed_samples INTEGER NOT NULL DEFAULT 0,
 cache_rate_sum REAL NOT NULL DEFAULT 0, cache_rate_samples INTEGER NOT NULL DEFAULT 0,
 UNIQUE(day,api_group_key,model)
);
CREATE INDEX IF NOT EXISTS request_daily_key_day ON request_daily_stats(api_group_key,day);
'''


def initialize(conn):
    conn.executescript(SCHEMA)
    conn.execute('INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)', (SETTING, str(DEFAULT_DAYS)))


def retention_days(conn):
    row = conn.execute('SELECT value FROM settings WHERE key=?', (SETTING,)).fetchone()
    value = int(row[0]) if row else DEFAULT_DAYS
    if not 1 <= value <= 3650:
        raise ValueError('历史请求保存时间必须是 1 到 3650 天的整数')
    return value


def save_days(conn, value):
    if type(value) is not int or not 1 <= value <= 3650:
        raise ValueError('历史请求保存时间必须是 1 到 3650 天的整数')
    conn.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)', (SETTING, str(value)))
    WAKE.set()


def cutoff_at(days, now=None):
    zone = ZoneInfo(os.environ.get('TZ') or 'Asia/Shanghai')
    now = now.astimezone(zone) if now else datetime.now(zone)
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)


def status(conn):
    days = retention_days(conn)
    last = conn.execute("SELECT value FROM settings WHERE key='request_history_last_cleanup'").fetchone()
    return {'days': days, 'default_days': DEFAULT_DAYS, 'cutoff': cutoff_at(days).isoformat(),
            'daily_rows': conn.execute('SELECT count(*) FROM request_daily_stats').fetchone()[0],
            'last_cleanup': json.loads(last[0]) if last else None}


def _batch(conn, table, cutoff, size, days):
    """Summary upsert and source deletion share one transaction, including watermarks."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        if retention_days(conn) != days:
            conn.rollback()
            return 0, False
        ids = [r[0] for r in conn.execute(f'SELECT id FROM {table} WHERE timestamp<? ORDER BY id LIMIT ?', (cutoff, size))]
        if not ids:
            conn.rollback()
            return 0, False
        max_id = max(ids)
        # Every affected event must have reached all derived statistics and billing.
        checkpoints = dict(conn.execute('SELECT name,last_aggregated_usage_event_id FROM usage_aggregation_checkpoints'))
        billing = conn.execute("SELECT value FROM settings WHERE key='keeper_usage_event_id'").fetchone()
        if any(checkpoints.get(k, 0) < max_id for k in ('overview','activity','latency')) or not billing or int(billing[0]) < max_id:
            conn.rollback()
            return 0, True
        marks = ','.join('?' for _ in ids)
        pending = conn.execute(f'''SELECT 1 FROM usage_identities identity JOIN {table} event
            ON event.auth_index=identity.identity AND event.id>COALESCE(identity.last_aggregated_usage_event_id,0)
            AND ((identity.auth_type=1 AND event.auth_type='oauth') OR (identity.auth_type=2 AND event.auth_type='apikey'))
            WHERE event.id IN ({marks}) LIMIT 1''', ids).fetchone()
        if pending:
            conn.rollback()
            return 0, True
        valid_speed = 'failed=0 AND output_tokens>0 AND ttft_ms>0 AND latency_ms>ttft_ms'
        sums = [
            'COUNT(*)', 'SUM(CASE WHEN failed=0 THEN 1 ELSE 0 END)', 'SUM(CASE WHEN failed<>0 THEN 1 ELSE 0 END)',
            *[f'COALESCE(SUM({name}),0)' for name in METRICS[3:9]],
            'SUM(CASE WHEN latency_ms>0 THEN latency_ms ELSE 0 END)', 'SUM(CASE WHEN latency_ms>0 THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN ttft_ms>0 THEN ttft_ms ELSE 0 END)', 'SUM(CASE WHEN ttft_ms>0 THEN 1 ELSE 0 END)',
            f'SUM(CASE WHEN {valid_speed} THEN output_tokens ELSE 0 END)',
            f'SUM(CASE WHEN {valid_speed} THEN latency_ms-ttft_ms ELSE 0 END)',
            f'SUM(CASE WHEN {valid_speed} THEN 1 ELSE 0 END)',
            'SUM(CASE WHEN input_tokens>0 THEN 1.0*cache_read_tokens/input_tokens ELSE 0 END)',
            'SUM(CASE WHEN input_tokens>0 THEN 1 ELSE 0 END)',
        ]
        conn.create_function('history_day_start', 1, lambda value: datetime.fromisoformat(value).astimezone(ZoneInfo(os.environ.get('TZ') or 'Asia/Shanghai')).replace(hour=0, minute=0, second=0, microsecond=0).isoformat())
        conn.execute(f'''INSERT INTO request_daily_stats(day,bucket_start,api_group_key,model,{','.join(METRICS)})
            SELECT SUBSTR(timestamp,1,10),MIN(history_day_start(timestamp)),api_group_key,model,{','.join(sums)} FROM {table}
            WHERE id IN ({marks}) GROUP BY SUBSTR(timestamp,1,10),api_group_key,model
            ON CONFLICT(day,api_group_key,model) DO UPDATE SET
            {','.join(f'{name}={name}+excluded.{name}' for name in METRICS)}''', ids)
        removed = conn.execute(f'DELETE FROM {table} WHERE id IN ({marks})', ids).rowcount
        if removed != len(ids):
            raise RuntimeError('Request compaction count mismatch')
        conn.commit()
        return removed, False
    except Exception:
        conn.rollback()
        raise


def _prune_text(conn, cutoff, days):
    conn.execute('BEGIN IMMEDIATE')
    try:
        if retention_days(conn) != days:
            conn.rollback()
            return
        conn.execute('PRAGMA secure_delete=ON')
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='request_records'").fetchone():
            # Receipt timestamps are UTC, so compare instants, not mixed-offset strings.
            conn.execute("DELETE FROM request_attempts WHERE record_id IN (SELECT id FROM request_records WHERE julianday(received_at)<julianday(?) AND outcome!='pending')", (cutoff,))
            conn.execute("DELETE FROM request_records WHERE julianday(received_at)<julianday(?) AND outcome!='pending'", (cutoff,))
            conn.execute('DELETE FROM prompt_conversation_state WHERE conversation_hash NOT IN (SELECT conversation_hash FROM request_records)')
            conn.execute('DELETE FROM request_payloads WHERE id NOT IN (SELECT payload_id FROM request_records)')
            conn.execute('CREATE TEMP TABLE retained_prompt_ids(id TEXT PRIMARY KEY)')
            for (compressed,) in conn.execute('SELECT body_gzip FROM request_payloads'):
                refs = json.loads(gzip.decompress(compressed))['user_prompt_ids']
                conn.executemany('INSERT OR IGNORE INTO retained_prompt_ids VALUES(?)', [(v,) for v in refs])
            conn.execute('DELETE FROM user_prompts WHERE id NOT IN (SELECT id FROM retained_prompt_ids)')
            conn.execute('DROP TABLE retained_prompt_ids')
        conn.execute("DELETE FROM reservations WHERE state!='active' AND created_at<?", (int(datetime.fromisoformat(cutoff).timestamp()),))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def compact(connect, now=None, batch_size=500, stop=None):
    if not LOCK.acquire(blocking=False):
        return {'skipped': 'already_running'}
    try:
        with closing(connect()) as conn:
            days = retention_days(conn)
            cutoff = cutoff_at(days, now).isoformat()
            result = {'days': days, 'cutoff': cutoff, 'compacted': 0, 'waiting_for_sync': False}
            for table in ('usage_events','usage_events_archive'):
                while not (stop and stop.is_set()):
                    count, waiting = _batch(conn, table, cutoff, batch_size, days)
                    result['compacted'] += count
                    result['waiting_for_sync'] |= waiting
                    if not count:break
            if (stop and stop.is_set()) or retention_days(conn) != days:
                result['interrupted'] = True
                return result
            _prune_text(conn, cutoff, days)
            result['finished_at'] = datetime.now().astimezone().isoformat()
            conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('request_history_last_cleanup',?)", (json.dumps(result),))
            # Deleted pages are reused immediately. Full VACUUM is an offline
            # maintenance operation, never a surprise writer lock in live traffic.
            conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
            return result
    finally:
        LOCK.release()


def worker(connect, stop):
    # Let startup ingestion/settlement catch up. Subsequent passes retry safely.
    if stop.wait(30):return
    while not stop.is_set():
        try:
            result = compact(connect, stop=stop)
            if result.get('compacted'):
                logging.info('Request history compaction: %s', result)
        except Exception:
            logging.exception('Request history compaction failed; uncommitted batches were retained')
        WAKE.wait(3600)
        WAKE.clear()

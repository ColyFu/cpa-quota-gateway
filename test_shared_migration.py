import sqlite3
import tempfile
import unittest
from pathlib import Path

from migrate_shared_store import merge


class SharedMigrationTests(unittest.TestCase):
    def test_all_rows_indexes_and_sequence_survive(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, q, out = [Path(tmp)/name for name in ['analytics.db', 'quota.db', 'app.db']]
            with sqlite3.connect(a) as conn:
                conn.executescript("CREATE TABLE usage_events(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT); INSERT INTO usage_events VALUES(12,'same'); DELETE FROM usage_events; INSERT INTO usage_events(request_id) VALUES('new'); CREATE TABLE app_settings(key TEXT,value TEXT); INSERT INTO app_settings VALUES('alias','用户');")
            with sqlite3.connect(q) as conn:
                conn.executescript("CREATE TABLE policies(key_hash TEXT PRIMARY KEY, enabled INTEGER); INSERT INTO policies VALUES('hash',1); CREATE TABLE periods(key_hash TEXT,spent_micro INTEGER); INSERT INTO periods VALUES('hash',12345); CREATE INDEX period_key ON periods(key_hash);")
            merge(a,q,out)
            with sqlite3.connect(out) as conn:
                self.assertEqual(conn.execute('SELECT id,request_id FROM usage_events').fetchall(), [(13,'new')])
                conn.execute("INSERT INTO usage_events(request_id) VALUES('next')")
                self.assertEqual(conn.execute('SELECT max(id) FROM usage_events').fetchone()[0],14)
                self.assertEqual(conn.execute('SELECT spent_micro FROM periods').fetchone()[0],12345)
                self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master WHERE name='period_key'").fetchone()[0],1)
            with self.assertRaises(ValueError): merge(a,q,out)

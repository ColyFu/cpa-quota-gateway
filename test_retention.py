import gzip
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import retention
from request_store import RequestStore

NOW=datetime(2026,10,23,12,tzinfo=ZoneInfo('Asia/Shanghai'))
OLD='2026-08-23T18:00:00+08:00'
BOUNDARY='2026-08-24T00:00:00+08:00'

class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'app.db'
        self.connect=lambda:sqlite3.connect(self.path,isolation_level=None)
        with self.connect() as c:
            c.executescript('''CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE reservations(id TEXT PRIMARY KEY,state TEXT,created_at INTEGER);
            CREATE TABLE usage_events(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT,api_group_key TEXT,model TEXT,auth_index TEXT DEFAULT '',auth_type TEXT DEFAULT '',request_id TEXT DEFAULT '',failed INTEGER DEFAULT 0,latency_ms INTEGER DEFAULT 0,ttft_ms INTEGER,input_tokens INTEGER DEFAULT 0,output_tokens INTEGER DEFAULT 0,reasoning_tokens INTEGER DEFAULT 0,cache_read_tokens INTEGER DEFAULT 0,cache_creation_tokens INTEGER DEFAULT 0,total_tokens INTEGER DEFAULT 0);
            CREATE TABLE usage_events_archive AS SELECT * FROM usage_events;
            CREATE TABLE usage_aggregation_checkpoints(name TEXT PRIMARY KEY,last_aggregated_usage_event_id INTEGER);
            CREATE TABLE usage_identities(identity TEXT,auth_type INTEGER,last_aggregated_usage_event_id INTEGER);
            CREATE TABLE ledger(event_id TEXT,cost_micro INTEGER); INSERT INTO ledger VALUES('keep',777);
            CREATE TABLE periods(spent_micro INTEGER,reserved_micro INTEGER); INSERT INTO periods VALUES(777,10);
            INSERT INTO usage_aggregation_checkpoints VALUES('overview',10000),('activity',10000),('latency',10000);
            INSERT INTO settings VALUES('keeper_usage_event_id','10000');''')
            retention.initialize(c)
        self.store=RequestStore(self.connect);self.store.initialize()

    def tearDown(self):self.tmp.cleanup()

    def add(self, **overrides):
        values={'timestamp':OLD,'api_group_key':'A','model':'model-a','input_tokens':100,'output_tokens':100,'cache_read_tokens':10,'total_tokens':200,'latency_ms':2100,'ttft_ms':100}
        values.update(overrides)
        with self.connect() as c:
            return c.execute('INSERT INTO usage_events('+','.join(values)+') VALUES('+','.join('?' for _ in values)+')',list(values.values())).lastrowid

    def run_compact(self,**kwargs):return retention.compact(self.connect,now=NOW,**kwargs)

    def test_grouping_boundary_weighted_speed_and_mean_cache_rate(self):
        self.add();self.add(input_tokens=1000,output_tokens=900,cache_read_tokens=900,total_tokens=1900,latency_ms=9100)
        self.add(failed=1,input_tokens=0,output_tokens=100,cache_read_tokens=0,total_tokens=100,latency_ms=10)
        self.add(api_group_key='B');self.add(model='model-b')
        kept=self.add(timestamp=BOUNDARY)
        result=self.run_compact(batch_size=1)
        self.assertEqual(result['compacted'],5)
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT id FROM usage_events').fetchall(),[(kept,)])
            self.assertEqual(c.execute('SELECT count(*) FROM request_daily_stats').fetchone()[0],3)
            row=c.execute("SELECT request_count,success_count,failure_count,input_tokens,output_tokens,cache_read_tokens,latency_sum_ms,latency_samples,1000.0*speed_output_tokens/speed_elapsed_ms,cache_rate_sum/cache_rate_samples,bucket_start FROM request_daily_stats WHERE api_group_key='A' AND model='model-a'").fetchone()
            self.assertEqual(row[:8],(3,2,1,1100,1100,910,11210,3))
            self.assertAlmostEqual(row[8],1000/11);self.assertAlmostEqual(row[9],.5)
            self.assertEqual(row[10],'2026-08-23T00:00:00+08:00')
            self.assertEqual(c.execute('SELECT * FROM ledger').fetchall(),[('keep',777)])
            self.assertEqual(c.execute('SELECT * FROM periods').fetchall(),[(777,10)])
        self.assertEqual(self.run_compact()['compacted'],0)
        self.add()
        self.assertEqual(self.run_compact()['compacted'],1)
        with self.connect() as c:self.assertEqual(c.execute("SELECT request_count FROM request_daily_stats WHERE api_group_key='A' AND model='model-a'").fetchone()[0],4)

    def test_sync_guards_leave_details_until_all_consumers_have_processed_them(self):
        identity=self.add(auth_index='account',auth_type='oauth')
        with self.connect() as c:
            c.execute("UPDATE settings SET value='0' WHERE key='keeper_usage_event_id'")
        self.assertTrue(self.run_compact()['waiting_for_sync'])
        with self.connect() as c:
            c.execute("UPDATE settings SET value='10000' WHERE key='keeper_usage_event_id'")
            c.execute("UPDATE usage_aggregation_checkpoints SET last_aggregated_usage_event_id=0 WHERE name='latency'")
        self.assertTrue(self.run_compact()['waiting_for_sync'])
        with self.connect() as c:
            c.execute('UPDATE usage_aggregation_checkpoints SET last_aggregated_usage_event_id=10000')
            c.execute("INSERT INTO usage_identities VALUES('account',1,0)")
        self.assertTrue(self.run_compact()['waiting_for_sync'])
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM usage_events').fetchone()[0],1)
            c.execute('UPDATE usage_identities SET last_aggregated_usage_event_id=?',(identity,))
        self.assertEqual(self.run_compact()['compacted'],1)

    def test_atomic_rollback_and_existing_archive_compaction(self):
        self.add()
        with self.connect() as c:c.execute("CREATE TRIGGER fail_delete BEFORE DELETE ON usage_events BEGIN SELECT RAISE(ABORT,'test'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.run_compact()
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM request_daily_stats').fetchone()[0],0)
            c.execute('DROP TRIGGER fail_delete')
            c.execute('INSERT INTO usage_events_archive SELECT * FROM usage_events');c.execute('DELETE FROM usage_events')
        self.assertEqual(self.run_compact()['compacted'],1)
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM usage_events_archive').fetchone()[0],0)

    def test_expired_prompt_gc_does_not_retain_replayed_history(self):
        first=self.store.begin('hash','A','model-a','/v1/responses',b'{"input":"old text"}','session')
        self.store.attempt(first);self.store.finish(first,200,'completed')
        second=self.store.begin('hash','A','model-a','/v1/responses',b'{"input":[{"role":"user","content":"old text"},{"role":"user","content":"new text"}]}','session')
        self.store.finish(second,200,'completed')
        with self.connect() as c:
            c.execute('UPDATE request_records SET received_at=? WHERE id=?',(OLD,first))
            c.execute('UPDATE request_records SET received_at=? WHERE id=?',(BOUNDARY,second))
        self.run_compact()
        self.store.begin('hash','A','model-a','/v1/responses',b'{"input":[{"role":"user","content":"old text"},{"role":"user","content":"new text"},{"type":"function_call_output","output":"automatic"}]}','session')
        with self.connect() as c:
            self.assertEqual([gzip.decompress(r[0]) for r in c.execute('SELECT text_gzip FROM user_prompts')],[b'new text'])
            self.assertEqual(c.execute('SELECT count(*) FROM request_attempts WHERE record_id=?',(first,)).fetchone()[0],0)

    def test_settings_default_validation_and_increasing_days_does_not_recreate_details(self):
        with self.connect() as c:
            self.assertEqual(retention.status(c)['days'],60)
            for value in [True,0,-1,1.5,'60',3651,None]:
                with self.assertRaises(ValueError):retention.save_days(c,value)
            retention.save_days(c,30);self.assertEqual(retention.retention_days(c),30)
        self.add();self.run_compact()
        with self.connect() as c:retention.save_days(c,365)
        self.assertEqual(self.run_compact()['compacted'],0)
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM usage_events').fetchone()[0],0)

    def test_increasing_retention_during_cleanup_stops_further_deletion(self):
        from unittest import mock
        self.add();self.add()
        batch=retention._batch
        def change_after_batch(conn,*args):
            result=batch(conn,*args)
            if result[0]:retention.save_days(conn,365)
            return result
        with mock.patch.object(retention,'_batch',side_effect=change_after_batch):
            result=self.run_compact(batch_size=1)
        self.assertTrue(result['interrupted'])
        self.assertEqual(result['compacted'],1)
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM usage_events').fetchone()[0],1)

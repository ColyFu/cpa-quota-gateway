import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from request_store import RequestStore, user_prompts


class RequestStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'app.db')
        self.connect = lambda: sqlite3.connect(self.path, isolation_level=None)
        self.store = RequestStore(self.connect)
        with self.connect() as c:
            c.execute('CREATE TABLE usage_events(request_id TEXT,input_tokens INTEGER,output_tokens INTEGER,reasoning_tokens INTEGER,cache_read_tokens INTEGER,cache_creation_tokens INTEGER,total_tokens INTEGER)')
            c.execute('CREATE TABLE usage_events_archive AS SELECT * FROM usage_events')
        self.store.initialize()

    def tearDown(self):
        self.tmp.cleanup()

    def test_compression_deduplication_and_distinct_receipts(self):
        body = ('{"input":"你好 ' + 'context ' * 1000 + '"}').encode()
        first = self.store.begin('hash-a', '小林', 'model', '/v1/responses', body)
        second = self.store.begin('hash-b', '小周', 'model', '/v1/responses', body)
        self.assertNotEqual(first, second)
        with self.connect() as c:
            rows = c.execute('SELECT body_gzip,original_bytes FROM request_payloads').fetchall()
            self.assertEqual(len(rows), 1)
            refs = json.loads(gzip.decompress(rows[0][0]))['user_prompt_ids']
            prompt = c.execute('SELECT text_gzip FROM user_prompts WHERE id=?', (refs[0],)).fetchone()[0]
            self.assertEqual(gzip.decompress(prompt).decode(), json.loads(body)['input'])
            self.assertEqual(rows[0][1], len(gzip.decompress(rows[0][0])))
            self.assertLess(len(rows[0][0]), len(body) / 10)
            self.assertEqual(c.execute('SELECT count(*) FROM request_records').fetchone()[0], 2)

    def test_retry_usage_is_joined_without_fabricating_pending_tokens(self):
        record = self.store.begin('hash-a', 'A', 'm', '/v1/responses', b'{}')
        with self.connect() as c:
            self.assertIsNone(c.execute('SELECT input_tokens FROM request_history').fetchone()[0])
        first, second = self.store.attempt(record), self.store.attempt(record)
        self.assertNotEqual(first, second)
        with self.connect() as c:
            for request_id in [first, second]:
                c.execute('INSERT INTO usage_events VALUES(?,100,20,3,50,10,120)', (request_id,))
        self.store.finish(record, 200, 'completed')
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT attempts,input_tokens,output_tokens,cache_read_tokens,http_status,outcome FROM request_history').fetchone(), (2,200,40,100,200,'completed'))
            c.execute('INSERT INTO usage_events_archive SELECT * FROM usage_events')
            c.execute('DELETE FROM usage_events')
            self.assertEqual(c.execute('SELECT input_tokens,output_tokens FROM request_history').fetchone(), (200,40))

    def test_failed_receipt_insert_rolls_back_payload(self):
        with self.connect() as c:
            c.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON request_records BEGIN SELECT RAISE(ABORT,'test'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.begin('hash-a', 'A', 'm', '/v1/responses', b'{"input":"sensitive"}')
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM request_payloads').fetchone()[0], 0)

    def test_only_explicit_user_text_survives_supported_protocols(self):
        cases = [
            ({'instructions':'SYSTEM', 'input':[{'role':'system','content':'SYSTEM'},{'role':'developer','content':'DEVELOPER'}, {'role':'assistant','content':'ANSWER'}, {'type':'function_call_output','output':'TOOL'}, {'role':'user','content':[{'type':'input_text','text':'hello'},{'type':'input_image','image_url':'IMAGE'}]}]}, ['hello']),
            ({'system':'SYSTEM','messages':[{'role':'assistant','content':'ANSWER'},{'role':'user','content':[{'type':'text','text':'hello'},{'type':'tool_result','content':'TOOL'},{'type':'image','source':'IMAGE'}]}]}, ['hello']),
            ({'systemInstruction':{'parts':[{'text':'SYSTEM'}]},'contents':[{'role':'model','parts':[{'text':'ANSWER'}]},{'role':'user','parts':[{'text':'hello'},{'functionResponse':{'response':'TOOL'}},{'inlineData':{'data':'IMAGE'}}]}]}, ['hello']),
            ({'input':'hello','instructions':'SYSTEM','tools':[{'description':'TOOL'}]}, ['hello']),
            ({'prompt':['hello','world'],'suffix':'ANSWER'}, ['hello','world']),
        ]
        for payload,expected in cases:
            self.assertEqual(user_prompts(json.dumps(payload).encode()),expected)

    def test_history_reuses_prompt_text_but_preserves_order_and_repeated_turns(self):
        first = self.store.begin('hash-a','A','m','/v1/responses',json.dumps({'messages':[{'role':'user','content':'你好'}]}).encode(),'session')
        second = self.store.begin('hash-a','A','m','/v1/responses',json.dumps({'messages':[{'role':'user','content':'你好'},{'role':'assistant','content':'ASSISTANT'},{'role':'user','content':'继续'},{'role':'user','content':'继续'}]}).encode(),'session')
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM user_prompts').fetchone()[0],2)
            conv=c.execute('SELECT DISTINCT conversation_hash FROM request_records').fetchall();self.assertEqual(len(conv),1)
            data=c.execute('SELECT p.body_gzip FROM request_records r JOIN request_payloads p ON r.payload_id=p.id WHERE r.id=?',(second,)).fetchone()[0]
            ids=json.loads(gzip.decompress(data))['user_prompt_ids'];self.assertEqual(len(ids),3);self.assertEqual(ids[1],ids[2])

    def test_existing_full_bodies_are_replaced_with_user_only_references(self):
        record=self.store.begin('h','A','m','/v1/responses',b'{}')
        original=json.dumps({'instructions':'SYSTEM_SECRET','input':'keep user text','tools':[{'description':'TOOL_SECRET'}]}).encode()
        with self.connect() as c:
            c.execute("INSERT INTO request_payloads VALUES('legacy',?,?, 'request_body')",(gzip.compress(original),len(original)))
            c.execute("UPDATE request_records SET payload_id='legacy' WHERE id=?",(record,))
        self.store.initialize()
        with self.connect() as c:
            self.assertEqual(c.execute("SELECT count(*) FROM request_payloads WHERE payload_kind='request_body'").fetchone()[0],0)
            contents=[gzip.decompress(r[0]) for r in c.execute('SELECT text_gzip FROM user_prompts')]
            self.assertIn(b'keep user text',contents)
            self.assertTrue(all(b'SYSTEM_SECRET' not in text and b'TOOL_SECRET' not in text for text in contents))

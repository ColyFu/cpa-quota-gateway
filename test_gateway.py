import importlib.util, io, json, os, sqlite3, tempfile, unittest
from unittest import mock

os.environ.setdefault('QUOTA_ADMIN_TOKEN','test-token')
spec=importlib.util.spec_from_file_location('gateway',os.path.join(os.path.dirname(__file__),'gateway.py'))
g=importlib.util.module_from_spec(spec); spec.loader.exec_module(g)

class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); g.DB_PATH=os.path.join(self.tmp.name,'quota.db'); g.init_db()
        c=g.db(); c.execute('INSERT INTO prices VALUES(?,?,?,?,?,?,?)',('model-a',5_000_000,30_000_000,500_000,6_250_000,1_000_000,g.now())); c.close()
    def tearDown(self): self.tmp.cleanup()
    def test_keeper_price_formula(self):
        c=g.db(); got=g.cost_micro(c,'model-a',1_000_000,100_000,200_000,100_000); c.close()
        self.assertEqual(got,7_225_000)
    def test_month_uses_configured_timezone(self):
        self.assertEqual(g.period_key(1788193800),'2026-09')
    def test_atomic_budget_and_release(self):
        h=object.__new__(g.Gateway); old=g.DEFAULT_LIMIT; g.DEFAULT_LIMIT=3_000_000
        try:
            rid,_=h.acquire('secret-key','model-a',2_000_000)
            with self.assertRaises(PermissionError): h.acquire('secret-key','model-a',2_000_000)
            h.release(rid); h.acquire('secret-key','model-a',2_000_000)
        finally: g.DEFAULT_LIMIT=old
    def test_per_key_concurrency(self):
        h=object.__new__(g.Gateway); old=g.DEFAULT_CONCURRENCY; g.DEFAULT_CONCURRENCY=1
        try:
            rid,_=h.acquire('another-key','model-a',1)
            with self.assertRaises(TimeoutError): h.acquire('another-key','model-a',1)
            h.release(rid)
        finally: g.DEFAULT_CONCURRENCY=old
    def proxy_request(self, status=200, connection_error=None):
        body=json.dumps({'model':'model-a','max_output_tokens':1}).encode()
        h=object.__new__(g.Gateway); h.path='/v1/responses'; h.command='POST'; h.request_version='HTTP/1.1'; h.requestline='POST /v1/responses HTTP/1.1'
        h.headers={'Authorization':'Bearer proxy-key','Content-Length':str(len(body))}; h.rfile=io.BytesIO(body); h.wfile=io.BytesIO(); h.client_address=('127.0.0.1',12345)
        class Response:
            def __init__(self): self.status=status; self.body=io.BytesIO(b'upstream response')
            def getheaders(self): return [('Content-Type','application/json')]
            def read(self,size=-1): return self.body.read(size)
        class Connection:
            def __init__(self,*args,**kwargs): pass
            def request(self,*args,**kwargs):
                if connection_error: raise connection_error
            def getresponse(self): return Response()
            def close(self): pass
        with mock.patch.object(g.http.client,'HTTPConnection',Connection): h.proxy()
        c=g.db(); row=c.execute("SELECT count(*) active FROM reservations WHERE state='active'").fetchone(); period=c.execute("SELECT coalesce(sum(reserved_micro),0) reserved FROM periods").fetchone(); c.close()
        return row['active'],period['reserved']
    def test_failed_upstream_response_releases_reservation(self):
        self.assertEqual(self.proxy_request(status=503),(0,0))
    def test_successful_upstream_response_keeps_reservation_for_usage(self):
        active,reserved=self.proxy_request(status=200)
        self.assertEqual(active,1); self.assertGreater(reserved,0)
    def test_upstream_connection_error_releases_reservation(self):
        self.assertEqual(self.proxy_request(connection_error=OSError('connection failed')),(0,0))
    def test_user_reset_starts_independent_cycle(self):
        c=g.db(); c.execute('BEGIN IMMEDIATE'); p=g.ensure_policy(c,g.key_hash('user-a'),'user-a'); reset_at=1788193800
        reset=g.reset_policy_periods(c,[p['key_hash']],reset_at); c.commit()
        row=c.execute('select * from policies where key_hash=?',(p['key_hash'],)).fetchone(); c.close()
        self.assertEqual(reset,[p['key_hash']]); self.assertEqual(row['period_started_at'],reset_at)
        self.assertEqual(row['period_ends_at'],g.add_months(reset_at,1)); self.assertTrue(row['period_id'].startswith('reset:'))
    def test_reset_all_keeps_users_on_separate_periods(self):
        c=g.db(); c.execute('BEGIN IMMEDIATE'); a=g.ensure_policy(c,g.key_hash('user-a'),'user-a'); b=g.ensure_policy(c,g.key_hash('user-b'),'user-b')
        first=g.reset_policy_periods(c,[a['key_hash']],1788193800); second=g.reset_policy_periods(c,[a['key_hash'],b['key_hash']],1788280200); c.commit()
        starts=[r[0] for r in c.execute('select period_started_at from policies order by key_hash')]; c.close()
        self.assertEqual(len(first),1); self.assertEqual(len(second),2); self.assertEqual(starts,[1788280200,1788280200])

if __name__=='__main__': unittest.main()

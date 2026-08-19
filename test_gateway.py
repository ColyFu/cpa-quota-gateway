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
            c=g.db(); g.ensure_policy(c,g.key_hash('secret-key'),'secret-key'); c.close()
            rid,_=h.acquire('secret-key','model-a',2_000_000)
            with self.assertRaises(PermissionError): h.acquire('secret-key','model-a',2_000_000)
            h.release(rid); h.acquire('secret-key','model-a',2_000_000)
        finally: g.DEFAULT_LIMIT=old
    def test_per_key_concurrency(self):
        h=object.__new__(g.Gateway); old=g.DEFAULT_CONCURRENCY; g.DEFAULT_CONCURRENCY=1
        try:
            c=g.db(); g.ensure_policy(c,g.key_hash('another-key'),'another-key'); c.close()
            rid,_=h.acquire('another-key','model-a',1)
            with self.assertRaises(TimeoutError): h.acquire('another-key','model-a',1)
            h.release(rid)
        finally: g.DEFAULT_CONCURRENCY=old
    def proxy_request(self, status=200, connection_error=None):
        body=json.dumps({'model':'model-a','max_output_tokens':1}).encode()
        c=g.db(); g.ensure_policy(c,g.key_hash('proxy-key'),'proxy-key'); c.close()
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
        self.assertEqual(row['cycle_type'],'calendar_month'); self.assertEqual(row['period_ends_at'],g.calendar_period_bounds(reset_at)[1]); self.assertTrue(row['period_id'].startswith('reset:'))
    def test_fixed_day_cycle_resets_after_configured_days(self):
        c=g.db(); c.execute('BEGIN IMMEDIATE'); p=g.ensure_policy(c,g.key_hash('user-a'),'user-a'); c.execute("update policies set cycle_type='fixed_days',cycle_days=7 where key_hash=?",(p['key_hash'],)); reset_at=1788193800
        g.reset_policy_periods(c,[p['key_hash']],reset_at); c.commit(); row=c.execute('select * from policies where key_hash=?',(p['key_hash'],)).fetchone(); c.close()
        self.assertEqual(row['period_ends_at'],reset_at+7*24*60*60)
    def test_user_limit_only_returns_requested_fields(self):
        c=g.db(); p=g.ensure_policy(c,g.key_hash('user-a'),'user-a'); c.execute('update policies set keeper_active=1 where key_hash=?',(p['key_hash'],)); result=g.user_limit(c,'user-a'); c.close()
        self.assertEqual(set(result),{'limit_usd','used_usd','reset_at','cycle'})
        self.assertEqual(result['cycle'],{'type':'calendar_month','days':None})
    def test_codex_weekly_usage_reads_keeper_cache(self):
        class Response:
            def __init__(self,data): self.data=data
            def raise_for_status(self): pass
            def json(self): return self.data
        class Session:
            def post(self,url,**kwargs):
                if url.endswith('/auth/login'): return Response({})
                return Response({'items':[{'quota':{'quota':[{'key':'rate_limit.primary_window','usedPercent':63,'resetAt':'2026-08-20T16:07:18+08:00','window':{'seconds':604800}}]}}]})
            def get(self,url,**kwargs): return Response({'results':[{'type':'codex','auth_index':'auth-1'}]})
        with mock.patch.object(g,'KEEPER_PASSWORD','test-password'),mock.patch.object(g.requests,'Session',return_value=Session()): result=g.fetch_codex_weekly_usage()
        self.assertEqual(result,{'used_percent':63.0,'reset_at':'2026-08-20T16:07:18+08:00'})
    def test_removed_keeper_key_is_deleted_with_ledger(self):
        c=g.db(); p=g.ensure_policy(c,g.key_hash('removed-key'),'removed-key'); kh=p['key_hash']; c.execute('insert or ignore into periods(key_hash,period) values(?,?)',(kh,p['period_id'])); c.execute("insert into request_ticks values(?,?)",(kh,g.now()))
        removed=g.delete_policies(c,[kh])
        self.assertEqual(removed,1)
        for table in ('policies','periods','request_ticks','reservations','ledger'): self.assertEqual(c.execute(f'select count(*) from {table} where key_hash=?',(kh,)).fetchone()[0],0)
        c.close()
        with self.assertRaises(KeyError): object.__new__(g.Gateway).acquire('removed-key','model-a',1)
    def test_keeper_sync_deletes_keys_missing_from_keeper(self):
        c=g.db(); stale=g.ensure_policy(c,g.key_hash('stale-key'),'stale-key'); c.close()
        class Response:
            def __init__(self,data): self.data=data
            def raise_for_status(self): pass
            def json(self): return self.data
        class Session:
            def post(self,url,**kwargs): return Response({})
            def get(self,url,**kwargs):
                if url.endswith('/pricing'): return Response({'pricing':[]})
                return Response({'items':[{'apiKey':'active-key','keyAlias':'Active'}]})
        with mock.patch.object(g,'KEEPER_PASSWORD','test-password'),mock.patch.object(g.requests,'Session',return_value=Session()): result=g.sync_keeper_metadata()
        c=g.db(); hashes={r['key_hash'] for r in c.execute('select key_hash from policies')}; c.close()
        self.assertEqual(result['removed_count'],1)
        self.assertNotIn(stale['key_hash'],hashes)
        self.assertIn(g.key_hash('active-key'),hashes)
    def test_reset_all_keeps_users_on_separate_periods(self):
        c=g.db(); c.execute('BEGIN IMMEDIATE'); a=g.ensure_policy(c,g.key_hash('user-a'),'user-a'); b=g.ensure_policy(c,g.key_hash('user-b'),'user-b')
        first=g.reset_policy_periods(c,[a['key_hash']],1788193800); second=g.reset_policy_periods(c,[a['key_hash'],b['key_hash']],1788280200); c.commit()
        starts=[r[0] for r in c.execute('select period_started_at from policies order by key_hash')]; c.close()
        self.assertEqual(len(first),1); self.assertEqual(len(second),2); self.assertEqual(starts,[1788280200,1788280200])

if __name__=='__main__': unittest.main()

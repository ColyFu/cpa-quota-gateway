import copy
import http.client
import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from test_gateway import g
from routing import AccountRouter, DEFAULT_CONFIG, RoutingUnavailable, session_identity, validate_config


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        g.DB_PATH = self.temp.name+'/quota.db'
        g.UPSTREAM_QUEUE = g.ConcurrencyQueue(3)
        g.SHUTDOWN_EVENT.clear()
        g.init_db()
        self.router = g.UPSTREAM_QUEUE.router
        self.config = copy.deepcopy(DEFAULT_CONFIG)
        self.a = g.key_hash('user-a'); self.b = g.key_hash('user-b')
        c = g.db()
        for key in ('user-a', 'user-b'): g.ensure_policy(c, g.key_hash(key), key)
        c.execute('INSERT INTO prices VALUES(?,?,?,?,?,?,?)',('model-a',1000000,2000000,0,0,1000000,g.now()))
        c.close()
        self.config.update(enabled=True, overflow_threshold=2, accounts=[
            {'id': 'a', 'name': '账号甲', 'concurrency': 1, 'enabled': True, 'models': ['model-a']},
            {'id': 'b', 'name': '账号乙', 'concurrency': 1, 'enabled': True, 'models': ['model-a']},
        ], preferences={self.a: ['a'], self.b: ['b']})
        self.router.save(self.config)

    def tearDown(self):
        self.temp.cleanup()

    def test_preferred_account_below_threshold_and_spill_at_threshold(self):
        self.assertEqual(self.router.choose(self.a, '', 'model-a', {'a': 1}, {'a': 1}), 'a')
        self.assertEqual(self.router.choose(self.a, '', 'model-a', {'a': 1}, {'a': 2}), 'b')
        self.assertEqual(self.router.choose(self.a, '', 'model-a', {'a': 1, 'b': 1}, {'a': 4}), 'a')

    def test_live_session_never_spills_and_survives_restart(self):
        self.assertEqual(self.router.choose(self.a, 'session', 'model-a', {}, {}), 'a')
        restarted = AccountRouter(g.db)
        self.assertEqual(restarted.choose(self.a, 'session', 'model-a', {'a': 1}, {'a': 100}), 'a')

    def test_sessions_are_scoped_to_user(self):
        self.assertEqual(self.router.choose(self.a, 'same-session', 'model-a', {}, {}), 'a')
        self.assertEqual(self.router.choose(self.b, 'same-session', 'model-a', {}, {}), 'b')

    def test_ttl_uses_last_request_and_setting_applies_to_existing_binding(self):
        clock = [100000.]
        self.router.clock = lambda: clock[0]
        self.router.choose(self.a, 'session', 'model-a', {}, {})
        clock[0] += 11*3600
        self.assertEqual(self.router.choose(self.a, 'session', 'model-a', {'a': 1}, {'a': 4}), 'a')
        clock[0] += 11*3600
        self.assertEqual(self.router.choose(self.a, 'session', 'model-a', {'a': 1}, {'a': 4}), 'a')
        self.config['session_ttl_hours'] = 1; self.router.save(self.config)
        clock[0] += 3601
        self.assertEqual(self.router.choose(self.a, 'session', 'model-a', {'a': 1}, {'a': 4}), 'b')

    def test_unavailable_bound_account_rebinds(self):
        self.router.choose(self.a, 'session', 'model-a', {}, {})
        self.router.unavailable = {'a'}
        self.assertEqual(self.router.choose(self.a, 'session', 'model-a', {}, {}), 'b')
        self.router.unavailable = {'a', 'b'}
        with self.assertRaises(RoutingUnavailable): self.router.choose(self.a, 'session', 'model-a', {}, {})

    def test_unknown_model_fails_closed(self):
        with self.assertRaises(RoutingUnavailable): self.router.choose(self.a, '', 'other-model', {}, {})

    def test_disabled_router_preserves_legacy_dispatch(self):
        self.config['enabled'] = False; self.router.save(self.config)
        self.assertEqual(self.router.choose(self.a, '', 'other-model', {}, {}), '')

    def test_validation_rejects_invalid_config(self):
        for changes in ({'session_ttl_hours': 0}, {'session_ttl_hours': float('nan')}, {'enabled': 'yes'}, {'overflow_threshold': -1}, {'overflow_threshold': True}, {'preferences': {self.a: ['absent']}}, {'accounts': [self.config['accounts'][0]]*2}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): validate_config({**self.config, **changes})

    def test_identity_recognizes_headers_and_explicit_bodies_without_key_fallback(self):
        for header in ('Session_ID', 'X-Session-ID', 'X-Codex-Session-Id', 'X-Claude-Code-Session-Id'):
            self.assertEqual(session_identity({header: 'one'}, b''), g.key_hash('one'))
        for value in ({'prompt_cache_key': 'one'}, {'conversation': {'id': 'one'}}, {'metadata': {'session_id': 'one'}}, {'sessionId':'one'}, {'metadata':{'user_id':'user_abc_session_one'}}, {'metadata':{'user_id':'{\"session_id\":\"one\"}'}}):
            self.assertEqual(session_identity({}, json.dumps(value)), g.key_hash('one'))
        self.assertEqual(session_identity({'Authorization': 'Bearer key'}, b'{}'), '')
        self.assertEqual(session_identity({}, b'[]'), '')

    def request(self, user, session=''):
        r = g.ClientRequest(None); r.model='model-a'; r.alias='测试用户'; r.session_hash=session
        r.user=user
        return r

    def wait(self, predicate):
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            if predicate(): return
            time.sleep(.01)
        self.fail('condition timed out')

    def test_busy_account_does_not_block_another_account_and_queue_cleans_up(self):
        q = g.UPSTREAM_QUEUE
        first=self.request(self.a); self.assertTrue(q.acquire(first,self.a))
        second=self.request(self.a); result=[]
        worker=threading.Thread(target=lambda: result.append(q.acquire(second,self.a)),daemon=True);worker.start()
        try:
            self.wait(lambda:q.snapshot()['queued']==1)
            third=self.request(self.b); self.assertTrue(q.acquire(third,self.b))
            snap=q.detailed_snapshot()
            self.assertEqual(snap['active'],2);self.assertEqual(snap['global_queued'],1)
            self.assertEqual({t['account_id'] for t in snap['tasks']},{'a','b'})
            self.assertEqual(len({t['id'] for t in snap['tasks']}),3)
            q.release(self.b,third); second.cancelled.set()
            worker.join(2); self.assertEqual(result,[False])
            q.release(self.a,first)
            self.assertEqual(q.detailed_snapshot()['tasks'],[])
            self.assertEqual(q.active_by_account,{})
        finally:
            second.cancelled.set();worker.join(2)

    def test_queue_stages_have_named_tasks_and_retain_identity_when_promoted(self):
        q = g.UPSTREAM_QUEUE; self.config['enabled']=False;self.router.save(self.config);q.set_limit(1)
        first=self.request(self.a);q.acquire(first,self.a,1)
        second=self.request(self.a);third=self.request(self.b);threads=[];results=[]
        for request in (second,third):
            t=threading.Thread(target=lambda r=request:results.append(q.acquire(r,r.user,1)),daemon=True);t.start();threads.append(t)
        try:
            self.wait(lambda:q.snapshot()['queued']==2)
            snap=q.detailed_snapshot();self.assertEqual({t['stage'] for t in snap['tasks']},{'active','personal','global'})
            self.assertTrue(all(t['name']=='测试用户' for t in snap['tasks']))
            self.assertNotIn('user-a',json.dumps(snap))
        finally:
            second.cancelled.set();third.cancelled.set()
            for t in threads:t.join(2)
            q.release(self.a,first)
        self.assertFalse(q.tasks)

    def serve(self):
        server=g.QuotaHTTPServer(('127.0.0.1',0),g.Gateway)
        t=threading.Thread(target=server.serve_forever,daemon=True);t.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        return server.server_address[1]

    def call(self, port, path, method='GET', data=None, headers=None):
        conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
        conn.request(method,path,body=json.dumps(data) if data is not None else None,headers=headers or {})
        r=conn.getresponse();body=r.read();conn.close();return r.status,json.loads(body)

    def test_admin_endpoints_reject_unauthorized_and_missing_csrf(self):
        port=self.serve()
        for path in ('/admin/routing','/admin/queue','/admin/accounts','/admin/key-links','/admin/history-retention'):
            self.assertEqual(self.call(port,path)[0],401)
        headers={'X-Quota-Admin-Token':g.ADMIN_TOKEN}
        self.assertEqual(self.call(port,'/admin/routing','PUT',self.config,headers)[0],403)
        self.assertEqual(self.call(port,'/admin/queue',headers=headers)[0],200)

    def test_history_retention_setting_is_admin_only_validated_and_persistent(self):
        port=self.serve();path='/admin/history-retention';headers={'X-Quota-Admin-Token':g.ADMIN_TOKEN}
        status,body=self.call(port,path,headers=headers)
        self.assertEqual(status,200);self.assertEqual(body['days'],60)
        self.assertEqual(self.call(port,path,'PUT',{'days':30},headers)[0],403)
        headers['X-Quota-Admin-Request']='1'
        for value in [0,-1,1.5,True,'30',3651]:
            self.assertEqual(self.call(port,path,'PUT',{'days':value},headers)[0],400)
        self.assertEqual(self.call(port,path,'PUT',{'days':120},headers)[1]['days'],120)
        self.assertEqual(self.call(port,path,headers=headers)[1]['days'],120)

    def test_preference_updates_one_user_without_overwriting_other_preferences(self):
        port=self.serve();headers={'X-Quota-Admin-Token':g.ADMIN_TOKEN,'X-Quota-Admin-Request':'1'}
        status,_=self.call(port,'/admin/preference','PUT',{'key_hash':self.a,'accounts':['b']},headers)
        self.assertEqual(status,200);self.assertEqual(self.router.config['preferences'][self.b],['b'])
        self.assertEqual(self.router.config['preferences'][self.a],['b'])
        self.assertEqual(self.call(port,'/admin/preference','PUT',{'key_hash':self.a,'accounts':['missing']},headers)[0],400)

    def test_enable_requires_verified_cpa_capability(self):
        port=self.serve();headers={'X-Quota-Admin-Token':g.ADMIN_TOKEN,'X-Quota-Admin-Request':'1'}
        with mock.patch.dict(os.environ, {'QUOTA_ROUTING_TOKEN':''}):
            self.assertEqual(self.call(port,'/admin/routing','PUT',self.config,headers)[0],400)

    def test_viewer_cookie_does_not_grant_gateway_admin(self):
        port=self.serve();response=mock.Mock(status_code=200)
        with mock.patch.dict(os.environ,{'UNIFIED_CONSOLE':'1'}),mock.patch.object(g.requests,'get',return_value=response):
            response.json.return_value={'authenticated':True,'role':'api_key_viewer'}
            self.assertEqual(self.call(port,'/admin/queue',headers={'Cookie':'session=viewer'})[0],401)
            response.json.return_value={'authenticated':True,'role':'admin'}
            self.assertEqual(self.call(port,'/admin/queue',headers={'Cookie':'session=admin'})[0],200)

    def test_http_proxy_enforces_selected_account_and_strips_client_pin(self):
        observed=[]
        class Upstream(g.BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(self):
                if self.path=='/quota-gateway/capabilities':
                    g.json_response(self,200,{'credential_pinning':True})
                elif self.path=='/v0/management/auth-files':
                    g.json_response(self,200,{'files':[{'id':'a'},{'id':'b'}]})
                elif self.path=='/v1/models':
                    observed.append(dict(self.headers));g.json_response(self,200,{'data':[{'id':'model-a'}]})
                else:g.json_response(self,404,{})
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                observed.append(dict(self.headers))
                g.json_response(self,200,{'ok':True})
        upstream=g.QuotaHTTPServer(('127.0.0.1',0),Upstream)
        threading.Thread(target=upstream.serve_forever,daemon=True).start()
        self.addCleanup(upstream.server_close);self.addCleanup(upstream.shutdown)
        port=self.serve()
        with mock.patch.object(g,'UPSTREAM','http://127.0.0.1:'+str(upstream.server_address[1])), mock.patch.dict(os.environ,{'QUOTA_ROUTING_TOKEN':'trusted-secret'}):
            for key,account in (('user-a','a'),('user-b','b')):
                headers={'Authorization':'Bearer '+key,'Session_ID':'same-session','X-Quota-Auth-ID':'forged','X-Quota-Routing-Token':'forged'}
                status,_=self.call(port,'/v1/responses','POST',{'model':'model-a','input':key,'max_output_tokens':10},headers)
                self.assertEqual(status,200)
                self.assertEqual(observed[-1]['X-Quota-Auth-ID'],account)
                self.assertEqual(observed[-1]['X-Quota-Routing-Token'],'trusted-secret')
            status,_=self.call(port,'/v1/models',headers=headers)
            self.assertEqual(status,200)
            self.assertNotIn('X-Quota-Auth-ID',observed[-1])
            self.config['enabled']=False;self.router.save(self.config)
            status,_=self.call(port,'/v1/responses','POST',{'model':'model-a','input':'legacy','max_output_tokens':10},headers)
            self.assertEqual(status,200)
            self.assertNotIn('X-Quota-Auth-ID',observed[-1]);self.assertNotIn('X-Quota-Routing-Token',observed[-1])
        self.assertFalse(g.UPSTREAM_QUEUE.tasks)

    def test_removed_features_are_unreachable_in_unified_console(self):
        port=self.serve()
        with mock.patch.dict(os.environ,{'UNIFIED_CONSOLE':'1'}):
            for path in ('/api/v1/ranking/status','/api/v1/auth/sessions','/api/v1/update/check'):
                self.assertEqual(self.call(port,path)[0],404)

if __name__=='__main__':unittest.main()

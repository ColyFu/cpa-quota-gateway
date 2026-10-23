import http.client
import importlib.util, io, json, os, socket, sqlite3, tempfile, threading, time, unittest
from unittest import mock

os.environ.setdefault('QUOTA_ADMIN_TOKEN','test-token')
spec=importlib.util.spec_from_file_location('gateway',os.path.join(os.path.dirname(__file__),'gateway.py'))
g=importlib.util.module_from_spec(spec); spec.loader.exec_module(g)

class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); g.DB_PATH=os.path.join(self.tmp.name,'quota.db'); g.REQUEST_ATTEMPTS.clear(); g.SHUTDOWN_EVENT.clear(); g.UPSTREAM_QUEUE=g.ConcurrencyQueue(10); g.init_db()
        c=g.db(); c.execute('INSERT INTO prices VALUES(?,?,?,?,?,?,?)',('model-a',5_000_000,30_000_000,500_000,6_250_000,1_000_000,g.now())); c.close()
    def tearDown(self): self.tmp.cleanup()
    def test_bulk_enable_disable_and_authorization(self):
        c=g.db()
        for key in ('bulk-a','bulk-b'): g.ensure_policy(c,g.key_hash(key),key)
        before=[tuple(r) for r in c.execute('SELECT monthly_limit_micro,period_id FROM policies ORDER BY key_hash')]
        c.close()
        server=g.ThreadingHTTPServer(('127.0.0.1',0),g.Gateway)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        def request(body,authorized=True,confirmed=True):
            conn=http.client.HTTPConnection(*server.server_address,timeout=5)
            headers={'Content-Type':'application/json'}
            if authorized: headers['X-Quota-Admin-Token']=g.ADMIN_TOKEN
            if confirmed: headers['X-Quota-Admin-Request']='1'
            conn.request('PUT','/admin/policies/enabled',json.dumps(body),headers)
            response=conn.getresponse(); result=(response.status,json.loads(response.read()));conn.close();return result
        try:
            self.assertEqual(request({'enabled':False},authorized=False)[0],401)
            self.assertEqual(request({'enabled':False},confirmed=False)[0],403)
            for invalid in ({'enabled':'false'},{'enabled':0},{},[]):
                self.assertEqual(request(invalid)[0],400)
            self.assertEqual(request({'enabled':False}),(200,{'updated_count':2,'enabled':False}))
            handler=object.__new__(g.Gateway)
            for key in ('bulk-a','bulk-b'):
                with self.assertRaises(ValueError): handler.acquire(key,'model-a',1)
            # Metadata refresh and period rollover must not re-enable a key.
            c=g.db();g.ensure_policy(c,g.key_hash('bulk-a'),'bulk-a')
            self.assertEqual(c.execute('SELECT sum(enabled) FROM policies').fetchone()[0],0)
            self.assertEqual([tuple(r) for r in c.execute('SELECT monthly_limit_micro,period_id FROM policies ORDER BY key_hash')],before);c.close()
            self.assertEqual(request({'enabled':True})[0],200)
            for key in ('bulk-a','bulk-b'): handler.release(handler.acquire(key,'model-a',1)[0])
        finally:
            server.shutdown();server.server_close();thread.join()

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
        h=object.__new__(g.Gateway); old=g.DEFAULT_CONCURRENCY; g.DEFAULT_CONCURRENCY=0
        try:
            c=g.db(); g.ensure_policy(c,g.key_hash('another-key'),'another-key'); c.close()
            first=h.acquire('another-key','model-a',1)[0]
            second=h.acquire('another-key','model-a',1)[0]
            h.release(first); h.release(second)
        finally: g.DEFAULT_CONCURRENCY=old

    def test_capacity_limit_waits_until_reservation_is_released(self):
        h=object.__new__(g.Gateway)
        c=g.db(); g.ensure_policy(c,g.key_hash('wait-key'),'wait-key'); c.execute(
            "update policies set concurrency=1 where key_hash=?",(g.key_hash('wait-key'),)
        ); c.close()
        first=h.acquire('wait-key','model-a',1)[0]
        result=[]
        worker=threading.Thread(target=lambda: result.append(h.acquire('wait-key','model-a',1)[0]))
        worker.start(); time.sleep(0.2)
        self.assertTrue(worker.is_alive())
        h.release(first); worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(result),1)
        h.release(result[0])

    def wait_until(self, predicate):
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            if predicate():return
            time.sleep(0.01)
        self.fail('condition did not become true')

    def test_global_queue_admits_waiters_in_arrival_order(self):
        queue=g.ConcurrencyQueue(1);self.assertTrue(queue.acquire(g.ClientRequest(None)))
        order=[];controls=[threading.Event() for _ in range(3)];workers=[]
        def run(index):
            if queue.acquire(g.ClientRequest(None)):
                order.append(index);controls[index].wait(3);queue.release()
        try:
            for index in range(3):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index+1)
            queue.release()
            for index in range(3):
                self.wait_until(lambda:len(order)==index+1)
                self.assertEqual(order,list(range(index+1)))
                controls[index].set()
            for worker in workers:worker.join(3)
            self.assertEqual(queue.snapshot(),{'limit':1,'active':0,'queued':0})
        finally:
            for control in controls:control.set()
            for worker in workers:worker.join(3)

    def test_superusers_receive_next_slots_in_fifo_order_without_preemption(self):
        queue=g.ConcurrencyQueue(1);queue.acquire(g.ClientRequest(None),'busy')
        order=[];controls=[threading.Event() for _ in range(4)];workers=[]
        def run(index):
            identity='user-'+str(index)
            if queue.acquire(g.ClientRequest(None),identity,is_superuser=index>=2):
                order.append(index);controls[index].wait(3);queue.release(identity)
        try:
            for index in range(4):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index+1)
            self.assertEqual(order,[])
            self.assertEqual(queue.snapshot()['active'],1)
            queue.release('busy')
            for position,index in enumerate((2,3,0,1)):
                self.wait_until(lambda:len(order)==position+1)
                self.assertEqual(order[-1],index);controls[index].set()
            for worker in workers:worker.join(3)
        finally:
            for control in controls:control.set()
            for worker in workers:worker.join(3)

    def test_personal_limit_does_not_hold_global_slots_or_block_other_users(self):
        queue=g.ConcurrencyQueue(2);queue.acquire(g.ClientRequest(None),'vip',personal_limit=1,is_superuser=True)
        queue.acquire(g.ClientRequest(None),'busy')
        order=[];controls={'vip':threading.Event(),'normal':threading.Event()};workers=[]
        def run(identity):
            if queue.acquire(g.ClientRequest(None),identity,personal_limit=1,is_superuser=identity=='vip'):
                order.append(identity);controls[identity].wait(3);queue.release(identity)
        try:
            for identity in ('vip','normal'):
                worker=threading.Thread(target=run,args=(identity,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==len(workers))
            queue.release('busy')
            self.wait_until(lambda:order==['normal'])
            self.assertEqual(queue.snapshot(),{'limit':2,'active':2,'queued':1})
            controls['normal'].set();workers[1].join(2)
            self.wait_until(lambda:order==['normal','vip'])
            self.assertEqual(queue.active_by_key['vip'],2)
            queue.release('vip')
        finally:
            for control in controls.values():control.set()
            for worker in workers:worker.join(3)

    def test_idle_capacity_is_borrowed_but_new_users_receive_released_slots_first(self):
        queue=g.ConcurrencyQueue(4)
        controls=[threading.Event() for _ in range(6)];workers=[];order=[]
        def run(index):
            identity='solo' if index<5 else 'other'
            if queue.acquire(g.ClientRequest(None),identity,personal_limit=2,is_superuser=index<5):
                order.append(index);controls[index].wait(5);queue.release(identity)
        try:
            for index in range(4):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:len(order)==index+1)
            self.assertEqual(queue.snapshot(),{'limit':4,'active':4,'queued':0})
            self.assertEqual(queue.active_by_key['solo'],4)
            for index in (4,5):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index-3)
            controls[0].set();self.wait_until(lambda:len(order)==5)
            self.assertEqual(order,[0,1,2,3,5])
            self.assertEqual(queue.active_by_key,{'solo':3,'other':1})
            controls[1].set();self.wait_until(lambda:len(order)==6)
            self.assertEqual(order,[0,1,2,3,5,4])
            self.assertEqual(queue.snapshot(),{'limit':4,'active':4,'queued':0})
            self.assertFalse(controls[2].is_set());self.assertFalse(controls[3].is_set())
        finally:
            for control in controls:control.set()
            for worker in workers:worker.join(5)
        self.assertEqual(queue.snapshot(),{'limit':4,'active':0,'queued':0})

    def test_borrowing_waits_until_other_users_leave_their_queues(self):
        queue=g.ConcurrencyQueue(2)
        queue.acquire(g.ClientRequest(None),'vip',personal_limit=1,is_superuser=True)
        queue.acquire(g.ClientRequest(None),'normal',personal_limit=1)
        order=[];controls={'vip':threading.Event(),'normal':threading.Event()};workers=[]
        requests={identity:g.ClientRequest(None) for identity in ('normal','vip')}
        def run(identity):
            if queue.acquire(requests[identity],identity,personal_limit=1,is_superuser=identity=='vip'):
                order.append(identity);controls[identity].wait(3);queue.release(identity)
        try:
            for identity in ('normal','vip'):
                worker=threading.Thread(target=run,args=(identity,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==len(workers))
            with queue.condition:
                queue.set_limit(3)
                self.assertIsNone(queue.next_ticket())
            self.assertEqual(order,[])
            requests['normal'].cancelled.set();workers[0].join(2)
            self.wait_until(lambda:order==['vip'])
            self.assertEqual(queue.snapshot(),{'limit':3,'active':3,'queued':0})
        finally:
            for control in controls.values():control.set()
            for worker in workers:worker.join(3)
            queue.release('vip');queue.release('normal')

    def test_personal_release_appends_older_request_behind_existing_global_waiter(self):
        queue=g.ConcurrencyQueue(2)
        queue.acquire(g.ClientRequest(None),'A',personal_limit=1)
        queue.acquire(g.ClientRequest(None),'busy')
        controls={'A':threading.Event(),'B':threading.Event()};workers=[];order=[]
        released=set()
        def run(identity):
            if queue.acquire(g.ClientRequest(None),identity,personal_limit=1):
                order.append(identity);controls[identity].wait(3);queue.release(identity)
        try:
            older=threading.Thread(target=run,args=('A',),daemon=True);older.start();workers.append(older)
            self.wait_until(lambda:queue.snapshot()['queued']==1)
            self.assertEqual(queue.snapshot(include_stages=True)['personal_queued'],1)
            newer=threading.Thread(target=run,args=('B',),daemon=True);newer.start();workers.append(newer)
            self.wait_until(lambda:queue.snapshot()['queued']==2)
            self.assertEqual(queue.snapshot(include_stages=True)['global_queued'],1)
            queue.release('A');released.add('A')
            self.wait_until(lambda:order==['B'])
            self.assertEqual(queue.snapshot(include_stages=True)['personal_queued'],0)
            self.assertEqual(queue.snapshot(include_stages=True)['global_queued'],1)
            queue.release('busy');released.add('busy')
            self.wait_until(lambda:order==['B','A'])
        finally:
            for identity in ('A','busy'):
                if identity not in released:queue.release(identity)
            for control in controls.values():control.set()
            for worker in workers:worker.join(3)
        self.assertEqual(queue.pending_by_key,{})

    def test_global_waiters_hold_personal_permits_and_new_promotions_join_at_tail(self):
        queue=g.ConcurrencyQueue(1);queue.acquire(g.ClientRequest(None),'busy')
        controls=[threading.Event() for _ in range(4)];workers=[];order=[];busy_released=False
        def run(index):
            identity='A' if index<3 else 'B'
            if queue.acquire(g.ClientRequest(None),identity,personal_limit=2):
                order.append(index);controls[index].wait(4);queue.release(identity)
        try:
            for index in range(4):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index+1)
            self.assertEqual(queue.snapshot(include_stages=True),{'limit':1,'active':1,'queued':4,'personal_queued':1,'global_queued':3})
            self.assertEqual(queue.pending_by_key,{'A':2,'B':1})
            queue.release('busy');busy_released=True
            for position,index in enumerate((0,1,3,2)):
                self.wait_until(lambda:len(order)==position+1)
                self.assertEqual(order[-1],index);controls[index].set()
        finally:
            if not busy_released:queue.release('busy')
            for control in controls:control.set()
            for worker in workers:worker.join(4)
        self.assertEqual(queue.pending_by_key,{})

    def test_cancelled_global_waiter_releases_personal_permit_without_cutting_in(self):
        queue=g.ConcurrencyQueue(1);queue.acquire(g.ClientRequest(None),'busy')
        controls=[threading.Event() for _ in range(3)];workers=[];order=[];busy_released=False
        requests=[g.ClientRequest(None) for _ in range(3)]
        def run(index):
            identity='A' if index<2 else 'B'
            if queue.acquire(requests[index],identity,personal_limit=1):
                order.append(index);controls[index].wait(4);queue.release(identity)
        try:
            for index in range(3):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index+1)
            requests[0].cancelled.set();workers[0].join(2)
            self.assertFalse(workers[0].is_alive())
            self.assertEqual(queue.snapshot(include_stages=True),{'limit':1,'active':1,'queued':2,'personal_queued':0,'global_queued':2})
            self.assertEqual(queue.pending_by_key,{'A':1,'B':1})
            queue.release('busy');busy_released=True
            self.wait_until(lambda:order==[2])
            controls[2].set();self.wait_until(lambda:order==[2,1])
        finally:
            if not busy_released:queue.release('busy')
            for control in controls:control.set()
            for worker in workers:worker.join(4)
        self.assertEqual(queue.pending_by_key,{})

    def test_raising_personal_limit_promotes_waiters_to_global_tail(self):
        queue=g.ConcurrencyQueue(1);queue.acquire(g.ClientRequest(None),'busy')
        controls=[threading.Event() for _ in range(3)];workers=[];order=[];busy_released=False
        def run(index):
            identity='A' if index<2 else 'B'
            if queue.acquire(g.ClientRequest(None),identity,personal_limit=1):
                order.append(index);controls[index].wait(4);queue.release(identity)
        try:
            for index in range(3):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index+1)
            self.assertEqual(queue.snapshot(include_stages=True)['personal_queued'],1)
            queue.set_policy('A',2,False)
            self.assertEqual(queue.snapshot(include_stages=True)['personal_queued'],0)
            self.assertEqual(queue.pending_by_key,{'A':2,'B':1})
            queue.release('busy');busy_released=True
            for position,index in enumerate((0,2,1)):
                self.wait_until(lambda:len(order)==position+1)
                self.assertEqual(order[-1],index);controls[index].set()
        finally:
            if not busy_released:queue.release('busy')
            for control in controls:control.set()
            for worker in workers:worker.join(4)
        self.assertEqual(queue.pending_by_key,{})

    def test_live_limit_and_superuser_changes_apply_to_already_queued_requests(self):
        queue=g.ConcurrencyQueue(1);queue.acquire(g.ClientRequest(None),'busy')
        order=[];controls=[threading.Event(),threading.Event()];workers=[]
        def run(index):
            identity='user-'+str(index)
            if queue.acquire(g.ClientRequest(None),identity):
                order.append(index);controls[index].wait(3);queue.release(identity)
        try:
            for index in range(2):
                worker=threading.Thread(target=run,args=(index,),daemon=True);worker.start();workers.append(worker)
                self.wait_until(lambda:queue.snapshot()['queued']==index+1)
            queue.set_policy('user-1',1,True);queue.set_limit(2)
            self.wait_until(lambda:order==[1])
            queue.set_limit(1);controls[1].set();workers[1].join(2)
            self.assertEqual(queue.snapshot(),{'limit':1,'active':1,'queued':1})
            self.assertEqual(order,[1])
            queue.release('busy');self.wait_until(lambda:order==[1,0])
        finally:
            for control in controls:control.set()
            for worker in workers:worker.join(3)

    def test_global_and_personal_settings_api_validates_authorization_and_survives_reload(self):
        c=g.db();policy=g.ensure_policy(c,g.key_hash('policy-key'),'policy-key');c.close()
        server=g.QuotaHTTPServer(('127.0.0.1',0),g.Gateway)
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        def request(path,body=None,authorized=True,confirmed=True):
            conn=http.client.HTTPConnection(*server.server_address,timeout=3)
            headers={'Content-Type':'application/json'}
            if authorized:headers['X-Quota-Admin-Token']=g.ADMIN_TOKEN
            if confirmed:headers['X-Quota-Admin-Request']='1'
            conn.request('GET' if body is None else 'PUT',path,None if body is None else json.dumps(body),headers)
            response=conn.getresponse();result=(response.status,json.loads(response.read()));conn.close();return result
        try:
            self.assertEqual(request('/admin/concurrency',{'global_limit':7},authorized=False)[0],401)
            self.assertEqual(request('/admin/concurrency',{'global_limit':7},confirmed=False)[0],403)
            for value in (-1,1.5,True,'10',None):
                self.assertEqual(request('/admin/concurrency',{'global_limit':value})[0],400)
            self.assertEqual(request('/admin/concurrency',{'global_limit':7}),(200,{'limit':7,'active':0,'queued':0,'personal_queued':0,'global_queued':0}))
            self.assertEqual(request('/admin/concurrency')[1]['limit'],7)
            for field,values in [('concurrency',(-1,1.5,True,'2')),('is_superuser',('true',1,None))]:
                for value in values:
                    self.assertEqual(request('/admin/policy',{'key_hash':policy['key_hash'],field:value})[0],400)
            status,saved=request('/admin/policy',{'key_hash':policy['key_hash'],'concurrency':2,'is_superuser':True})
            self.assertEqual(status,200);self.assertEqual(saved['concurrency'],2);self.assertIs(saved['is_superuser'],True)
            self.assertTrue(g.UPSTREAM_QUEUE.policies[policy['key_hash']]['is_superuser'])
            c=g.db();g.ensure_policy(c,policy['key_hash'],'policy-key');c.close()
            g.UPSTREAM_QUEUE=g.ConcurrencyQueue(99);g.init_db();g.load_concurrency_configuration()
            self.assertEqual(g.UPSTREAM_QUEUE.snapshot()['limit'],7)
            self.assertEqual(g.UPSTREAM_QUEUE.policies[policy['key_hash']],{'concurrency':2,'is_superuser':True})
            self.assertIs(request('/admin/summary')[1]['items'][0]['is_superuser'],True)
            self.assertEqual(request('/admin/summary')[1]['concurrency']['limit'],7)
            self.assertEqual(request('/admin/policy',{'key_hash':policy['key_hash'],'is_superuser':False})[1]['is_superuser'],False)
        finally:
            server.shutdown();server.server_close();worker.join(3)

    def test_existing_database_adds_superuser_flag_without_resetting_personal_limit(self):
        c=g.db();policy=g.ensure_policy(c,g.key_hash('legacy-key'),'legacy-key')
        c.execute('update policies set concurrency=2 where key_hash=?',(policy['key_hash'],))
        c.execute('alter table policies drop column is_superuser');c.close()
        g.init_db();c=g.db();row=c.execute('select concurrency,is_superuser from policies where key_hash=?',(policy['key_hash'],)).fetchone();c.close()
        self.assertEqual(tuple(row),(2,0))

    def test_pending_billing_reservation_does_not_block_personal_concurrency(self):
        self.proxy_request(status=200)
        c=g.db();c.execute('update policies set concurrency=1 where key_hash=?',(g.key_hash('proxy-key'),));c.close()
        g.UPSTREAM_QUEUE.set_policy(g.key_hash('proxy-key'),1,False)
        result=[]
        worker=threading.Thread(target=lambda:result.append(self.proxy_request(status=200)),daemon=True)
        worker.start();worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(result[0][0],2)

    def test_disconnected_waiter_leaves_queue_without_taking_a_slot(self):
        queue=g.ConcurrencyQueue(1);queue.acquire(g.ClientRequest(None))
        client,peer=socket.socketpair();results=[]
        worker=threading.Thread(target=lambda:results.append(queue.acquire(g.ClientRequest(client))),daemon=True)
        try:
            worker.start();self.wait_until(lambda:queue.snapshot()['queued']==1)
            peer.close();worker.join(2)
            self.assertEqual(results,[False])
            self.assertEqual(queue.snapshot(),{'limit':1,'active':1,'queued':0})
        finally:
            client.close();peer.close();queue.release();worker.join(2)

    def test_proxy_error_releases_global_slot_and_request_fingerprint(self):
        c=g.db();g.ensure_policy(c,g.key_hash('proxy-key'),'proxy-key');c.close()
        body=b'{"model":"model-a","max_output_tokens":1}'
        handler=object.__new__(g.Gateway);handler.path='/v1/responses';handler.command='POST'
        handler.headers={'Authorization':'Bearer proxy-key','Content-Length':str(len(body))}
        handler.rfile=io.BytesIO(body);handler.client_address=('127.0.0.1',12345)
        queue=g.ConcurrencyQueue(1)
        with mock.patch.object(g,'UPSTREAM_QUEUE',queue),mock.patch.object(g.Gateway,'proxy_admitted',side_effect=RuntimeError('test failure')):
            with self.assertRaisesRegex(RuntimeError,'test failure'):handler.proxy()
        self.assertEqual(queue.snapshot(),{'limit':1,'active':0,'queued':0})
        self.assertEqual(len(g.ACTIVE_REQUESTS),0)
        fingerprint,_=g.request_fingerprint('proxy-key','POST','/v1/responses',body,handler.headers)
        self.assertFalse(g.REQUEST_ATTEMPTS.entries[fingerprint]['in_flight'])

    def test_eleventh_http_request_waits_without_reserving_quota_or_replaying_duplicates(self):
        c=g.db();g.ensure_policy(c,g.key_hash('proxy-key'),'proxy-key');c.close()
        queue=g.ConcurrencyQueue(10);controls=[threading.Event() for _ in range(11)]
        entered=[];running=0;peak=0;lock=threading.Lock();results=[];errors=[];clients=[]
        class Upstream(g.BaseHTTPRequestHandler):
            def do_POST(self):
                nonlocal running,peak
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                number=body['input']
                with lock:
                    entered.append(number);running+=1;peak=max(peak,running)
                controls[number].wait(8)
                with lock:running-=1
                self.send_response(200);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'ok')
            def log_message(self,*args):pass
        upstream=g.QuotaHTTPServer(('127.0.0.1',0),Upstream)
        gateway=g.QuotaHTTPServer(('127.0.0.1',0),g.Gateway)
        servers=[threading.Thread(target=server.serve_forever,daemon=True) for server in (upstream,gateway)]
        for server in servers:server.start()
        def start_client(number):
            try:
                conn=http.client.HTTPConnection(*gateway.server_address,timeout=10)
                conn.request('POST','/v1/responses',json.dumps({'model':'model-a','max_output_tokens':1,'input':number}),{'Authorization':'Bearer proxy-key','X-Client-Request-Id':'request-'+str(number)})
                response=conn.getresponse();results.append((response.status,response.read()));conn.close()
            except Exception as exc:errors.append(exc)
        with mock.patch.object(g,'UPSTREAM_QUEUE',queue),mock.patch.object(g,'UPSTREAM','http://127.0.0.1:'+str(upstream.server_port)):
            try:
                for number in range(10):
                    client=threading.Thread(target=start_client,args=(number,),daemon=True);client.start();clients.append(client)
                self.wait_until(lambda:len(entered)==10)
                client=threading.Thread(target=start_client,args=(10,),daemon=True);client.start();clients.append(client)
                self.wait_until(lambda:queue.snapshot()['queued']==1)
                self.assertEqual(queue.snapshot(),{'limit':10,'active':10,'queued':1})
                c=g.db();self.assertEqual(c.execute("select count(*) from reservations where state='active'").fetchone()[0],10);c.close()
                duplicate=http.client.HTTPConnection(*gateway.server_address,timeout=3)
                duplicate.request('POST','/v1/responses',json.dumps({'model':'model-a','max_output_tokens':1,'input':10}),{'Authorization':'Bearer proxy-key','X-Client-Request-Id':'changed-id'})
                response=duplicate.getresponse();self.assertEqual(response.status,409);response.read();duplicate.close()
                self.assertEqual(len(entered),10)
                controls[0].set();self.wait_until(lambda:len(entered)==11)
                self.assertEqual(entered[-1],10)
                self.assertEqual(peak,10)
            finally:
                for control in controls:control.set()
                for client in clients:client.join(10)
                for server in (gateway,upstream):server.shutdown();server.server_close()
                for server in servers:server.join(3)
        self.assertEqual(errors,[])
        self.assertEqual(results,[(200,b'ok')]*11)
        self.assertEqual(queue.snapshot(),{'limit':10,'active':0,'queued':0})

    def proxy_request(self, status=200, connection_error=None, response_error=None, body_size=None):
        body=json.dumps({'model':'model-a','max_output_tokens':1}).encode()
        if body_size:
            body += b' ' * max(0, body_size - len(body))
        c=g.db(); g.ensure_policy(c,g.key_hash('proxy-key'),'proxy-key'); c.close()
        h=object.__new__(g.Gateway); h.path='/v1/responses'; h.command='POST'; h.request_version='HTTP/1.1'; h.requestline='POST /v1/responses HTTP/1.1'
        h.headers={'Authorization':'Bearer proxy-key','Content-Length':str(len(body))}; h.rfile=io.BytesIO(body); h.wfile=io.BytesIO(); h.client_address=('127.0.0.1',12345)
        class Response:
            def __init__(self): self.status=status; self.body=io.BytesIO(b'upstream response')
            def getheaders(self): return [('Content-Type','application/json')]
            def read(self,size=-1):
                if response_error and self.body.tell() >= response_error[0]:
                    raise response_error[1]
                return self.body.read(size)
            def close(self): pass
        class Connection:
            def __init__(self,*args,**kwargs): self.sock=None
            def request(self,*args,**kwargs):
                if connection_error: raise connection_error
            def getresponse(self): return Response()
            def close(self): pass
        with mock.patch.object(g.http.client,'HTTPConnection',Connection),mock.patch.object(g,'UPSTREAM_HEADER_RETRY_ATTEMPTS',-1),mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_BYTES',64*1024*1024): h.proxy()
        c=g.db(); row=c.execute("SELECT count(*) active FROM reservations WHERE state='active'").fetchone(); period=c.execute("SELECT coalesce(sum(reserved_micro),0) reserved FROM periods").fetchone(); c.close()
        return row['active'],period['reserved']
    def test_failed_upstream_response_releases_reservation(self):
        self.assertEqual(self.proxy_request(status=503),(0,0))
    def test_successful_upstream_response_keeps_reservation_for_usage(self):
        active,reserved=self.proxy_request(status=200)
        self.assertEqual(active,1); self.assertGreater(reserved,0)
    def test_upstream_connection_error_releases_reservation(self):
        self.assertEqual(self.proxy_request(connection_error=OSError('connection failed')),(0,0))

    def test_stream_error_releases_reservation_after_headers(self):
        self.assertEqual(
            self.proxy_request(
                response_error=(0, http.client.IncompleteRead(b'partial'))
            ),
            (0, 0),
        )

    def test_large_request_retries_one_transient_header_failure(self):
        body_size = 128 * 1024
        calls = []
        body = json.dumps({'model': 'model-a', 'max_output_tokens': 1}).encode()
        body += b' ' * (body_size - len(body))
        c = g.db(); g.ensure_policy(c, g.key_hash('proxy-key'), 'proxy-key'); c.close()
        h = object.__new__(g.Gateway)
        h.path='/v1/responses'; h.command='POST'; h.request_version='HTTP/1.1'; h.requestline='POST /v1/responses HTTP/1.1'
        h.headers={'Authorization':'Bearer proxy-key','Content-Length':str(len(body))}; h.rfile=io.BytesIO(body); h.wfile=io.BytesIO(); h.client_address=('127.0.0.1',12345)
        class Response:
            status=200
            def __init__(self): self.body=io.BytesIO(b'ok')
            def getheaders(self): return [('Content-Type','text/plain')]
            def read(self,size=-1): return self.body.read(size)
            def close(self): pass
        class Connection:
            def __init__(self,*args,**kwargs): pass
            def request(self,*args,**kwargs):
                calls.append(1)
                if len(calls) == 1: raise ConnectionError('temporary connect failure')
            def getresponse(self): return Response()
            def close(self): pass
        with mock.patch.object(g.http.client,'HTTPConnection',Connection), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_BYTES',1024), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS',60), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS',1), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS',120), \
             mock.patch.object(g,'UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS',0):
            h.proxy()
        self.assertEqual(len(calls), 2)
        c=g.db(); active=c.execute("SELECT count(*) FROM reservations WHERE state='active'").fetchone()[0]; c.close()
        self.assertEqual(active, 1)

    def test_large_request_retries_transient_application_status(self):
        calls = []
        body = json.dumps({'model': 'model-a', 'max_output_tokens': 1}).encode() + b' ' * (128 * 1024)
        c = g.db(); g.ensure_policy(c, g.key_hash('proxy-key'), 'proxy-key'); c.close()
        h = object.__new__(g.Gateway)
        h.path='/v1/responses'; h.command='POST'; h.request_version='HTTP/1.1'; h.requestline='POST /v1/responses HTTP/1.1'
        h.headers={'Authorization':'Bearer proxy-key','Content-Length':str(len(body))}; h.rfile=io.BytesIO(body); h.wfile=io.BytesIO(); h.client_address=('127.0.0.1',12345)
        class Response:
            status=503
            def __init__(self): self.body=io.BytesIO(b'failed')
            def getheaders(self): return [('Content-Type','text/plain')]
            def read(self,size=-1): return self.body.read(size)
            def close(self): pass
        class Connection:
            def __init__(self,*args,**kwargs): pass
            def request(self,*args,**kwargs): calls.append(1)
            def getresponse(self): return Response()
            def close(self): pass
        with mock.patch.object(g.http.client,'HTTPConnection',Connection), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_BYTES',1024), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS',1), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS',120):
            h.proxy()
        self.assertEqual(len(calls), 2)
    def test_request_fingerprint_uses_client_request_id_and_body_hash(self):
        headers={'X-Client-Request-Id':'turn-a'}
        first,explicit=g.request_fingerprint('key-a','POST','/v1/responses',b'{"x":1}',headers)
        repeated,_=g.request_fingerprint('key-a','POST','/v1/responses',b'{"x":1}',headers)
        changed_body,_=g.request_fingerprint('key-a','POST','/v1/responses',b'{"x":2}',headers)
        changed_key,_=g.request_fingerprint('key-b','POST','/v1/responses',b'{"x":1}',headers)
        self.assertTrue(explicit); self.assertEqual(first,repeated)
        self.assertNotEqual(first,changed_body); self.assertNotEqual(first,changed_key)
    def test_request_fingerprint_falls_back_to_exact_body(self):
        first,explicit=g.request_fingerprint('key-a','POST','/v1/responses',b'same',{})
        repeated,_=g.request_fingerprint('key-a','POST','/v1/responses',b'same',{})
        changed,_=g.request_fingerprint('key-a','POST','/v1/responses',b'changed',{})
        self.assertFalse(explicit); self.assertEqual(first,repeated); self.assertNotEqual(first,changed)
    def test_attempt_budget_is_shared_across_client_reconnects(self):
        tracker=g.RequestAttemptTracker(600); fingerprint='request-a'
        for expected in range(1,6):
            state,_=tracker.begin(fingerprint,True,5); self.assertEqual(state,'ok')
            self.assertEqual(tracker.reserve_attempt(fingerprint,5),expected)
            tracker.finish(fingerprint,True,completed=False)
        state,attempts=tracker.begin(fingerprint,True,5)
        self.assertEqual((state,attempts),('exhausted',5))
    def test_zero_attempt_budget_allows_reconnects_without_a_ceiling(self):
        tracker=g.RequestAttemptTracker(600); fingerprint='request-a'
        for expected in range(1,8):
            state,_=tracker.begin(fingerprint,True,0); self.assertEqual(state,'ok')
            self.assertEqual(tracker.reserve_attempt(fingerprint,0),expected)
            tracker.finish(fingerprint,True,completed=False)
        self.assertIsNone(tracker.remaining(fingerprint,0))
    def test_zero_time_budget_is_unlimited(self):
        self.assertTrue(g.budget_available(None))
        self.assertIsNone(g.budget_remaining(None))

    def test_shutdown_does_not_wait_for_request_threads(self):
        self.assertEqual(g.QuotaHTTPServer.request_queue_size, 4096)
        self.assertTrue(g.QuotaHTTPServer.daemon_threads)
        self.assertFalse(g.QuotaHTTPServer.block_on_close)
        g.SHUTDOWN_EVENT.set()
        started=time.monotonic()
        self.assertTrue(g.wait_before_retry(None))
        self.assertLess(time.monotonic()-started,0.1)
    def test_attempt_tracker_blocks_in_flight_and_completed_duplicates(self):
        tracker=g.RequestAttemptTracker(600); fingerprint='request-a'
        self.assertEqual(tracker.begin(fingerprint,True,5)[0],'ok')
        self.assertEqual(tracker.begin(fingerprint,True,5)[0],'in_flight')
        self.assertEqual(tracker.reserve_attempt(fingerprint,5),1)
        tracker.finish(fingerprint,True,completed=True)
        self.assertEqual(tracker.begin(fingerprint,True,5)[0],'completed')
    def test_changing_client_id_does_not_bypass_in_flight_guard(self):
        first, first_id=g.request_fingerprint('key','POST','/v1/responses',b'same',{'X-Client-Request-Id':'first'})
        second, second_id=g.request_fingerprint('key','POST','/v1/responses',b'same',{'X-Client-Request-Id':'second'})
        self.assertEqual(first,second)
        tracker=g.RequestAttemptTracker(600)
        self.assertEqual(tracker.begin(first,first_id,0)[0],'ok')
        self.assertEqual(tracker.begin(second,second_id,0)[0],'in_flight')
        tracker.finish(first,first_id,completed=True)
        self.assertEqual(tracker.begin(first,first_id,0)[0],'completed')
        self.assertEqual(tracker.begin(second,second_id,0)[0],'ok')
    def test_failed_request_cools_down_then_gets_a_new_attempt_budget(self):
        tracker=g.RequestAttemptTracker(600)
        with mock.patch.object(g.time,'monotonic',return_value=100):
            self.assertEqual(tracker.begin('request','first',3)[0],'ok')
            tracker.reserve_attempt('request',3)
            tracker.finish('request','first',failed=True)
            self.assertEqual(tracker.begin('request','second',3)[0],'exhausted')
        with mock.patch.object(g.time,'monotonic',return_value=101+g.UPSTREAM_FAILURE_COOLDOWN_SECONDS):
            self.assertEqual(tracker.begin('request','second',3),('ok',0))
            self.assertEqual(tracker.reserve_attempt('request',3),1)
    def test_retry_backoff_is_exponential_capped_and_cancellable(self):
        with mock.patch.object(g,'UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS',2),mock.patch.object(g,'UPSTREAM_RETRY_MAX_BACKOFF_SECONDS',10),mock.patch.object(g.random,'uniform',return_value=1):
            self.assertEqual([g.retry_pause(None,attempt) for attempt in (1,2,3,10)],[2,4,8,10])
            cancelled=threading.Event();cancelled.set()
            self.assertTrue(g.wait_before_retry(None,10,cancelled))
    def test_repeated_failure_stops_after_three_attempts_across_changed_client_ids(self):
        c=g.db();g.ensure_policy(c,g.key_hash('proxy-key'),'proxy-key');c.close()
        body=b'{"model":"model-a","max_output_tokens":1}'
        calls=[]
        class Response:
            status=502
            def __init__(self):self.body=io.BytesIO(b'failed')
            def read(self,size=-1):return self.body.read(size)
            def getheaders(self):return []
            def close(self):pass
        class Connection:
            def __init__(self,*args,**kwargs):self.sock=None
            def request(self,*args,**kwargs):calls.append(1)
            def getresponse(self):return Response()
            def close(self):pass
        with mock.patch.object(g.http.client,'HTTPConnection',Connection),mock.patch.object(g,'UPSTREAM_HEADER_RETRY_ATTEMPTS',2),mock.patch.object(g,'UPSTREAM_MAX_ATTEMPTS_PER_REQUEST',3),mock.patch.object(g,'UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS',0):
            for request_id in ('first','changed-id'):
                handler=object.__new__(g.Gateway)
                handler.path='/v1/responses';handler.command='POST';handler.request_version='HTTP/1.1';handler.requestline='POST /v1/responses HTTP/1.1'
                handler.headers={'Authorization':'Bearer proxy-key','Content-Length':str(len(body)),'X-Client-Request-Id':request_id}
                handler.rfile=io.BytesIO(body);handler.wfile=io.BytesIO();handler.client_address=('127.0.0.1',12345)
                handler.proxy()
            self.assertIn(b'502',handler.wfile.getvalue().split(b'\r\n',1)[0])
            self.assertIn(b'failed',handler.wfile.getvalue())
        self.assertEqual(len(calls),3)
        c=g.db();self.assertEqual(c.execute("select count(*) from reservations where state='active'").fetchone()[0],0);c.close()
    def test_client_disconnect_cancels_blocked_upstream_without_replay(self):
        c=g.db();g.ensure_policy(c,g.key_hash('proxy-key'),'proxy-key');c.close()
        entered=threading.Event();release=threading.Event();calls=[]
        class Upstream(g.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                calls.append(1);entered.set();release.wait(5)
                try:
                    self.send_response(503);self.end_headers()
                except OSError:
                    pass
            def log_message(self,*args): pass
        upstream=g.QuotaHTTPServer(('127.0.0.1',0),Upstream)
        gateway=g.QuotaHTTPServer(('127.0.0.1',0),g.Gateway)
        workers=[threading.Thread(target=server.serve_forever,daemon=True) for server in (upstream,gateway)]
        monitor=threading.Thread(target=g.monitor_clients,daemon=True)
        for worker in workers:worker.start()
        monitor.start()
        try:
            with mock.patch.object(g,'UPSTREAM','http://127.0.0.1:'+str(upstream.server_port)):
                client=socket.create_connection(gateway.server_address,timeout=3)
                body=b'{"model":"model-a","max_output_tokens":1}'
                client.sendall(b'POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer proxy-key\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)
                self.assertTrue(entered.wait(3));client.close()
                deadline=time.monotonic()+3
                while time.monotonic()<deadline:
                    with g.ACTIVE_REQUESTS_LOCK:
                        active=len(g.ACTIVE_REQUESTS)
                    if not active:break
                    time.sleep(0.05)
                self.assertEqual(active,0)
                self.assertEqual(len(calls),1)
                c=g.db();self.assertEqual(c.execute("select count(*) from reservations where state='active'").fetchone()[0],0);c.close()
        finally:
            release.set();g.SHUTDOWN_EVENT.set();monitor.join(2)
            for server in (gateway,upstream):server.shutdown();server.server_close()
            for worker in workers:worker.join(2)
    def test_large_request_policy_allows_one_bounded_retry(self):
        with mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_BYTES',1024),mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS',240),mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS',1),mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS',480):
            policy=g.upstream_retry_policy(1024)
        self.assertEqual(policy,{'large':True,'local_attempts':2,'budget_seconds':480,'attempt_timeout_seconds':240})

    def test_zero_retry_setting_is_unlimited_and_negative_disables_replay(self):
        with mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_BYTES',1024), \
             mock.patch.object(g,'UPSTREAM_HEADER_RETRY_ATTEMPTS',0), \
             mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS',0):
            self.assertEqual(g.upstream_retry_policy(1023)['local_attempts'],0)
            self.assertEqual(g.upstream_retry_policy(1024)['local_attempts'],0)
        with mock.patch.object(g,'UPSTREAM_HEADER_RETRY_ATTEMPTS',-1):
            self.assertEqual(g.upstream_retry_policy(1)['local_attempts'],1)

    def test_large_request_header_replay_requires_transport_error(self):
        self.assertTrue(g.is_retryable_header_error(ConnectionError('reset by peer'), True))
        self.assertTrue(g.is_retryable_header_error(http.client.IncompleteRead(b''), True))
        self.assertFalse(g.is_retryable_header_error(ValueError('bad request'), True))
    def test_small_request_policy_allows_five_bounded_attempts(self):
        with mock.patch.object(g,'UPSTREAM_LARGE_REQUEST_BYTES',1024),mock.patch.object(g,'UPSTREAM_HEADER_RETRY_ATTEMPTS',4),mock.patch.object(g,'UPSTREAM_HEADER_RETRY_BUDGET_SECONDS',120),mock.patch.object(g,'UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS',90):
            policy=g.upstream_retry_policy(1023)
        self.assertEqual(policy,{'large':False,'local_attempts':5,'budget_seconds':120,'attempt_timeout_seconds':90})
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
    def test_init_normalizes_legacy_period_end_to_cycle_rule(self):
        reset_at=1788193800; c=g.db(); p=g.ensure_policy(c,g.key_hash('user-a'),'user-a')
        c.execute('update policies set period_id=?,period_started_at=?,period_ends_at=? where key_hash=?',('old-reset',reset_at,g.add_months(reset_at,1),p['key_hash'])); c.close()
        g.init_db(); c=g.db(); row=c.execute('select * from policies where key_hash=?',(p['key_hash'],)).fetchone(); c.close()
        self.assertEqual(row['period_ends_at'],g.calendar_period_bounds(reset_at)[1])
    def test_init_migrates_multi_month_cycle_to_fixed_days(self):
        reset_at=1788193800; c=g.db(); p=g.ensure_policy(c,g.key_hash('user-a'),'user-a')
        c.execute("update policies set cycle_months=3,cycle_type='calendar_month',period_started_at=?,period_ends_at=? where key_hash=?",(reset_at,g.add_months(reset_at,3),p['key_hash'])); c.close()
        g.init_db(); c=g.db(); row=c.execute('select * from policies where key_hash=?',(p['key_hash'],)).fetchone(); c.close()
        self.assertEqual((row['cycle_type'],row['cycle_days']),('fixed_days',90))
        self.assertEqual(row['period_ends_at'],reset_at+90*24*60*60)
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

    def test_resp_read_collects_short_bulk_reads(self):
        class ShortReader:
            def __init__(self, payload):
                self.payload = io.BytesIO(payload)

            def read(self, size=-1):
                if size < 0:
                    return self.payload.read(size)
                return self.payload.read(min(size, 2))

            def readline(self):
                return self.payload.readline()

        subscriber = g.UsageSubscriber()
        stream = ShortReader(b'$8\r\nabcdefgh\r\n')
        self.assertEqual(subscriber.resp_read(stream), b'abcdefgh')

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

    def test_duplicate_usage_event_does_not_settle_newer_reservation(self):
        h=object.__new__(g.Gateway); key='settle-key'
        c=g.db(); g.ensure_policy(c,g.key_hash(key),key); c.close()
        first=h.acquire(key,'model-a',1_000_000)[0]
        second=h.acquire(key,'model-a',1_000_000)[0]
        event={'api_key':key,'model':'model-a','request_id':'event-a','timestamp':'2026-08-31T09:00:00+08:00','input_tokens':1,'output_tokens':1}
        subscriber=g.UsageSubscriber()
        self.assertEqual(subscriber.settle(event),'inserted')
        self.assertEqual(subscriber.settle(event),'duplicate')
        c=g.db(); rows=list(c.execute("select id,state from reservations where id in (?,?) order by created_at,id",(first,second))); c.close()
        self.assertEqual([r['state'] for r in rows],['settled','active'])
        h.release(second)

    def test_zero_token_failure_does_not_write_ledger_or_consume_live_hold(self):
        handler=object.__new__(g.Gateway);key='settle-key'
        c=g.db();g.ensure_policy(c,g.key_hash(key),key);c.close()
        reservation=handler.acquire(key,'model-a',1_000_000)[0]
        subscriber=g.UsageSubscriber()
        event={'api_key':key,'model':'model-a','request_id':'failed-attempt','failed':True,'tokens':{'input_tokens':0,'output_tokens':0}}
        self.assertEqual(subscriber.settle(event),'ignored')
        self.assertEqual(subscriber.settle(event,replay=True),'ignored')
        c=g.db()
        self.assertEqual(c.execute('select count(*) from ledger').fetchone()[0],0)
        self.assertEqual(c.execute("select state from reservations where id=?",(reservation,)).fetchone()[0],'active')
        c.close()
        event.update(request_id='successful-attempt',failed=False,tokens={'input_tokens':10,'output_tokens':5})
        self.assertEqual(subscriber.settle(event),'inserted')
        c=g.db()
        self.assertEqual(c.execute('select sum(cost_micro) from ledger').fetchone()[0],200)
        self.assertEqual(c.execute("select state from reservations where id=?",(reservation,)).fetchone()[0],'settled')
        c.close()

    def _keeper_db_with_events(self, directory, key, rows):
        path=os.path.join(directory,'keeper.db'); c=sqlite3.connect(path)
        c.execute('''create table usage_events(
            id integer primary key, api_group_key text, request_id text,
            timestamp text, created_at text, model text, model_alias text,
            input_tokens integer, output_tokens integer, cache_read_tokens integer,
            cache_creation_tokens integer, failed numeric)''')
        for row in rows:
            c.execute('insert into usage_events values(?,?,?,?,?,?,?,?,?,?,?,?)',(
                row['id'],key,row.get('request_id',''),row.get('timestamp','2026-08-31T09:00:00+08:00'),
                row.get('created_at','2026-08-31T09:00:01+08:00'),row.get('model','model-a'),
                row.get('model_alias','model-a'),row.get('input_tokens',1),row.get('output_tokens',1),
                row.get('cache_read_tokens',0),row.get('cache_creation_tokens',0),row.get('failed',0)))
        c.commit(); c.close(); return path

    def test_keeper_usage_replay_advances_checkpoint_and_releases_reservation(self):
        h=object.__new__(g.Gateway); key='keeper-key'
        c=g.db(); g.ensure_policy(c,g.key_hash(key),key); c.close()
        rid=h.acquire(key,'model-a',1_000_000)[0]
        old_db,old_batch,old_interval=g.KEEPER_DB,g.KEEPER_USAGE_BATCH_SIZE,g.KEEPER_USAGE_SYNC_INTERVAL_SECONDS
        try:
            with tempfile.TemporaryDirectory() as directory:
                g.KEEPER_DB=self._keeper_db_with_events(directory,key,[
                    {
                        'id':7,
                        'request_id':'keeper-event',
                        'timestamp':g.iso_time(g.now()+60),
                        'created_at':g.iso_time(g.now()+61),
                    }
                ])
                g.KEEPER_USAGE_BATCH_SIZE=10; g.KEEPER_USAGE_SYNC_INTERVAL_SECONDS=0
                result=g.sync_keeper_usage(force=True)
                self.assertEqual(result['inserted'],1); self.assertEqual(result['checkpoint'],7)
                c=g.db(); active=c.execute("select count(*) from reservations where state='active'").fetchone()[0]; ledger=c.execute('select count(*) from ledger').fetchone()[0]; checkpoint=c.execute("select value from settings where key='keeper_usage_event_id'").fetchone()[0]; c.close()
                self.assertEqual((active,ledger,checkpoint),(0,1,'7'))
                # Replaying from an old checkpoint is idempotent and must not
                # release a new request's reservation.
                h.acquire(key,'model-a',1_000_000)
                g.setting_set('keeper_usage_event_id',0)
                replay=g.sync_keeper_usage(force=True)
                self.assertEqual(replay['duplicate'],1)
                c=g.db(); self.assertEqual(c.execute("select count(*) from reservations where state='active'").fetchone()[0],1); c.close()
        finally:
            g.KEEPER_DB,g.KEEPER_USAGE_BATCH_SIZE,g.KEEPER_USAGE_SYNC_INTERVAL_SECONDS=old_db,old_batch,old_interval
            c=g.db(); c.execute("update reservations set state='expired'"); c.execute("update periods set reserved_micro=0"); c.close()

if __name__=='__main__': unittest.main()

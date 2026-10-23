import contextlib
import http.client
import io
import os
import tempfile
import threading
import unittest
from unittest import mock
from test_gateway import g


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_db = g.DB_PATH
        g.DB_PATH = os.path.join(self.tmp.name, 'test.db')
        g.REQUEST_ATTEMPTS.clear()
        g.SHUTDOWN_EVENT.clear()
        g.UPSTREAM_QUEUE = g.ConcurrencyQueue(10)
        g.init_db()
        c = g.db()
        g.ensure_policy(c, g.key_hash('test-key'), 'test-key')
        c.execute('INSERT INTO prices VALUES(?,?,?,?,?,?,?)', ('test-model', 1000000, 1000000, 0, 0, 1000000, g.now()))
        c.close()
        self.handler = object.__new__(g.Gateway)
        h = self.handler
        body = b'{"model":"test-model","stream":true}'
        h.path = '/v1/responses'
        h.command = 'POST'
        h.request_version = 'HTTP/1.1'
        h.requestline = 'POST /v1/responses HTTP/1.1'
        h.headers = {'Authorization': 'Bearer test-key', 'Content-Length': str(len(body)), 'X-Client-Request-Id': 'test-turn'}
        h.rfile = io.BytesIO(body)
        h.wfile = io.BytesIO()
        h.client_address = ('127.0.0.1', 1)

    def tearDown(self):
        g.DB_PATH = self.old_db
        self.tmp.cleanup()

    def run_proxy(self, statuses, wait_hook=None, headers=()):
        calls = []
        class Response:
            def __init__(self, status):
                self.status = status
                self.body = io.BytesIO(b'data: {"type":"response.completed"}\n\n' if status == 200 else b'{"error":{"message":"response protection is unavailable"}}')
            def read(self, size=-1): return self.body.read(size)
            def getheaders(self): return list(headers)
            def close(self): pass
        class Connection:
            sock = None
            def __init__(self, *args, **kwargs): self.timeout = kwargs.get('timeout')
            def request(self, *args, **kwargs): calls.append(self.timeout)
            def getresponse(self): return Response(statuses[min(len(calls)-1, len(statuses)-1)])
            def close(self): pass
        with contextlib.ExitStack() as stack:
            for name, value in {
                'UPSTREAM_HEADER_RETRY_ATTEMPTS': 0,
                'UPSTREAM_MAX_ATTEMPTS_PER_REQUEST': 0,
                'UPSTREAM_HEADER_RETRY_BUDGET_SECONDS': 0,
                'UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS': 0,
                'UPSTREAM_TRANSIENT_RECOVERY_SECONDS': 600,
            }.items(): stack.enter_context(mock.patch.object(g, name, value))
            stack.enter_context(mock.patch.object(g.http.client, 'HTTPConnection', Connection))
            wait = stack.enter_context(mock.patch.object(g, 'wait_before_retry', side_effect=wait_hook or (lambda *a: False)))
            self.handler.proxy()
        return calls, wait

    def test_five_temporary_failures_recover_without_client_reconnect(self):
        calls, wait = self.run_proxy([502]*5+[200])
        self.assertEqual(len(calls), 6)
        self.assertEqual(wait.call_count, 5)
        result = self.handler.wfile.getvalue()
        self.assertIn(b'200 OK', result)
        self.assertNotIn(b'409', result)
        self.assertIn(b'response.completed', result)
        self.assertIsNone(calls[0])
        self.assertTrue(all(0 < value <= 600 for value in calls[1:]))

    def test_recovery_deadline_expires_and_releases_slot_and_reservation(self):
        clock = [100.0]
        def advance(*args):
            clock[0] += 200
            return False
        with mock.patch.object(g.time, 'monotonic', side_effect=lambda: clock[0]):
            calls, _ = self.run_proxy([502], advance)
        self.assertEqual(len(calls), 3)
        self.assertEqual(clock[0], 700)
        self.assertIn(b'502 Bad Gateway', self.handler.wfile.getvalue())
        self.assertIn(b'response protection is unavailable', self.handler.wfile.getvalue())
        self.handler.rfile.seek(0)
        self.handler.wfile = io.BytesIO()
        with mock.patch.object(g.time, 'monotonic', return_value=700), mock.patch.object(g.http.client, 'HTTPConnection', side_effect=AssertionError('cooldown must not call upstream')):
            self.handler.proxy()
        self.assertIn(b'502 Bad Gateway', self.handler.wfile.getvalue())
        self.assertIn(b'response protection is unavailable', self.handler.wfile.getvalue())
        self.assertNotIn(b'attempt budget', self.handler.wfile.getvalue())
        self.assertEqual(g.UPSTREAM_QUEUE.active, 0)
        c = g.db()
        self.assertEqual(c.execute("SELECT count(*) FROM reservations WHERE state='active'").fetchone()[0], 0)
        c.close()

    def test_cancellation_during_backoff_does_not_make_another_attempt(self):
        def cancel(*args):
            args[2].set()
            return True
        calls, _ = self.run_proxy([502], cancel)
        self.assertEqual(len(calls), 1)
        self.assertEqual(g.UPSTREAM_QUEUE.active, 0)

    def test_authentication_and_quota_errors_are_not_retried(self):
        for status in (401, 403, 429):
            with self.subTest(status=status):
                self.handler.rfile.seek(0)
                self.handler.wfile = io.BytesIO()
                g.REQUEST_ATTEMPTS.clear()
                calls, wait = self.run_proxy([status])
                self.assertEqual(len(calls), 1)
                wait.assert_not_called()
                self.assertIn(str(status).encode(), self.handler.wfile.getvalue())

    def test_retry_after_is_respected_and_too_long_delay_is_not_retried(self):
        calls, wait = self.run_proxy([502, 200], headers=[('Retry-After', '45')])
        self.assertEqual(len(calls), 2)
        self.assertEqual(wait.call_args.args[-1], 45)
        self.handler.rfile.seek(0)
        self.handler.wfile = io.BytesIO()
        g.REQUEST_ATTEMPTS.clear()
        calls, wait = self.run_proxy([502, 200], headers=[('Retry-After', '900')])
        self.assertEqual(len(calls), 1)
        wait.assert_not_called()

    def test_deadline_does_not_restart_with_each_error(self):
        with mock.patch.object(g, 'UPSTREAM_TRANSIENT_RECOVERY_SECONDS', 600):
            with mock.patch.object(g.time, 'monotonic', return_value=100):
                first = g.transient_retry_deadline(None)
            with mock.patch.object(g.time, 'monotonic', return_value=500):
                self.assertEqual(g.transient_retry_deadline(first), 700)
        with mock.patch.object(g, 'UPSTREAM_TRANSIENT_RECOVERY_SECONDS', 0):
            self.assertIsNone(g.transient_retry_deadline(None))

    def test_real_http_recovers_four_failures_before_forwarding_output(self):
        calls = []
        payload = b'data: {"type":"response.completed"}\n\n'
        class Upstream(g.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                calls.append(1)
                status = 502 if len(calls) <= 4 else 200
                body = b'{"error":{"message":"response protection is unavailable"}}' if status == 502 else payload
                self.send_response(status)
                self.send_header('Content-Type', 'application/json' if status == 502 else 'text/event-stream')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args): pass
        upstream = g.QuotaHTTPServer(('127.0.0.1', 0), Upstream)
        gateway = g.QuotaHTTPServer(('127.0.0.1', 0), g.Gateway)
        threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (upstream, gateway)]
        for thread in threads: thread.start()
        try:
            with contextlib.ExitStack() as stack:
                for name, value in {
                    'UPSTREAM': 'http://127.0.0.1:' + str(upstream.server_port),
                    'UPSTREAM_HEADER_RETRY_ATTEMPTS': 0,
                    'UPSTREAM_MAX_ATTEMPTS_PER_REQUEST': 0,
                    'UPSTREAM_TRANSIENT_RECOVERY_SECONDS': 600,
                    'UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS': 0.005,
                }.items(): stack.enter_context(mock.patch.object(g, name, value))
                client = http.client.HTTPConnection(*gateway.server_address, timeout=3)
                client.request('POST', '/v1/responses', b'{"model":"test-model","stream":true}', {'Authorization': 'Bearer test-key'})
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read(), payload)
                self.assertEqual(len(calls), 5)
                client.close()
        finally:
            for server in (upstream, gateway): server.shutdown(); server.server_close()
            for thread in threads: thread.join(2)

    def test_recovery_deadline_interrupts_blocked_upstream_and_keeps_original_error(self):
        calls = []
        release = threading.Event()
        original = b'{"error":{"message":"response protection is unavailable","type":"internal_error"}}'
        class Upstream(g.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                calls.append(1)
                if len(calls) > 1:
                    release.wait(5)
                    return
                self.send_response(502)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(original)))
                self.end_headers()
                self.wfile.write(original)
            def log_message(self, *args): pass
        upstream = g.QuotaHTTPServer(('127.0.0.1', 0), Upstream)
        gateway = g.QuotaHTTPServer(('127.0.0.1', 0), g.Gateway)
        threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (upstream, gateway)]
        monitor = threading.Thread(target=g.monitor_clients, daemon=True)
        for thread in threads: thread.start()
        monitor.start()
        try:
            with contextlib.ExitStack() as stack:
                for name, value in {
                    'UPSTREAM': 'http://127.0.0.1:' + str(upstream.server_port),
                    'UPSTREAM_HEADER_RETRY_ATTEMPTS': 0,
                    'UPSTREAM_MAX_ATTEMPTS_PER_REQUEST': 0,
                    'UPSTREAM_TRANSIENT_RECOVERY_SECONDS': 0.2,
                    'UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS': 0.005,
                }.items(): stack.enter_context(mock.patch.object(g, name, value))
                client = http.client.HTTPConnection(*gateway.server_address, timeout=2)
                client.request('POST', '/v1/responses', b'{"model":"test-model","stream":true}', {'Authorization': 'Bearer test-key'})
                response = client.getresponse()
                self.assertEqual(response.status, 502)
                self.assertEqual(response.read(), original)
                self.assertEqual(len(calls), 2)
                client.close()
        finally:
            release.set()
            g.SHUTDOWN_EVENT.set()
            monitor.join(2)
            for server in (upstream, gateway): server.shutdown(); server.server_close()
            for thread in threads: thread.join(2)
            g.SHUTDOWN_EVENT.clear()

    def test_started_output_is_not_replayed(self):
        calls = []
        class Response:
            status = 200
            def __init__(self): self.reads = 0
            def getheaders(self): return []
            def read(self, size=-1):
                self.reads += 1
                if self.reads == 1: return b'data: first-output\n\n'
                raise http.client.IncompleteRead(b'partial')
        class Connection:
            sock = None
            def __init__(self, *a, **kw): pass
            def request(self, *a, **kw): calls.append(1)
            def getresponse(self): return Response()
            def close(self): pass
        with mock.patch.object(g.http.client, 'HTTPConnection', Connection), mock.patch.object(g, 'UPSTREAM_TRANSIENT_RECOVERY_SECONDS', 600):
            self.handler.proxy()
        self.assertEqual(len(calls), 1)
        self.assertIn(b'first-output', self.handler.wfile.getvalue())


if __name__ == '__main__': unittest.main()

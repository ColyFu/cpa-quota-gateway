#!/usr/bin/env python3
"""Small, independent quota gateway for CLIProxyAPI.

The gateway intentionally has no import or source dependency on cpa-usage-keeper.
It snapshots the Keeper pricing API into its own SQLite database and keeps all
policy and accounting data there, so upgrading Keeper does not replace quotas.
"""
import argparse, base64, calendar, errno, hashlib, http.client, json, os, random, re, secrets, select, signal, socket, sqlite3, sys, threading, time
from collections import deque
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from failover import ProFailoverGate
from routing import AccountRouter, RoutingUnavailable, session_identity
import unified

try:
    import requests
except ImportError:
    requests = None

MICRO = 1_000_000
BASE_DIR = Path(__file__).resolve().parent
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS policies(
 key_hash TEXT PRIMARY KEY, key_hint TEXT NOT NULL, alias TEXT NOT NULL DEFAULT '',
 monthly_limit_micro INTEGER NOT NULL, rpm INTEGER NOT NULL, concurrency INTEGER NOT NULL,
 enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 cycle_months INTEGER NOT NULL DEFAULT 1, cycle_type TEXT NOT NULL DEFAULT 'calendar_month',
 cycle_days INTEGER NOT NULL DEFAULT 30, period_id TEXT NOT NULL DEFAULT '',
 period_started_at INTEGER NOT NULL DEFAULT 0, period_ends_at INTEGER NOT NULL DEFAULT 0,
 keeper_active INTEGER NOT NULL DEFAULT 1, is_superuser INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS periods(
 key_hash TEXT NOT NULL, period TEXT NOT NULL, spent_micro INTEGER NOT NULL DEFAULT 0,
 reserved_micro INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(key_hash,period));
CREATE TABLE IF NOT EXISTS reservations(
 id TEXT PRIMARY KEY, key_hash TEXT NOT NULL, period TEXT NOT NULL, amount_micro INTEGER NOT NULL,
 state TEXT NOT NULL, model TEXT NOT NULL, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS reservations_active ON reservations(key_hash,state);
CREATE TABLE IF NOT EXISTS ledger(
 event_id TEXT PRIMARY KEY, key_hash TEXT NOT NULL, period TEXT NOT NULL, model TEXT NOT NULL,
 request_id TEXT NOT NULL DEFAULT '', input_tokens INTEGER NOT NULL DEFAULT 0,
 output_tokens INTEGER NOT NULL DEFAULT 0, cache_read_tokens INTEGER NOT NULL DEFAULT 0,
 cache_creation_tokens INTEGER NOT NULL DEFAULT 0, cost_micro INTEGER NOT NULL DEFAULT 0,
 failed INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS request_ticks(key_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS request_ticks_recent ON request_ticks(key_hash,created_at);
CREATE TABLE IF NOT EXISTS prices(
 model TEXT PRIMARY KEY, prompt_micro_per_1m INTEGER NOT NULL, completion_micro_per_1m INTEGER NOT NULL,
 cache_read_micro_per_1m INTEGER NOT NULL, cache_creation_micro_per_1m INTEGER NOT NULL,
 multiplier_ppm INTEGER NOT NULL DEFAULT 1000000, source_updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS ledger_request_unique ON ledger(key_hash,request_id) WHERE request_id <> '';
"""

def env(name, default=""):
    return os.environ.get(name, default).strip()

def int_env(name, default):
    try: return int(env(name, str(default)))
    except ValueError: return default

def float_env(name, default):
    try: return float(env(name, str(default)))
    except ValueError: return default

DB_PATH = env("QUOTA_DB", str(BASE_DIR / "data" / "quota.db"))
UPSTREAM = env("UPSTREAM_URL", "http://127.0.0.1:8317")
UPSTREAM_HEADER_RETRY_ATTEMPTS = max(-1, int_env("UPSTREAM_HEADER_RETRY_ATTEMPTS", 2))
# A zero timeout/budget means that a successful long-running Codex turn is
# allowed to finish instead of being cut off by the gateway.
UPSTREAM_HEADER_RETRY_BUDGET_SECONDS = max(0, int_env("UPSTREAM_HEADER_RETRY_BUDGET_SECONDS", 0))
UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS = max(0, int_env("UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS", 0))
UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS = max(
    0.0, min(60.0, float_env("UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS", 0.5))
)
UPSTREAM_RETRY_MAX_BACKOFF_SECONDS = max(
    UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS, float_env("UPSTREAM_RETRY_MAX_BACKOFF_SECONDS", 30)
)
# Bound recovery after a transient failure; do not time out an initially healthy long turn.
UPSTREAM_TRANSIENT_RECOVERY_SECONDS = max(0, int_env("UPSTREAM_TRANSIENT_RECOVERY_SECONDS", 0))
UPSTREAM_FAILURE_COOLDOWN_SECONDS = max(1, int_env("UPSTREAM_FAILURE_COOLDOWN_SECONDS", 60))
UPSTREAM_STREAM_TIMEOUT_SECONDS = max(0, int_env("UPSTREAM_STREAM_TIMEOUT_SECONDS", 0))
UPSTREAM_MAX_ATTEMPTS_PER_REQUEST = max(0, int_env("UPSTREAM_MAX_ATTEMPTS_PER_REQUEST", 3))
UPSTREAM_REQUEST_FINGERPRINT_TTL_SECONDS = max(
    60, min(3600, int_env("UPSTREAM_REQUEST_FINGERPRINT_TTL_SECONDS", 1200))
)
UPSTREAM_LARGE_REQUEST_BYTES = max(
    64 * 1024, int_env("UPSTREAM_LARGE_REQUEST_BYTES", 2 * 1024 * 1024)
)
UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS = max(
    0, int_env("UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS", 0)
)
UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS = max(
    -1, int_env("UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS", 2)
)
UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS = max(
    0,
    int_env(
        "UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS",
        UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS
        * (1 + UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS),
    ),
)
RETRYABLE_UPSTREAM_STATUSES = frozenset((500, 502, 503, 504))
SHUTDOWN_EVENT = threading.Event()
KEEPER_URL = env("KEEPER_URL", "http://127.0.0.1:8318")
KEEPER_DB = env("KEEPER_DB", str(BASE_DIR.parent / "cpa-usage-keeper" / "data" / "app.db"))
KEEPER_PASSWORD = env("KEEPER_LOGIN_PASSWORD", env("LOGIN_PASSWORD"))
CPA_MANAGEMENT_KEY = env("CPA_MANAGEMENT_KEY")
CPA_CONFIG_PATH = env("CPA_CONFIG_PATH", str(BASE_DIR.parent / "cli-proxy-api" / "config.yaml"))
PRO_FAILOVER_ENABLED = env("PRO_FAILOVER_ENABLED", "true").lower() in ("1", "true", "yes", "on")
PRO_FAILOVER_WINDOW_SECONDS = int_env("PRO_FAILOVER_WINDOW_SECONDS", 600)
DEFAULT_LIMIT = int(round(float(env("DEFAULT_BUDGET_USD", env("DEFAULT_MONTHLY_BUDGET_USD", "1000"))) * MICRO))
DEFAULT_RPM = max(0, int_env("DEFAULT_RPM", 0))
DEFAULT_CONCURRENCY = max(0, int_env("DEFAULT_CONCURRENCY", 0))
GLOBAL_CONCURRENCY = max(0, int_env("GLOBAL_CONCURRENCY", 10))
RESERVATION = int(round(float(env("RESERVATION_USD", "2")) * MICRO))
RESERVATION_TTL = max(0, int_env("RESERVATION_TTL_SECONDS", 0))
DEFAULT_MAX_OUTPUT_TOKENS = int_env("DEFAULT_MAX_OUTPUT_TOKENS", 128000)
KEEPER_SYNC_INTERVAL_SECONDS = max(5, int_env("KEEPER_SYNC_INTERVAL_SECONDS", 60))
KEEPER_USAGE_SYNC_INTERVAL_SECONDS = max(
    5, int_env("KEEPER_USAGE_SYNC_INTERVAL_SECONDS", KEEPER_SYNC_INTERVAL_SECONDS)
)
KEEPER_USAGE_BATCH_SIZE = max(1, min(5000, int_env("KEEPER_USAGE_BATCH_SIZE", 250)))
MAX_REQUEST_BODY_BYTES = max(0, int_env("MAX_REQUEST_BODY_BYTES", 0))
BILLING_TIMEZONE = env("BILLING_TIMEZONE", "Asia/Shanghai")
try: BILLING_ZONE = ZoneInfo(BILLING_TIMEZONE)
except Exception: BILLING_ZONE = timezone.utc
ADMIN_TOKEN = env("QUOTA_ADMIN_TOKEN")
if not ADMIN_TOKEN:
    ADMIN_TOKEN = secrets.token_urlsafe(32)
    print("QUOTA_ADMIN_TOKEN was not set; generated token (set it in the service environment to keep it stable)", file=sys.stderr)

# A standby instance can serve new requests during a zero-downtime rolling
# restart, but must not run a second usage subscriber against the same Redis
# pub/sub channel. Production leaves this disabled (the default).
GATEWAY_STANDBY = env("GATEWAY_STANDBY", "0").lower() in ("1", "true", "yes", "on")

def now(): return int(time.time())
def period_key(ts=None):
    d = datetime.fromtimestamp(ts or time.time(), BILLING_ZONE)
    return f"{d.year:04d}-{d.month:02d}"
def add_months(ts, months):
    d=datetime.fromtimestamp(ts,BILLING_ZONE); index=d.year*12+d.month-1+max(1,int(months)); year,month=divmod(index,12); month+=1
    day=min(d.day,calendar.monthrange(year,month)[1])
    return int(datetime(year,month,day,d.hour,d.minute,d.second,tzinfo=BILLING_ZONE).timestamp())
def calendar_period_bounds(ts=None):
    d=datetime.fromtimestamp(ts or time.time(),BILLING_ZONE)
    start=int(datetime(d.year,d.month,1,tzinfo=BILLING_ZONE).timestamp())
    return start,add_months(start,1)
def fixed_period_bounds(ts, days):
    start=int(ts or time.time()); end=int((datetime.fromtimestamp(start,BILLING_ZONE)+timedelta(days=max(1,int(days)))).timestamp())
    return start,end
def cycle_bounds(ts, cycle_type='calendar_month', cycle_days=30):
    return fixed_period_bounds(ts,cycle_days) if cycle_type=='fixed_days' else calendar_period_bounds(ts)
def next_cycle_end(start, cycle_type, cycle_days):
    return cycle_bounds(start,cycle_type,cycle_days)[1]
def cycle_label(policy):
    return '自然月' if policy['cycle_type']=='calendar_month' else f"{int(policy['cycle_days'])} 天"
def key_hash(key): return hashlib.sha256(key.encode()).hexdigest()
def key_hint(key): return (key[:4] + "..." + key[-4:]) if len(key) >= 10 else ("..." + key[-4:])

def event_unix_time(value):
    """Parse Keeper/CLIProxy timestamps without assuming the host timezone."""
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BILLING_ZONE)
    return int(parsed.timestamp())


def period_start_from_id(period):
    """Extract a comparable start time from persisted period identifiers."""
    text = str(period or '')
    if text.startswith(('reset:', 'cycle:')):
        match = re.match(r'^(?:reset|cycle):(\d+)', text)
        return int(match.group(1)) if match else None
    match = re.match(r'^(\d{4})-(\d{2})$', text)
    if match:
        try:
            return int(datetime(
                int(match.group(1)), int(match.group(2)), 1, tzinfo=BILLING_ZONE
            ).timestamp())
        except ValueError:
            return None
    return None

def request_fingerprint(key, method, path, body, headers):
    """Identify one Codex turn without retaining credentials or request text."""
    client_request_id = str(headers.get("X-Client-Request-Id", "")).strip()
    body_hash = hashlib.sha256(body).hexdigest()
    material = "\0".join((key_hash(key), method.upper(), path, body_hash))
    return hashlib.sha256(material.encode()).hexdigest(), client_request_id

def upstream_retry_policy(body_size):
    large = body_size >= UPSTREAM_LARGE_REQUEST_BYTES
    if large:
        return {
            "large": True,
            "local_attempts": local_attempt_limit(UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS),
            "budget_seconds": UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS,
            "attempt_timeout_seconds": UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS,
        }
    return {
        "large": False,
        "local_attempts": local_attempt_limit(UPSTREAM_HEADER_RETRY_ATTEMPTS),
        "budget_seconds": UPSTREAM_HEADER_RETRY_BUDGET_SECONDS,
        "attempt_timeout_seconds": UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS,
    }


def local_attempt_limit(retries):
    """Translate a retry setting into attempts; zero means no ceiling."""
    if retries < 0:
        return 1
    return 0 if retries == 0 else retries + 1


def local_attempt_available(attempt, limit):
    """Return whether another local attempt may be made (1-based attempt)."""
    return limit <= 0 or attempt < limit


def budget_remaining(deadline):
    """Return remaining retry budget, or None when the budget is unlimited."""
    if deadline is None:
        return None
    return deadline - time.monotonic()


def budget_available(deadline):
    remaining = budget_remaining(deadline)
    return remaining is None or remaining > 0


def transient_retry_deadline(deadline):
    """Start a bounded recovery window once, after the first transient failure."""
    if UPSTREAM_TRANSIENT_RECOVERY_SECONDS <= 0:
        return deadline
    recovery_end = time.monotonic() + UPSTREAM_TRANSIENT_RECOVERY_SECONDS
    return recovery_end if deadline is None else min(deadline, recovery_end)


def retry_after_delay(headers):
    """Read server-directed retry delays without treating quota errors as transient."""
    value = next((v for k, v in headers if k.lower() == 'retry-after'), '')
    try:
        if str(value).strip().isdigit():
            return int(value)
        parsed = parsedate_to_datetime(value)
        return max(0.0, parsed.timestamp() - time.time())
    except (ValueError, TypeError, OverflowError):
        return 0.0


def retry_pause(deadline, attempt=1, minimum_delay=0):
    pause = min(
        UPSTREAM_RETRY_MAX_BACKOFF_SECONDS,
        UPSTREAM_HEADER_RETRY_BACKOFF_SECONDS * (2 ** min(16, max(0, attempt - 1))),
    )
    pause *= random.uniform(0.8, 1.0)
    pause = max(pause, minimum_delay)
    remaining = budget_remaining(deadline)
    if remaining is not None:
        pause = min(pause, max(0.0, remaining))
    return pause


def wait_before_retry(deadline, attempt=1, cancelled=None, minimum_delay=0):
    """Wait for retry backoff, but wake immediately when service stops."""
    until = time.monotonic() + retry_pause(deadline, attempt, minimum_delay)
    while not SHUTDOWN_EVENT.is_set() and not (cancelled and cancelled.is_set()):
        remaining = until - time.monotonic()
        if remaining <= 0:
            return False
        SHUTDOWN_EVENT.wait(min(0.25, remaining))
    return True


def attempt_limit_text(max_attempts):
    return "unlimited" if max_attempts <= 0 else str(max_attempts)


def attempt_budget_available(fingerprint, max_attempts):
    """Whether another shared attempt may be reserved (zero means unlimited)."""
    if max_attempts <= 0:
        return True
    remaining = REQUEST_ATTEMPTS.remaining(fingerprint, max_attempts)
    return remaining is not None and remaining > 0


def is_retryable_header_error(error, large_request=False):
    """Allow large request replay only for failures before a response exists."""
    if not large_request:
        return True
    if isinstance(error, (TimeoutError, ConnectionError, http.client.HTTPException)):
        return True
    retryable_errnos = {
        getattr(errno, name)
        for name in (
            "ECONNABORTED",
            "ECONNREFUSED",
            "ECONNRESET",
            "EHOSTDOWN",
            "EHOSTUNREACH",
            "ENETRESET",
            "ENETUNREACH",
            "EPIPE",
            "ETIMEDOUT",
        )
        if hasattr(errno, name)
    }
    return isinstance(error, OSError) and error.errno in retryable_errnos

class RequestAttemptTracker:
    """Track upstream work across gateway retries and client reconnects."""
    def __init__(self, ttl_seconds, max_entries=10000):
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self.lock = threading.Lock()
        self.entries = {}

    def _purge(self, current):
        expired = [
            fingerprint for fingerprint, entry in self.entries.items()
            if not entry["in_flight"] and current - entry["updated"] >= self.ttl_seconds
        ]
        for fingerprint in expired:
            self.entries.pop(fingerprint, None)
        if len(self.entries) < self.max_entries:
            return
        removable = sorted(
            (
                (entry["updated"], fingerprint)
                for fingerprint, entry in self.entries.items()
                if not entry["in_flight"]
            )
        )
        for _, fingerprint in removable[: max(1, len(self.entries) - self.max_entries + 1)]:
            self.entries.pop(fingerprint, None)

    def begin(self, fingerprint, explicit_id, max_attempts):
        current = time.monotonic()
        with self.lock:
            self._purge(current)
            entry = self.entries.setdefault(
                fingerprint,
                {"attempts": 0, "completed": False, "completed_id": "", "in_flight": False, "updated": current, "retry_at": 0},
            )
            entry["updated"] = current
            if entry["in_flight"]:
                return "in_flight", entry["attempts"]
            if explicit_id and entry["completed"] and explicit_id == entry["completed_id"]:
                return "completed", entry["attempts"]
            if entry["retry_at"] > current:
                return "exhausted", entry["attempts"]
            if entry["retry_at"] or entry["completed"]:
                entry.update(attempts=0, completed=False, completed_id="", retry_at=0)
                entry.pop('upstream_failure', None)
            if max_attempts > 0 and entry["attempts"] >= max_attempts:
                entry["retry_at"] = current + UPSTREAM_FAILURE_COOLDOWN_SECONDS
                return "exhausted", entry["attempts"]
            entry["in_flight"] = True
            return "ok", entry["attempts"]

    def reserve_attempt(self, fingerprint, max_attempts):
        current = time.monotonic()
        with self.lock:
            entry = self.entries.get(fingerprint)
            if not entry or (max_attempts > 0 and entry["attempts"] >= max_attempts):
                return 0
            entry["attempts"] += 1
            entry["updated"] = current
            return entry["attempts"]

    def remaining(self, fingerprint, max_attempts):
        with self.lock:
            entry = self.entries.get(fingerprint)
            attempts = entry["attempts"] if entry else 0
            if max_attempts <= 0:
                return None
            return max(0, max_attempts - attempts)

    def used(self, fingerprint):
        with self.lock:
            entry = self.entries.get(fingerprint)
            return entry["attempts"] if entry else 0

    def save_failure(self, fingerprint, status, headers, body):
        # Keep only bounded upstream error responses, never request text or keys.
        if len(body) > 16384:
            return
        with self.lock:
            entry = self.entries.get(fingerprint)
            if entry is not None:
                entry['upstream_failure'] = (status, tuple(headers), body)

    def failure(self, fingerprint):
        with self.lock:
            entry = self.entries.get(fingerprint)
            return entry.get('upstream_failure') if entry else None

    def cooldown_remaining(self, fingerprint):
        with self.lock:
            entry = self.entries.get(fingerprint)
            if not entry or not entry['retry_at']:
                return UPSTREAM_FAILURE_COOLDOWN_SECONDS
            return max(1, int(entry['retry_at'] - time.monotonic()) + 1)

    def finish(self, fingerprint, explicit_id, completed=False, failed=False):
        current = time.monotonic()
        with self.lock:
            entry = self.entries.get(fingerprint)
            if not entry:
                return
            entry["in_flight"] = False
            entry["updated"] = current
            if completed and explicit_id:
                entry["completed"] = True
                entry["completed_id"] = explicit_id
            elif completed:
                self.entries.pop(fingerprint, None)
            elif failed:
                entry["retry_at"] = current + UPSTREAM_FAILURE_COOLDOWN_SECONDS

    def clear(self):
        with self.lock:
            self.entries.clear()

REQUEST_ATTEMPTS = RequestAttemptTracker(UPSTREAM_REQUEST_FINGERPRINT_TTL_SECONDS)


class ClientRequest:
    def __init__(self, client):
        self.client = client
        self.cancelled = threading.Event()
        self.upstream = None
        self.response = None
        self.completed = False
        self.failed = False
        self.recovery_deadline = None
        self.recovery_expired = False

    def disconnected(self):
        if self.client is None:
            return False
        try:
            readable, _, _ = select.select([self.client], [], [], 0)
            return bool(readable) and self.client.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b''
        except BlockingIOError:
            return False
        except (OSError, ValueError):
            return True

    def cancel(self):
        self.cancelled.set()
        self.close_upstream()

    def expire_recovery(self):
        if not self.recovery_expired:
            self.recovery_expired = True
            self.close_upstream()

    def close_upstream(self):
        upstream_socket = getattr(self.upstream, 'sock', None)
        if upstream_socket is None:
            upstream_socket = getattr(getattr(getattr(self.response, 'fp', None), 'raw', None), '_sock', None)
        if upstream_socket is not None:
            try:
                upstream_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


ACTIVE_REQUESTS = {}
ACTIVE_REQUESTS_LOCK = threading.Lock()


def monitor_clients():
    while not SHUTDOWN_EVENT.wait(0.5):
        with ACTIVE_REQUESTS_LOCK:
            requests = list(ACTIVE_REQUESTS.values())
        for request in requests:
            if request.disconnected() or request.cancelled.is_set():
                request.cancel()
            elif request.recovery_deadline is not None and time.monotonic() >= request.recovery_deadline:
                request.expire_recovery()


class ConcurrencyQueue:
    def __init__(self, limit):
        self.limit = max(0, limit)
        self.condition = threading.Condition()
        self.active = 0
        self.active_by_key = {}
        self.pending_by_key = {}
        self.policies = {}
        self.personal_waiting = deque()
        self.global_waiting = deque()
        self.router = None
        self.tasks = {}
        self.running = {}
        self.active_by_account = {}

    def set_limit(self, limit):
        with self.condition:
            self.limit = max(0, limit)
            self.condition.notify_all()

    def set_policy(self, key_hash, concurrency, is_superuser):
        with self.condition:
            self.policies[key_hash] = {'concurrency': concurrency, 'is_superuser': bool(is_superuser)}
            self.promote_waiters()
            self.condition.notify_all()

    def promote_ticket(self, ticket):
        self.personal_waiting.remove(ticket)
        self.global_waiting.append(ticket)
        self.tasks[ticket[0]]['stage'] = 'global'
        self.pending_by_key[ticket[1]] = self.pending_by_key.get(ticket[1], 0) + 1

    def promote_waiters(self):
        for ticket in list(self.personal_waiting):
            policy = self.policies[ticket[1]]
            claimed = self.active_by_key.get(ticket[1], 0) + self.pending_by_key.get(ticket[1], 0)
            if not policy['concurrency'] or claimed < policy['concurrency']:
                self.promote_ticket(ticket)

    def remove_global_ticket(self, ticket):
        self.global_waiting.remove(ticket)
        self.pending_by_key[ticket[1]] -= 1
        if not self.pending_by_key[ticket[1]]:
            del self.pending_by_key[ticket[1]]

    def next_ticket(self):
        selected = None
        for ticket in self.global_waiting:
            policy = self.policies[ticket[1]]
            if not self.account_ready(ticket):
                continue
            if policy['concurrency'] and self.active_by_key.get(ticket[1], 0) >= policy['concurrency']:
                continue
            if policy['is_superuser']:
                return ticket
            if selected is None:
                selected = ticket
        if selected is not None:
            return selected
        waiting_keys = {ticket[1] for ticket in self.global_waiting}
        waiting_keys.update(ticket[1] for ticket in self.personal_waiting)
        if len(waiting_keys) != 1:
            return None
        if self.global_waiting:
            return next((t for t in self.global_waiting if self.account_ready(t)), None)
        ticket = next((t for t in self.personal_waiting if self.account_ready(t)), None)
        if ticket is None:
            return None
        self.promote_ticket(ticket)
        return ticket

    def account_ready(self, ticket):
        identity = self.tasks[ticket[0]]['account_id']
        return not self.router or self.router.ready(identity, self.active_by_account)

    def acquire(self, request, key_hash='', personal_limit=0, is_superuser=False):
        ticket = (object(), key_hash)
        admitted = False
        with self.condition:
            account_id = ''
            if self.router and getattr(request, 'route_account', True):
                waiting = {}
                for task in self.tasks.values():
                    if task['stage'] != 'active' and task['account_id']:
                        waiting[task['account_id']] = waiting.get(task['account_id'], 0) + 1
                account_id = self.router.choose(key_hash, getattr(request, 'session_hash', ''), getattr(request, 'model', ''), self.active_by_account, waiting)
            request.account_id = account_id
            request.task_ticket = ticket[0]
            self.tasks[ticket[0]] = {'id': secrets.token_hex(8), 'key_hash': key_hash,
                'name': getattr(request, 'alias', '') or '未命名用户', 'model': getattr(request, 'model', ''),
                'created_at': time.time(), 'account_id': account_id, 'stage': 'personal'}
            self.policies.setdefault(key_hash, {'concurrency': personal_limit, 'is_superuser': bool(is_superuser)})
            self.personal_waiting.append(ticket)
            self.promote_waiters()
            try:
                while True:
                    if SHUTDOWN_EVENT.is_set() or request.cancelled.is_set() or request.disconnected():
                        return False
                    if account_id and self.router:
                        account = self.router.account(account_id)
                        if not account or not account['enabled'] or account_id in self.router.unavailable:
                            raise RoutingUnavailable('排队期间认证账号不可用，请重新提交请求')
                    if (not self.limit or self.active < self.limit) and self.next_ticket() is ticket:
                        self.remove_global_ticket(ticket)
                        self.active += 1
                        self.active_by_key[key_hash] = self.active_by_key.get(key_hash, 0) + 1
                        self.tasks[ticket[0]]['stage'] = 'active'
                        self.running[ticket[0]] = ticket
                        if account_id:
                            self.active_by_account[account_id] = self.active_by_account.get(account_id, 0) + 1
                        admitted = True
                        self.condition.notify_all()
                        return True
                    self.condition.wait(0.25)
            finally:
                if not admitted:
                    self.tasks.pop(ticket[0], None)
                    if ticket in self.global_waiting:
                        self.remove_global_ticket(ticket)
                    else:
                        self.personal_waiting.remove(ticket)
                    self.promote_waiters()
                    self.condition.notify_all()

    def release(self, key_hash='', request=None):
        with self.condition:
            if self.active <= 0 or self.active_by_key.get(key_hash, 0) <= 0:
                raise RuntimeError('concurrency slot released without acquisition')
            task_id = getattr(request, 'task_ticket', None) if request else next((i for i, t in self.running.items() if t[1] == key_hash), None)
            if task_id is not None:
                task = self.tasks.pop(task_id)
                self.running.pop(task_id)
                identity = task['account_id']
                if identity:
                    self.active_by_account[identity] -= 1
                    if not self.active_by_account[identity]:
                        del self.active_by_account[identity]
            self.active -= 1
            self.active_by_key[key_hash] -= 1
            if not self.active_by_key[key_hash]:
                del self.active_by_key[key_hash]
            self.promote_waiters()
            self.condition.notify_all()

    def detailed_snapshot(self):
        with self.condition:
            return {**self.snapshot(include_stages=True), 'timestamp': time.time(),
                    'tasks': [dict(self.tasks[t[0]]) for t in list(self.running.values()) + list(self.global_waiting) + list(self.personal_waiting)]}

    def snapshot(self, include_stages=False):
        with self.condition:
            result = {'limit': self.limit, 'active': self.active, 'queued': len(self.personal_waiting) + len(self.global_waiting)}
            if include_stages:
                result.update(personal_queued=len(self.personal_waiting), global_queued=len(self.global_waiting))
            return result


UPSTREAM_QUEUE = ConcurrencyQueue(GLOBAL_CONCURRENCY)

def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=5, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=5000")
    return c
def init_db():
    c = db(); c.executescript(SCHEMA)
    columns={row['name'] for row in c.execute('PRAGMA table_info(policies)')}
    migrations={
        'cycle_months':"ALTER TABLE policies ADD COLUMN cycle_months INTEGER NOT NULL DEFAULT 1",
        'cycle_type':"ALTER TABLE policies ADD COLUMN cycle_type TEXT NOT NULL DEFAULT 'calendar_month'",
        'cycle_days':"ALTER TABLE policies ADD COLUMN cycle_days INTEGER NOT NULL DEFAULT 30",
        'period_id':"ALTER TABLE policies ADD COLUMN period_id TEXT NOT NULL DEFAULT ''",
        'period_started_at':"ALTER TABLE policies ADD COLUMN period_started_at INTEGER NOT NULL DEFAULT 0",
        'period_ends_at':"ALTER TABLE policies ADD COLUMN period_ends_at INTEGER NOT NULL DEFAULT 0",
        'keeper_active':"ALTER TABLE policies ADD COLUMN keeper_active INTEGER NOT NULL DEFAULT 1",
        'is_superuser':"ALTER TABLE policies ADD COLUMN is_superuser INTEGER NOT NULL DEFAULT 0",
    }
    for name,sql in migrations.items():
        if name not in columns: c.execute(sql)
    c.execute("UPDATE policies SET cycle_type='fixed_days',cycle_days=30*cycle_months WHERE cycle_months>1 AND cycle_type='calendar_month'")
    c.execute("UPDATE policies SET cycle_months=1")
    c.execute("UPDATE policies SET cycle_type='calendar_month' WHERE cycle_type NOT IN ('calendar_month','fixed_days') OR cycle_type=''")
    c.execute("UPDATE policies SET cycle_days=30 WHERE cycle_days<1")
    t=now()
    for p in c.execute('SELECT * FROM policies').fetchall():
        start=int(p['period_started_at']); end=int(p['period_ends_at']); pid=p['period_id']
        if start<=0 or end<=start or not pid:
            start,end=cycle_bounds(t,p['cycle_type'],p['cycle_days'])
            pid=period_key(t) if p['cycle_type']=='calendar_month' else f'cycle:{start}'
            c.execute('UPDATE policies SET period_id=?,period_started_at=?,period_ends_at=? WHERE key_hash=?',(pid,start,end,p['key_hash']))
        else:
            expected_end=next_cycle_end(start,p['cycle_type'],p['cycle_days'])
            if end!=expected_end: c.execute('UPDATE policies SET period_ends_at=? WHERE key_hash=?',(expected_end,p['key_hash']))
    c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('last_price_sync','0')")
    c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('last_key_sync','0')")
    c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('keeper_usage_event_id','0')")
    c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('last_keeper_usage_sync','0')")
    c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('global_concurrency',?)", (str(GLOBAL_CONCURRENCY),))
    c.close()
    UPSTREAM_QUEUE.router = AccountRouter(db)

def setting_get(key, default=""):
    c=db()
    try:
        row=c.execute('SELECT value FROM settings WHERE key=?',(key,)).fetchone()
        return str(row['value']) if row else default
    finally: c.close()

def setting_set(key, value):
    c=db()
    try: c.execute('INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)',(key,str(value)))
    finally: c.close()

def load_concurrency_configuration():
    c=db()
    try:
        limit=int(c.execute("SELECT value FROM settings WHERE key='global_concurrency'").fetchone()['value'])
        UPSTREAM_QUEUE.set_limit(limit)
        for policy in c.execute('SELECT key_hash,concurrency,is_superuser FROM policies'):
            UPSTREAM_QUEUE.set_policy(policy['key_hash'],policy['concurrency'],policy['is_superuser'])
    finally: c.close()

FAILOVER_GATE = None
KEEPER_SYNC_LOCK = threading.Lock()

def model_price(c, model):
    r = c.execute("SELECT * FROM prices WHERE model=?", (model,)).fetchone()
    return r

def last_keeper_sync():
    try:
        return int(setting_get('last_price_sync', '0'))
    except (TypeError, ValueError):
        return 0

def maybe_sync_keeper_metadata(force=False):
    """Refresh the local pricing/key snapshot when it is stale.

    The background worker normally keeps this fresh. Request-time checks make
    a newly added or edited Keeper price usable immediately after a cache miss.
    """
    if not force and now() - last_keeper_sync() < KEEPER_SYNC_INTERVAL_SECONDS:
        return False
    return sync_keeper_metadata() is not None
def cost_micro(c, model, it, ot, crt, cwt):
    p = model_price(c, model)
    if not p: return None
    normal = max(0, int(it) - int(crt) - int(cwt))
    raw = normal*p['prompt_micro_per_1m'] + int(ot)*p['completion_micro_per_1m'] + int(crt)*p['cache_read_micro_per_1m'] + int(cwt)*p['cache_creation_micro_per_1m']
    return (raw * p['multiplier_ppm']) // MICRO // MICRO

def ensure_policy(c, kh, key):
    t = now(); hint = key_hint(key); start,end=calendar_period_bounds(t); current=period_key(t)
    c.execute("INSERT OR IGNORE INTO policies(key_hash,key_hint,monthly_limit_micro,rpm,concurrency,created_at,updated_at,cycle_months,cycle_type,cycle_days,period_id,period_started_at,period_ends_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (kh,hint,DEFAULT_LIMIT,DEFAULT_RPM,DEFAULT_CONCURRENCY,t,t,1,'calendar_month',30,current,start,end))
    return refresh_policy_period(c,c.execute("SELECT * FROM policies WHERE key_hash=?", (kh,)).fetchone(),t)

def refresh_policy_period(c, policy, ts=None):
    t=ts or now(); start=int(policy['period_started_at']); end=int(policy['period_ends_at']); cycle_type=policy['cycle_type']; days=max(1,int(policy['cycle_days']))
    if start<=0 or end<=start:
        start,end=cycle_bounds(t,cycle_type,days); pid=period_key(t) if cycle_type=='calendar_month' else f'cycle:{start}'
    elif t>=end:
        start=end; end=next_cycle_end(start,cycle_type,days)
        while t>=end: start=end; end=next_cycle_end(start,cycle_type,days)
        pid=f'cycle:{start}'
    else: return policy
    c.execute('UPDATE policies SET period_id=?,period_started_at=?,period_ends_at=?,updated_at=? WHERE key_hash=?',(pid,start,end,t,policy['key_hash']))
    return c.execute('SELECT * FROM policies WHERE key_hash=?',(policy['key_hash'],)).fetchone()

def reset_policy_periods(c, key_hashes, ts=None):
    t=ts or now(); reset=[]
    for kh in key_hashes:
        p=c.execute('SELECT * FROM policies WHERE key_hash=?',(kh,)).fetchone()
        if not p: continue
        end=next_cycle_end(t,p['cycle_type'],p['cycle_days']); pid=f'reset:{t}:{secrets.token_hex(4)}'
        c.execute('UPDATE policies SET period_id=?,period_started_at=?,period_ends_at=?,updated_at=? WHERE key_hash=?',(pid,t,end,t,kh))
        c.execute('INSERT OR IGNORE INTO periods(key_hash,period) VALUES(?,?)',(kh,pid)); reset.append(kh)
    return reset

def delete_policies(c, key_hashes):
    removed=0
    for kh in key_hashes:
        for table in ('reservations','ledger','request_ticks','periods'):
            c.execute(f'DELETE FROM {table} WHERE key_hash=?',(kh,))
        removed+=c.execute('DELETE FROM policies WHERE key_hash=?',(kh,)).rowcount
    return removed

def event_id(kh, request_id, fallback):
    stable=request_id.strip() if request_id else fallback
    return hashlib.sha256((kh+'|'+stable).encode()).hexdigest()

def expire_reservations(c, t):
    # expires_at=0 is the explicit unlimited-lifetime mode. Normal request
    # cleanup still works when an operator chooses a positive TTL.
    rows=c.execute("SELECT key_hash,period,sum(amount_micro) amount FROM reservations WHERE state='active' AND expires_at>0 AND expires_at<? GROUP BY key_hash,period",(t,)).fetchall()
    c.execute("UPDATE reservations SET state='expired' WHERE state='active' AND expires_at>0 AND expires_at<?",(t,))
    for r in rows:
        c.execute("UPDATE periods SET reserved_micro=max(0,reserved_micro-?) WHERE key_hash=? AND period=?",(r['amount'],r['key_hash'],r['period']))


def period_for_event(c, policy, event_timestamp):
    """Choose the historical period that contains a Keeper event."""
    if event_timestamp is None:
        return policy['period_id']
    candidates = [(int(policy['period_started_at']), policy['period_id'])]
    for row in c.execute(
        'SELECT period FROM periods WHERE key_hash=?', (policy['key_hash'],)
    ):
        start = period_start_from_id(row['period'])
        if start is not None:
            candidates.append((start, row['period']))
    eligible = [item for item in candidates if item[0] <= event_timestamp]
    return max(eligible, key=lambda item: item[0])[1] if eligible else policy['period_id']

def json_response(h, status, obj, extra=None):
    body = json.dumps(obj, ensure_ascii=False).encode()
    h.send_response(status); h.send_header("Content-Type", "application/json"); h.send_header("Content-Length", str(len(body))); h.send_header("Cache-Control", "no-store")
    for k,v in (extra or {}).items(): h.send_header(k, str(v))
    h.send_header("Connection", "close"); h.close_connection=True
    h.end_headers(); h.wfile.write(body)

def iso_time(ts):
    return datetime.fromtimestamp(int(ts),BILLING_ZONE).isoformat(timespec='seconds')

def policy_summaries(c):
    items=[]
    for row in c.execute('SELECT * FROM policies ORDER BY alias,key_hint').fetchall():
        p=refresh_policy_period(c,row)
        usage=c.execute('SELECT spent_micro,reserved_micro FROM periods WHERE key_hash=? AND period=?',(p['key_hash'],p['period_id'])).fetchone()
        spent=int(usage['spent_micro']) if usage else 0; reserved=int(usage['reserved_micro']) if usage else 0
        items.append({'key_hash':p['key_hash'],'key_hint':p['key_hint'],'alias':p['alias'],'limit_usd':p['monthly_limit_micro']/MICRO,'spent_usd':spent/MICRO,'reserved_usd':reserved/MICRO,'used_usd':(spent+reserved)/MICRO,'remaining_usd':max(0,p['monthly_limit_micro']-spent-reserved)/MICRO,'rpm':p['rpm'],'concurrency':p['concurrency'],'is_superuser':bool(p['is_superuser']),'enabled':bool(p['enabled']),'cycle_type':p['cycle_type'],'cycle_days':p['cycle_days'],'cycle_label':cycle_label(p),'period_id':p['period_id'],'period_started_at':iso_time(p['period_started_at']),'period_ends_at':iso_time(p['period_ends_at']),'keeper_active':bool(p['keeper_active'])})
    return items

def user_limit(c, key):
    kh=key_hash(key); c.execute('BEGIN IMMEDIATE'); p=c.execute('SELECT * FROM policies WHERE key_hash=?',(kh,)).fetchone()
    if not p or not p['keeper_active']:
        c.rollback(); return None
    p=refresh_policy_period(c,p); expire_reservations(c,now())
    usage=c.execute('SELECT spent_micro,reserved_micro FROM periods WHERE key_hash=? AND period=?',(kh,p['period_id'])).fetchone()
    spent=int(usage['spent_micro']) if usage else 0; reserved=int(usage['reserved_micro']) if usage else 0
    result={'limit_usd':p['monthly_limit_micro']/MICRO,'used_usd':(spent+reserved)/MICRO,'reset_at':iso_time(p['period_ends_at']),'cycle':{'type':p['cycle_type'],'days':p['cycle_days'] if p['cycle_type']=='fixed_days' else None}}
    c.commit(); return result

def fetch_codex_weekly_usage():
    if not requests or not KEEPER_PASSWORD: raise RuntimeError('Keeper is not configured')
    s=requests.Session(); h={'X-CPA-Usage-Keeper-Request':'fetch'}
    r=s.post(KEEPER_URL+'/api/v1/auth/login',json={'password':KEEPER_PASSWORD},headers=h,timeout=10); r.raise_for_status()
    r=s.get(KEEPER_URL+'/api/v1/quota/inspection',headers=h,timeout=10); r.raise_for_status()
    indexes=[str(x.get('auth_index') or '') for x in r.json().get('results',[]) if x.get('type')=='codex' and x.get('auth_index')]
    if not indexes: raise RuntimeError('no Codex quota is cached in Keeper')
    r=s.post(KEEPER_URL+'/api/v1/quota/cache',json={'auth_indexes':indexes},headers=h,timeout=10); r.raise_for_status()
    for item in r.json().get('items',[]):
        for quota in ((item.get('quota') or {}).get('quota') or []):
            window=quota.get('window') or {}
            if quota.get('key')=='rate_limit.primary_window' and int(window.get('seconds') or 0)==7*24*60*60:
                return {'used_percent':float(quota.get('usedPercent') or 0),'reset_at':str(quota.get('resetAt') or '')}
    raise RuntimeError('weekly Codex quota is unavailable in Keeper')

class UsageSubscriber(threading.Thread):
    daemon = True
    def run(self):
        while True:
            try: self.connect_once()
            except Exception as e: print(f"usage subscriber reconnect: {e}", file=sys.stderr)
            time.sleep(3)
    @staticmethod
    def read_exact(f, size):
        """Read a complete RESP bulk payload from an unbuffered socket file."""
        chunks = []
        remaining = size
        while remaining:
            chunk = f.read(remaining)
            if not chunk:
                raise ConnectionError("redis closed while reading bulk payload")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def resp_read(self, f):
        p = f.read(1)
        if not p: raise ConnectionError("redis closed")
        if p == b'*':
            n = int(f.readline()); return [self.resp_read(f) for _ in range(n)]
        if p == b'$':
            n = int(f.readline());
            if n < 0: return None
            v = self.read_exact(f, n)
            if self.read_exact(f, 2) != b'\r\n':
                raise ConnectionError("invalid RESP bulk terminator")
            return v
        return f.readline().rstrip(b'\r\n')
    def send(self, f, *parts):
        f.write(("*%d\r\n"%len(parts)).encode())
        for x in parts:
            b=x.encode() if isinstance(x,str) else x; f.write(("$%d\r\n"%len(b)).encode()+b+b"\r\n")
        f.flush()
    def connect_once(self):
        s=socket.create_connection((urlsplit(UPSTREAM).hostname or '127.0.0.1', urlsplit(UPSTREAM).port or 8317), timeout=10)
        f=s.makefile('rwb', buffering=0); self.send(f,'AUTH',CPA_MANAGEMENT_KEY); self.resp_read(f); self.send(f,'SUBSCRIBE','usage'); self.resp_read(f); s.settimeout(None)
        while True:
            msg=self.resp_read(f)
            if not isinstance(msg,list) or len(msg)<3: continue
            raw=msg[2]
            if not isinstance(raw,(bytes,bytearray)): continue
            try: self.settle(json.loads(raw.decode()))
            except Exception as e: print(f"usage event ignored: {e}", file=sys.stderr)
    def settle(self, event, replay=False):
        """Apply one usage event exactly once and release its own hold.

        Keeper replay marks historical events separately so an old event cannot
        consume a reservation created after it. A duplicate ledger event must
        never settle a different active reservation.
        """
        if FAILOVER_GATE and not replay:
            FAILOVER_GATE.observe(event)
        key=str(event.get('api_key') or '')
        if not key: return 'ignored'
        kh=key_hash(key); model=str(event.get('model') or 'unknown'); tok=event.get('tokens') or {}
        it=int(tok.get('input_tokens') or event.get('input_tokens') or 0); ot=int(tok.get('output_tokens') or event.get('output_tokens') or 0)
        crt=int(tok.get('cache_read_tokens') or event.get('cache_read_tokens') or 0); cwt=int(tok.get('cache_creation_tokens') or event.get('cache_creation_tokens') or 0)
        if event.get('failed') and not any((
            it, ot, crt, cwt,
            tok.get('total_tokens'), event.get('total_tokens'),
            tok.get('reasoning_tokens'), event.get('reasoning_tokens'),
            tok.get('cached_tokens'), event.get('cached_tokens'),
        )):
            return 'ignored'
        rid=str(event.get('request_id') or ''); stamp=str(event.get('timestamp') or '')
        eid=event_id(kh,rid,stamp+'|'+model+'|'+str(it)+'|'+str(ot))
        c=db(); t=now(); c.execute('BEGIN IMMEDIATE')
        try:
            p=c.execute('SELECT * FROM policies WHERE key_hash=? AND keeper_active=1',(kh,)).fetchone()
            if not p:
                c.rollback(); return 'ignored'
            p=refresh_policy_period(c,p,t)
            event_timestamp=event_unix_time(stamp)
            if replay:
                per=period_for_event(c,p,event_timestamp)
                completed_at=event_unix_time(event.get('_keeper_created_at'))
                cutoff=completed_at if completed_at is not None else event_timestamp
                if cutoff is None:
                    r=c.execute(
                        "SELECT id,amount_micro,period FROM reservations "
                        "WHERE key_hash=? AND period=? AND state='active' "
                        "ORDER BY created_at,id LIMIT 1", (kh,per)
                    ).fetchone()
                else:
                    target_time = event_timestamp if event_timestamp is not None else cutoff
                    r=c.execute(
                        "SELECT id,amount_micro,period FROM reservations "
                        "WHERE key_hash=? AND period=? AND state='active' "
                        "AND created_at<=? ORDER BY ABS(created_at-?),created_at,id LIMIT 1",
                        (kh,per,cutoff,target_time),
                    ).fetchone()
            else:
                r=c.execute(
                    "SELECT id,amount_micro,period FROM reservations "
                    "WHERE key_hash=? AND state='active' ORDER BY created_at,id LIMIT 1", (kh,)
                ).fetchone()
                per=r['period'] if r else p['period_id']
            c.execute("INSERT OR IGNORE INTO periods(key_hash,period) VALUES(?,?)",(kh,per))
            cm=cost_micro(c,model,it,ot,crt,cwt)
            if cm is None and event.get('alias'): cm=cost_micro(c,str(event['alias']),it,ot,crt,cwt)
            if cm is None: raise ValueError(f'missing Keeper price for model {model}')
            cm=max(0,cm)
            inserted=c.execute("INSERT OR IGNORE INTO ledger(event_id,key_hash,period,model,request_id,input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_micro,failed,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(eid,kh,per,model,rid,it,ot,crt,cwt,cm,1 if event.get('failed') else 0,t)).rowcount
            if inserted:
                c.execute("UPDATE periods SET spent_micro=spent_micro+? WHERE key_hash=? AND period=?",(cm,kh,per))
            # Only the transaction that inserted this event owns a reservation.
            # A duplicate replay must never consume a later request's hold.
            if inserted and r:
                c.execute("UPDATE reservations SET state='settled' WHERE id=?",(r['id'],)); c.execute("UPDATE periods SET reserved_micro=max(0,reserved_micro-?) WHERE key_hash=? AND period=?",(r['amount_micro'],kh,r['period']))
            c.commit()
            return 'inserted' if inserted else 'duplicate'
        except Exception:
            c.rollback(); raise
        finally: c.close()


KEEPER_USAGE_SYNC_LOCK = threading.Lock()


def keeper_usage_event(row):
    """Convert one durable Keeper row to the subscriber event shape."""
    def integer(name):
        try:
            return int(row[name] or 0)
        except (TypeError, ValueError):
            return 0

    return {
        'api_key': str(row['api_group_key'] or '').strip(),
        'model': str(row['model'] or row['model_alias'] or 'unknown').strip(),
        'alias': str(row['model_alias'] or '').strip(),
        'request_id': str(row['request_id'] or '').strip(),
        'timestamp': str(row['timestamp'] or '').strip(),
        'input_tokens': integer('input_tokens'),
        'output_tokens': integer('output_tokens'),
        'cache_read_tokens': integer('cache_read_tokens'),
        'cache_creation_tokens': integer('cache_creation_tokens'),
        'failed': bool(integer('failed')),
        '_keeper_created_at': str(row['created_at'] or '').strip(),
        '_keeper_event_id': int(row['id']),
    }


def keeper_usage_checkpoint():
    try:
        return max(0, int(setting_get('keeper_usage_event_id', '0')))
    except (TypeError, ValueError):
        return 0


def sync_keeper_usage(force=False):
    """Replay durable Keeper usage rows missed by the Redis subscriber.

    The checkpoint is advanced only after each complete batch. Replaying a
    committed row is harmless because ``settle`` deduplicates its ledger row
    and only the first insert can release a reservation.
    """
    if not KEEPER_DB or not os.path.exists(KEEPER_DB):
        return None
    if not force:
        try:
            last_sync = int(setting_get('last_keeper_usage_sync', '0'))
        except (TypeError, ValueError):
            last_sync = 0
        if now() - last_sync < KEEPER_USAGE_SYNC_INTERVAL_SECONDS:
            return {'skipped': True, 'checkpoint': keeper_usage_checkpoint()}
    if not KEEPER_USAGE_SYNC_LOCK.acquire(blocking=False):
        return None
    source = None
    try:
        source = sqlite3.connect(f'file:{KEEPER_DB}?mode=ro', uri=True)
        source.row_factory = sqlite3.Row
        subscriber = UsageSubscriber()
        checkpoint = keeper_usage_checkpoint()
        stats = {'inserted': 0, 'duplicate': 0, 'ignored': 0, 'batches': 0}
        while True:
            rows = source.execute(
                'SELECT id,api_group_key,request_id,timestamp,created_at,model,model_alias,'
                'input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,failed '
                'FROM usage_events WHERE id>? ORDER BY id LIMIT ?',
                (checkpoint, KEEPER_USAGE_BATCH_SIZE),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                result = subscriber.settle(keeper_usage_event(row), replay=True)
                stats[result] = stats.get(result, 0) + 1
                checkpoint = int(row['id'])
            setting_set('keeper_usage_event_id', checkpoint)
            stats['batches'] += 1
            if len(rows) < KEEPER_USAGE_BATCH_SIZE:
                break
        setting_set('last_keeper_usage_sync', now())
        stats['checkpoint'] = checkpoint
        if stats['inserted'] or stats['duplicate'] or stats['ignored']:
            print(
                'keeper usage sync: '
                + json.dumps(stats, ensure_ascii=False, sort_keys=True),
                flush=True,
            )
        return stats
    except Exception as exc:
        print(f'keeper usage sync failed at checkpoint {keeper_usage_checkpoint()}: {exc}', file=sys.stderr)
        return None
    finally:
        if source is not None:
            source.close()
        KEEPER_USAGE_SYNC_LOCK.release()

class Gateway(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self, fmt,*args): print(f"{self.client_address[0]} {fmt%args}", file=sys.stderr)
    def auth_key(self):
        m=re.match(r'\s*Bearer\s+(.+)\s*$', self.headers.get('Authorization',''), re.I)
        return m.group(1).strip() if m else ''
    def acquire(self, key, model, amount, enforce_concurrency=True):
        kh=key_hash(key)
        wait_started = None
        last_wait_log = 0.0
        while True:
            c=db(); t=now(); rid=secrets.token_hex(16); wait_seconds=0.0
            try:
                c.execute('BEGIN IMMEDIATE')
                p=c.execute('SELECT * FROM policies WHERE key_hash=? AND keeper_active=1',(kh,)).fetchone()
                if not p: raise KeyError('invalid API key')
                p=refresh_policy_period(c,p,t)
                per=p['period_id']
                expire_reservations(c,t)
                c.execute("DELETE FROM request_ticks WHERE created_at<?",(t-120,))
                c.execute("INSERT OR IGNORE INTO periods(key_hash,period) VALUES(?,?)",(kh,per))
                if not p['enabled']: raise ValueError('disabled')
                recent=c.execute("SELECT count(*) n FROM request_ticks WHERE key_hash=? AND created_at>=?",(kh,t-60)).fetchone()['n']
                inflight=c.execute("SELECT count(*) n FROM reservations WHERE key_hash=? AND state='active'",(kh,)).fetchone()['n']
                used=c.execute("SELECT spent_micro,reserved_micro FROM periods WHERE key_hash=? AND period=?",(kh,per)).fetchone()
                if p['rpm']>0 and recent>=p['rpm']:
                    oldest=c.execute("SELECT min(created_at) created_at FROM request_ticks WHERE key_hash=? AND created_at>=?",(kh,t-60)).fetchone()['created_at']
                    wait_seconds=max(0.25, float(oldest or t)+60-t)
                elif enforce_concurrency and p['concurrency']>0 and inflight>=p['concurrency']:
                    expiry=c.execute("SELECT min(expires_at) expires_at FROM reservations WHERE key_hash=? AND state='active' AND expires_at>0",(kh,)).fetchone()['expires_at']
                    wait_seconds=max(0.25, float(expiry)-t) if expiry and expiry>t else 1.0
                if wait_seconds:
                    # Release SQLite before sleeping so a completion can settle
                    # the reservation and wake the next waiting request.
                    c.commit()
                else:
                    if amount and used['spent_micro']+used['reserved_micro']+amount>p['monthly_limit_micro']:
                        raise PermissionError('quota')
                    c.execute("INSERT INTO request_ticks(key_hash,created_at) VALUES(?,?)",(kh,t))
                    expires_at=t+RESERVATION_TTL if RESERVATION_TTL>0 else 0
                    if amount:
                        c.execute("INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?)",(rid,kh,per,amount,'active',model,t,expires_at)); c.execute("UPDATE periods SET reserved_micro=reserved_micro+? WHERE key_hash=? AND period=?",(amount,kh,per))
                    c.commit(); return rid,p
            except Exception:
                c.rollback(); raise
            finally: c.close()
            if wait_seconds:
                current=time.monotonic()
                if wait_started is None:
                    wait_started=current
                    last_wait_log=current
                    print(
                        f'quota capacity wait key={key_hint(key)} '
                        f'rpm={p["rpm"]} concurrency={p["concurrency"]}',
                        file=sys.stderr,
                    )
                elif current-last_wait_log>=30:
                    last_wait_log=current
                    print(
                        f'quota capacity still waiting key={key_hint(key)} '
                        f'elapsed={int(current-wait_started)}s',
                        file=sys.stderr,
                    )
                # Poll frequently enough to notice a released reservation, but
                # never turn a configured limit into a request failure.
                time.sleep(min(max(wait_seconds,0.25),5.0))
    def release(self, rid):
        if not rid:return
        c=db(); c.execute('BEGIN IMMEDIATE'); r=c.execute("SELECT * FROM reservations WHERE id=? AND state='active'",(rid,)).fetchone()
        if r:
            c.execute("UPDATE reservations SET state='expired' WHERE id=?",(rid,)); c.execute("UPDATE periods SET reserved_micro=max(0,reserved_micro-?) WHERE key_hash=? AND period=?",(r['amount_micro'],r['key_hash'],r['period']))
        c.commit(); c.close()
    def admin_authenticated(self):
        if secrets.compare_digest(self.headers.get('X-Quota-Admin-Token',''),ADMIN_TOKEN): return True
        auth=self.headers.get('Authorization','')
        if auth.lower().startswith('basic '):
            try:
                user,password=base64.b64decode(auth.split(None,1)[1]).decode().split(':',1)
                return user=='admin' and secrets.compare_digest(password,ADMIN_TOKEN)
            except Exception: return False
        return unified.session_admin(self, unified.namespace(globals()))
    def do_admin(self):
        if not self.admin_authenticated(): json_response(self,401,{'error':'admin authentication required'},{'WWW-Authenticate':'Basic realm="CPA Quota"'}); return True
        if self.command in ('POST','PUT','PATCH','DELETE') and self.headers.get('X-Quota-Admin-Request')!='1':
            json_response(self,403,{'error':'missing admin request confirmation header'}); return True
        path=self.path.split('?',1)[0]
        if path in ('/admin','/admin/') and self.command=='GET':
            body=Path(__file__).with_name('admin.html').read_bytes(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Content-Length',str(len(body))); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body); return True
        if unified.admin_route(self, unified.namespace(globals()), path): return True
        c=db()
        if path=='/admin/concurrency':
            if self.command=='GET':
                c.close(); json_response(self,200,UPSTREAM_QUEUE.snapshot(include_stages=True)); return True
            if self.command!='PUT':
                c.close(); json_response(self,405,{'error':'method not allowed'},{'Allow':'GET, PUT'}); return True
            try:
                length=int(self.headers.get('Content-Length','0'))
                if not 0<length<=16384: raise ValueError('invalid body size')
                payload=json.loads(self.rfile.read(length))
                if not isinstance(payload,dict) or type(payload.get('global_limit')) is not int or payload['global_limit']<0:
                    raise ValueError('global_limit must be a non-negative integer')
            except (ValueError,TypeError):
                c.close(); json_response(self,400,{'error':'global_limit must be a non-negative integer'}); return True
            try:
                with UPSTREAM_QUEUE.condition:
                    c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('global_concurrency',?)",(str(payload['global_limit']),))
                    UPSTREAM_QUEUE.set_limit(payload['global_limit'])
            finally: c.close()
            json_response(self,200,UPSTREAM_QUEUE.snapshot(include_stages=True)); return True
        if path=='/admin/health':
            sync={r['key']:r['value'] for r in c.execute("SELECT key,value FROM settings WHERE key IN ('last_price_sync','last_key_sync','last_keeper_usage_sync','keeper_usage_event_id')")}
            json_response(self,200,{'ok':True,'db':DB_PATH,'last_price_sync':int(sync.get('last_price_sync','0')),'last_key_sync':int(sync.get('last_key_sync','0')),'last_keeper_usage_sync':int(sync.get('last_keeper_usage_sync','0')),'keeper_usage_event_id':int(sync.get('keeper_usage_event_id','0')),'concurrency':UPSTREAM_QUEUE.snapshot(include_stages=True)}); c.close(); return True
        if path=='/admin/sync' and self.command=='POST':
            c.close(); result=sync_keeper_metadata()
            if result is None: json_response(self,502,{'error':'Keeper metadata sync failed'}); return True
            json_response(self,200,result); return True
        if path=='/admin/policies' and self.command=='GET':
            rows=[dict(x) for x in c.execute('SELECT key_hash,key_hint,alias,monthly_limit_micro/1000000.0 limit_usd,rpm,concurrency,is_superuser,enabled,updated_at FROM policies ORDER BY key_hint')]; json_response(self,200,{'policies':rows}); c.close(); return True
        if path=='/admin/summary' and self.command=='GET':
            c.execute('BEGIN IMMEDIATE'); rows=policy_summaries(c); c.commit()
            sync={r['key']:int(r['value']) for r in c.execute("SELECT key,value FROM settings WHERE key IN ('last_price_sync','last_key_sync')")}
            latest_sync=max(sync.values()) if sync else 0
            meta={'total_keys':len(rows),'active_keys':sum(1 for x in rows if x['keeper_active']),'enabled_keys':sum(1 for x in rows if x['enabled'] and x['keeper_active']),'used_usd':sum(x['used_usd'] for x in rows if x['keeper_active']),'limit_usd':sum(x['limit_usd'] for x in rows if x['keeper_active']),'price_count':c.execute('SELECT count(*) n FROM prices').fetchone()['n'],'last_sync_at':iso_time(latest_sync) if latest_sync>0 else ''}
            json_response(self,200,{'timezone':BILLING_TIMEZONE,'meta':meta,'items':rows,'concurrency':UPSTREAM_QUEUE.snapshot(include_stages=True)}); c.close(); return True
        if path=='/admin/policies/enabled' and self.command=='PUT':
            try:
                n=int(self.headers.get('Content-Length','0'))
                if not 0<n<=16384: raise ValueError('invalid body size')
                body=json.loads(self.rfile.read(n))
                if not isinstance(body,dict) or type(body.get('enabled')) is not bool:
                    raise ValueError('enabled must be a boolean')
            except (ValueError,TypeError):
                c.close(); json_response(self,400,{'error':'enabled must be a boolean'}); return True
            try:
                c.execute('BEGIN IMMEDIATE')
                count=c.execute('UPDATE policies SET enabled=?,updated_at=?',(int(body['enabled']),now())).rowcount
                c.commit()
            finally: c.close()
            json_response(self,200,{'updated_count':count,'enabled':body['enabled']}); return True
        if path=='/admin/policy' and self.command=='PUT':
            try:
                n=int(self.headers.get('Content-Length','0'))
                if not 0<n<=16384: raise ValueError('invalid body size')
                body=json.loads(self.rfile.read(n))
                if not isinstance(body,dict): raise ValueError('expected an object')
            except (ValueError,TypeError):
                c.close(); json_response(self,400,{'error':'invalid policy body'}); return True
            key=str(body.get('api_key') or '')
            kh=str(body.get('key_hash') or (key_hash(key) if key else '')); c.execute('BEGIN IMMEDIATE'); row=c.execute('SELECT * FROM policies WHERE key_hash=?',(kh,)).fetchone()
            if not row: c.rollback(); c.close(); json_response(self,404,{'error':'policy not found; send api_key once to create'}); return True
            limit_value=body.get('limit_usd',body.get('monthly_limit_usd')); cycle_type=str(body.get('cycle_type') or row['cycle_type']); cycle_days=int(body.get('cycle_days') or row['cycle_days'])
            if (limit_value is not None and float(limit_value)<0) or ('rpm' in body and int(body['rpm'])<0) or ('concurrency' in body and (type(body['concurrency']) is not int or body['concurrency']<0)) or ('is_superuser' in body and type(body['is_superuser']) is not bool) or cycle_type not in ('calendar_month','fixed_days') or not 1<=cycle_days<=3650:
                c.rollback(); c.close(); json_response(self,400,{'error':'invalid policy values'}); return True
            cols=[]; vals=[]
            for name, val in [('monthly_limit_micro', (float(limit_value)*MICRO if limit_value is not None else None)),('rpm',body.get('rpm')),('concurrency',body.get('concurrency')),('is_superuser',body.get('is_superuser')),('enabled',body.get('enabled'))]:
                if val is not None: cols.append(name+'=?'); vals.append(int(val))
            cycle_changed=cycle_type!=row['cycle_type'] or (cycle_type=='fixed_days' and cycle_days!=row['cycle_days'])
            if 'cycle_type' in body or 'cycle_days' in body:
                cols.extend(('cycle_type=?','cycle_days=?')); vals.extend((cycle_type,cycle_days if cycle_type=='fixed_days' else 30))
            if cols:
                vals += [now(),kh]; c.execute('UPDATE policies SET '+','.join(cols)+',updated_at=? WHERE key_hash=?',vals)
                if cycle_changed: reset_policy_periods(c,[kh])
                c.commit(); out=next(x for x in policy_summaries(c) if x['key_hash']==kh)
                UPSTREAM_QUEUE.set_policy(kh,out['concurrency'],out['is_superuser'])
                json_response(self,200,out)
            else: c.rollback(); json_response(self,400,{'error':'no policy fields'})
            c.close(); return True
        if path=='/admin/policy/reset' and self.command=='POST':
            n=int(self.headers.get('Content-Length','0')); body=json.loads(self.rfile.read(min(n,16384))); kh=str(body.get('key_hash') or '')
            c.execute('BEGIN IMMEDIATE'); reset=reset_policy_periods(c,[kh]); c.commit()
            if not reset: c.close(); json_response(self,404,{'error':'policy not found'}); return True
            out=next(x for x in policy_summaries(c) if x['key_hash']==kh); c.close(); json_response(self,200,out); return True
        if path=='/admin/reset-all' and self.command=='POST':
            c.execute('BEGIN IMMEDIATE'); hashes=[r['key_hash'] for r in c.execute('SELECT key_hash FROM policies WHERE keeper_active=1')]; reset=reset_policy_periods(c,hashes); c.commit(); c.close(); json_response(self,200,{'reset_count':len(reset),'reset_at':iso_time(now())}); return True
        c.close(); json_response(self,404,{'error':'not found'}); return True
    def dispatch(self):
        if unified.public_route(self, unified.namespace(globals())): return
        if self.path == '/admin' or self.path.startswith('/admin/'): self.do_admin(); return
        if self.path.split('?',1)[0] in ('/v1/limit','/v1/usage'): self.do_user_query(); return
        self.proxy()
    def do_GET(self): self.dispatch()
    def do_POST(self): self.dispatch()
    def do_PUT(self): self.dispatch()
    def do_DELETE(self): self.dispatch()
    def do_PATCH(self): self.dispatch()
    def do_user_query(self):
        if self.command!='GET': json_response(self,405,{'error':'method not allowed'},{'Allow':'GET'}); return
        key=self.auth_key()
        if not key: json_response(self,401,{'error':'missing bearer API key'}); return
        c=db()
        try: limit=user_limit(c,key)
        finally: c.close()
        if limit is None: json_response(self,401,{'error':'invalid API key'}); return
        if self.path.split('?',1)[0]=='/v1/limit': json_response(self,200,limit); return
        try: json_response(self,200,fetch_codex_weekly_usage())
        except Exception as e:
            print(f'Keeper Codex quota query failed: {e}',file=sys.stderr)
            json_response(self,502,{'error':'Codex weekly usage is unavailable'} )
    def proxy(self):
        if not self.path.startswith('/v1/'):
            json_response(self,404,{'error':'not found'}); return
        key=self.auth_key()
        if not key: json_response(self,401,{'error':{'message':'missing bearer API key','type':'invalid_request_error'}}); return
        body=b''
        try:
            n=int(self.headers.get('Content-Length','0') or 0)
        except (TypeError, ValueError):
            json_response(self,400,{'error':{'message':'invalid Content-Length','type':'invalid_request_error'}}); return
        if n>0:
            if MAX_REQUEST_BODY_BYTES>0 and n>MAX_REQUEST_BODY_BYTES:
                json_response(self,413,{'error':'request too large'}); return
            body=self.rfile.read(n)
        elif 'chunked' in self.headers.get('Transfer-Encoding','').lower():
            chunks=[]; total=0
            while True:
                line=self.rfile.readline().strip(); size=int(line.split(b';',1)[0],16)
                if size==0:
                    self.rfile.readline(); break
                total+=size
                if MAX_REQUEST_BODY_BYTES>0 and total>MAX_REQUEST_BODY_BYTES:
                    json_response(self,413,{'error':'request too large'}); return
                chunks.append(self.rfile.read(size)); self.rfile.read(2)
            body=b''.join(chunks)
        model='unknown'; amount=0
        if body:
            try:
                q=json.loads(body); model=str(q.get('model') or 'unknown'); maxout=int(q.get('max_output_tokens') or q.get('max_completion_tokens') or DEFAULT_MAX_OUTPUT_TOKENS)
                c=db(); p=model_price(c,model); c.close()
                if not p and maybe_sync_keeper_metadata(force=True):
                    c=db(); p=model_price(c,model); c.close()
                if not p:
                    json_response(self,503,{'error':{'message':'model price is not configured in CPA Usage Keeper','type':'server_error','code':'model_price_unavailable'}}); return
                output_hold=(maxout*p['completion_micro_per_1m']*p['multiplier_ppm'])//MICRO//MICRO
                input_hold=((len(body)//3)*p['prompt_micro_per_1m']*p['multiplier_ppm'])//MICRO//MICRO
                amount=max(RESERVATION,output_hold+input_hold+RESERVATION//4)
            except (ValueError, TypeError, json.JSONDecodeError): amount=RESERVATION
        c=db()
        try:
            policy=c.execute('SELECT key_hash,alias,enabled,concurrency,is_superuser FROM policies WHERE key_hash=? AND keeper_active=1',(key_hash(key),)).fetchone()
        finally:
            c.close()
        if not policy:
            json_response(self,401,{'error':{'message':'invalid API key','type':'authentication_error','code':'invalid_api_key'}}); return
        if not policy['enabled']:
            json_response(self,403,{'error':{'message':'API key disabled','type':'permission_error'}}); return
        fingerprint, explicit_request_id = request_fingerprint(key, self.command, self.path, body, self.headers)
        track_state, attempts_used = REQUEST_ATTEMPTS.begin(fingerprint, explicit_request_id, UPSTREAM_MAX_ATTEMPTS_PER_REQUEST)
        if track_state != 'ok':
            self.reject_duplicate(track_state, fingerprint, attempts_used)
            return
        client_request = ClientRequest(getattr(self, 'connection', None))
        client_request.alias = policy['alias']
        client_request.model = model
        client_request.route_account = bool(body)
        client_request.session_hash = session_identity(self.headers, body)
        with ACTIVE_REQUESTS_LOCK:
            ACTIVE_REQUESTS[id(client_request)] = client_request
        admitted = False
        try:
            if client_request.route_account:
                unified.ensure_routing_ready(unified.namespace(globals()))
            admitted = UPSTREAM_QUEUE.acquire(client_request, policy['key_hash'], policy['concurrency'], policy['is_superuser'])
            if admitted:
                self.proxy_admitted(key, body, model, amount, client_request, fingerprint, attempts_used)
        except RoutingUnavailable as exc:
            json_response(self,503,{'error':{'message':str(exc),'type':'server_error','code':'account_routing_unavailable'}})
        finally:
            REQUEST_ATTEMPTS.finish(fingerprint, explicit_request_id, completed=client_request.completed, failed=client_request.failed)
            if admitted:
                UPSTREAM_QUEUE.release(policy['key_hash'], client_request)
            with ACTIVE_REQUESTS_LOCK:
                ACTIVE_REQUESTS.pop(id(client_request), None)

    def reject_duplicate(self, track_state, fingerprint, attempts_used):
        request_tag = fingerprint[:16]
        if track_state == 'exhausted':
            failure = REQUEST_ATTEMPTS.failure(fingerprint)
            if failure is not None:
                self.send_upstream_failure(failure, fingerprint, attempts_used)
                return
        if track_state != "ok":
            errors = {
                "in_flight": (
                    "An identical Codex request is already running",
                    "duplicate_request_in_flight",
                    5,
                ),
                "completed": (
                    "This Codex request ID has already completed",
                    "duplicate_request_completed",
                    UPSTREAM_REQUEST_FINGERPRINT_TTL_SECONDS,
                ),
                "exhausted": (
                    "This Codex request exhausted its shared upstream attempt budget",
                    "duplicate_retry_limit",
                    UPSTREAM_FAILURE_COOLDOWN_SECONDS,
                ),
            }
            message, code, retry_after = errors[track_state]
            print(
                f'upstream request suppressed id={request_tag} state={track_state} '
                f'attempts={attempts_used}/{attempt_limit_text(UPSTREAM_MAX_ATTEMPTS_PER_REQUEST)}',
                file=sys.stderr,
            )
            json_response(
                self,
                503 if track_state == "exhausted" else 409,
                {'error': {'message': message, 'type': 'server_error', 'code': code}},
                {
                    'Retry-After': retry_after,
                    'X-Quota-Request-Fingerprint': request_tag,
                    'X-Quota-Attempts-Used': attempts_used,
                },
            )
            return

    def send_upstream_failure(self, failure, fingerprint, attempts_used):
        """Preserve the actual upstream status and body after recovery expires."""
        status, headers, body = failure
        self.send_response(status)
        for name, value in headers:
            if name.lower() not in ('connection', 'transfer-encoding', 'content-length', 'retry-after'):
                self.send_header(name, value)
        original_delay = next((value for name, value in headers if name.lower() == 'retry-after'), None)
        self.send_header('Retry-After', original_delay or str(REQUEST_ATTEMPTS.cooldown_remaining(fingerprint)))
        self.send_header('X-Quota-Gateway', 'active')
        self.send_header('X-Quota-Request-Fingerprint', fingerprint[:16])
        self.send_header('X-Quota-Attempts-Used', str(attempts_used))
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def proxy_admitted(self, key, body, model, amount, client_request, fingerprint, attempts_used):
        try: rid,_=self.acquire(key,model,amount,enforce_concurrency=False)
        except KeyError: json_response(self,401,{'error':{'message':'invalid API key','type':'authentication_error','code':'invalid_api_key'}}); return
        except PermissionError: json_response(self,429,{'error':{'message':'cycle quota exceeded','type':'quota_exceeded','code':'quota_exceeded'}}, {'Retry-After':'3600'}); return
        except TimeoutError as e: json_response(self,429,{'error':{'message':str(e),'type':'rate_limit_error','code':str(e)}},{'Retry-After':'1'}); return
        except ValueError: json_response(self,403,{'error':{'message':'API key disabled','type':'permission_error'}}); return
        except Exception as e: print(f'acquire failed: {e}',file=sys.stderr); json_response(self,503,{'error':{'message':'quota ledger unavailable','type':'server_error'}}); return
        request_tag = fingerprint[:16]
        upstream_status=None; conn=None; response_started=False; response_finished=False
        response_bytes=0; stream_error=None
        try:
            target=urlsplit(UPSTREAM); path=self.path
            hdr={k:v for k,v in self.headers.items() if k.lower() not in ('host','content-length','connection','x-quota-auth-id','x-quota-routing-token')}; hdr['Host']=target.netloc; hdr['Content-Length']=str(len(body)); hdr['X-Forwarded-For']=self.client_address[0]
            if getattr(client_request, 'account_id', ''):
                hdr['X-Quota-Auth-ID'] = client_request.account_id
                hdr['X-Quota-Routing-Token'] = env('QUOTA_ROUTING_TOKEN')
            policy=upstream_retry_policy(len(body))
            deadline=(
                None
                if policy['budget_seconds'] <= 0
                else time.monotonic()+policy['budget_seconds']
            )
            if policy['large']:
                print(
                    f'upstream large request id={request_tag} bytes={len(body)} '
                    f'timeout={policy["attempt_timeout_seconds"]}s '
                    f'local_attempts={policy["local_attempts"]} '
                    f'shared_attempts={attempts_used}/{attempt_limit_text(UPSTREAM_MAX_ATTEMPTS_PER_REQUEST)}',
                    file=sys.stderr,
                )
            attempt_limit_exhausted=False
            attempt = 0
            while (
                not SHUTDOWN_EVENT.is_set()
                and not client_request.cancelled.is_set()
                and local_attempt_available(attempt, policy['local_attempts'])
            ):
                if client_request.disconnected():
                    client_request.cancel()
                    break
                attempt += 1
                remaining=budget_remaining(deadline)
                if remaining is not None and remaining <= 0:
                    break
                shared_attempt=REQUEST_ATTEMPTS.reserve_attempt(
                    fingerprint, UPSTREAM_MAX_ATTEMPTS_PER_REQUEST
                )
                if not shared_attempt:
                    attempt_limit_exhausted=True
                    break
                attempt_timeout=policy['attempt_timeout_seconds']
                if remaining is not None:
                    attempt_timeout=min(attempt_timeout, remaining) if attempt_timeout > 0 else remaining
                if attempt_timeout <= 0:
                    attempt_timeout=None
                conn=http.client.HTTPConnection(target.hostname,target.port or 80,timeout=attempt_timeout)
                client_request.upstream = conn
                try:
                    conn.request(self.command,path,body=body,headers=hdr)
                    resp=conn.getresponse()
                    client_request.response = resp
                    upstream_status=resp.status
                    if client_request.cancelled.is_set():
                        break
                    if resp.status in RETRYABLE_UPSTREAM_STATUSES:
                        deadline=transient_retry_deadline(deadline)
                        client_request.recovery_deadline=deadline
                    error_body=b''
                    if resp.status in RETRYABLE_UPSTREAM_STATUSES:
                        sock=getattr(conn, 'sock', None)
                        remaining=budget_remaining(deadline)
                        if sock is not None and remaining is not None:
                            sock.settimeout(max(0.001, remaining))
                        error_body=resp.read(16385)
                        REQUEST_ATTEMPTS.save_failure(fingerprint, resp.status, resp.getheaders(), error_body)
                    server_delay=retry_after_delay(resp.getheaders())
                    remaining=budget_remaining(deadline)
                    can_retry=(
                        resp.status in RETRYABLE_UPSTREAM_STATUSES
                        and len(error_body) <= 16384
                        and (remaining is None or server_delay < remaining)
                        and local_attempt_available(attempt, policy['local_attempts'])
                        and budget_available(deadline)
                        and attempt_budget_available(
                            fingerprint, UPSTREAM_MAX_ATTEMPTS_PER_REQUEST
                        )
                    )
                    if can_retry:
                        print(
                            f'upstream header retry id={request_tag} '
                            f'local={attempt+1}/{policy["local_attempts"]} '
                            f'shared={shared_attempt}/{attempt_limit_text(UPSTREAM_MAX_ATTEMPTS_PER_REQUEST)} '
                            f'status={resp.status}',
                            file=sys.stderr,
                        )
                        resp.close(); conn.close(); conn=None
                        if wait_before_retry(deadline, attempt, client_request.cancelled, server_delay): break
                        continue

                    first_chunk=error_body
                    if 200 <= resp.status < 300:
                        client_request.recovery_deadline=None
                        # Do not commit response headers until at least the
                        # first upstream bytes arrive. A reset before the first
                        # byte can then be retried transparently, including for
                        # large Codex histories.
                        sock=getattr(conn,'sock',None)
                        if sock is not None:
                            sock.settimeout(
                                None if UPSTREAM_STREAM_TIMEOUT_SECONDS <= 0
                                else UPSTREAM_STREAM_TIMEOUT_SECONDS
                            )
                        first_chunk=getattr(resp, 'read1', resp.read)(65536)
                    self.send_response(resp.status)
                    client_request.recovery_deadline=None
                    for k,v in resp.getheaders():
                        if k.lower() not in ('connection','transfer-encoding','content-length'): self.send_header(k,v)
                    self.send_header('X-Quota-Gateway','active')
                    self.send_header('X-Quota-Request-Fingerprint',request_tag)
                    self.send_header('X-Quota-Attempt',str(shared_attempt))
                    self.send_header('Connection','close'); self.close_connection=True; self.end_headers()
                    response_started=True
                    if first_chunk:
                        response_bytes += len(first_chunk)
                        self.wfile.write(first_chunk); self.wfile.flush()
                    while True:
                        if client_request.cancelled.is_set():
                            break
                        b=getattr(resp, 'read1', resp.read)(65536)
                        if not b: break
                        response_bytes += len(b)
                        self.wfile.write(b); self.wfile.flush()
                    response_finished=not client_request.cancelled.is_set()
                    break
                except Exception as e:
                    if response_started:
                        stream_error=e
                        print(
                            f'proxy upstream stream failed id={request_tag} '
                            f'status={upstream_status} bytes={response_bytes}: '
                            f'{type(e).__name__}: {e}',
                            file=sys.stderr,
                        )
                        break
                    if is_retryable_header_error(e, policy['large']):
                        deadline=transient_retry_deadline(deadline)
                        client_request.recovery_deadline=deadline
                    remaining=budget_remaining(deadline)
                    can_retry=(
                        is_retryable_header_error(e, policy['large'])
                        and not SHUTDOWN_EVENT.is_set()
                        and not client_request.cancelled.is_set()
                        and
                        local_attempt_available(attempt, policy['local_attempts'])
                        and (remaining is None or remaining > 0)
                        and attempt_budget_available(
                            fingerprint, UPSTREAM_MAX_ATTEMPTS_PER_REQUEST
                        )
                    )
                    if not can_retry:
                        print(
                            f'proxy upstream header failed id={request_tag} '
                            f'shared={shared_attempt}/{attempt_limit_text(UPSTREAM_MAX_ATTEMPTS_PER_REQUEST)}: {e}',
                            file=sys.stderr,
                        )
                        break
                    print(
                        f'upstream header retry id={request_tag} '
                        f'local={attempt+1}/{policy["local_attempts"]} '
                        f'shared={shared_attempt}/{attempt_limit_text(UPSTREAM_MAX_ATTEMPTS_PER_REQUEST)} '
                        f'error={type(e).__name__}: {e}',
                        file=sys.stderr,
                    )
                    try: conn.close()
                    except Exception: pass
                    conn=None
                    if wait_before_retry(deadline, attempt, client_request.cancelled): break
            if not response_started and not client_request.cancelled.is_set():
                failure=REQUEST_ATTEMPTS.failure(fingerprint)
                if failure is not None:
                    self.send_upstream_failure(failure, fingerprint, REQUEST_ATTEMPTS.used(fingerprint))
                    upstream_status=failure[0]
                    response_finished=True
                    return
                code='upstream_attempt_limit' if attempt_limit_exhausted else 'upstream_header_timeout'
                message=(
                    'The shared upstream attempt budget was exhausted'
                    if attempt_limit_exhausted
                    else 'CPA upstream did not provide response headers'
                )
                json_response(
                    self,
                    503,
                    {
                        'error': {
                            'message': message,
                            'type': 'server_error',
                            'code': code,
                        }
                    },
                    {
                        'Retry-After': str(UPSTREAM_FAILURE_COOLDOWN_SECONDS),
                        'X-Quota-Request-Fingerprint': request_tag,
                        'X-Quota-Attempts-Used': REQUEST_ATTEMPTS.used(fingerprint),
                    },
                )
        except Exception as e:
            if response_started and stream_error is None:
                stream_error=e
                print(
                    f'proxy upstream stream failed id={request_tag} '
                    f'status={upstream_status} bytes={response_bytes}: '
                    f'{type(e).__name__}: {e}',
                    file=sys.stderr,
                )
            else:
                print(f'proxy failed: {e}',file=sys.stderr)
        finally:
            client_request.upstream = None
            client_request.response = None
            if conn:
                try: conn.close()
                except Exception: pass
            client_request.completed=bool(response_finished and upstream_status is not None and 200 <= upstream_status < 300)
            client_request.failed=bool(not client_request.cancelled.is_set() and (
                upstream_status in RETRYABLE_UPSTREAM_STATUSES
                or (not response_started and not SHUTDOWN_EVENT.is_set())
            ))
            # Successful requests remain reserved until their usage event settles them.
            # Any incomplete response cannot be trusted to have reached the client.
            if (
                not response_finished
                or upstream_status is None
                or not 200 <= upstream_status < 300
            ):
                try: self.release(rid)
                except Exception as e: print(f'reservation release failed: {e}',file=sys.stderr)

def _sync_keeper_metadata():
    if not requests or not KEEPER_PASSWORD: return None
    c=None
    try:
        s=requests.Session(); h={'X-CPA-Usage-Keeper-Request':'fetch'}
        r=s.post(KEEPER_URL+'/api/v1/auth/login',json={'password':KEEPER_PASSWORD},headers=h,timeout=10); r.raise_for_status()
        r=s.get(KEEPER_URL+'/api/v1/pricing',headers=h,timeout=10); r.raise_for_status(); prices=r.json().get('pricing',[])
        r=s.get(KEEPER_URL+'/api/v1/usage/api-keys/settings',headers=h,timeout=10); r.raise_for_status(); keys=r.json().get('items',[])
        c=db(); t=now(); c.execute('BEGIN IMMEDIATE')
        for p in prices:
            c.execute('INSERT OR REPLACE INTO prices VALUES(?,?,?,?,?,?,?)',(p['model'],round(float(p.get('prompt_price_per_1m') or 0)*MICRO),round(float(p.get('completion_price_per_1m') or 0)*MICRO),round(float(p.get('cache_read_price_per_1m') or 0)*MICRO),round(float(p.get('cache_write_price_per_1m') or p.get('cache_creation_price_per_1m') or 0)*MICRO),round(float(p.get('price_multiplier') or 1)*MICRO),t))
        c.execute('UPDATE policies SET keeper_active=0')
        synced=0
        for item in keys:
            key=str(item.get('apiKey') or '').strip()
            if not key: continue
            kh=key_hash(key); ensure_policy(c,kh,key)
            c.execute('UPDATE policies SET key_hint=?,alias=?,keeper_active=1,updated_at=? WHERE key_hash=?',(key_hint(key),str(item.get('keyAlias') or '').strip(),t,kh)); synced+=1
        removed=delete_policies(c,[r['key_hash'] for r in c.execute('SELECT key_hash FROM policies WHERE keeper_active=0').fetchall()])
        c.execute("UPDATE settings SET value=? WHERE key='last_price_sync'",(str(t),)); c.execute("UPDATE settings SET value=? WHERE key='last_key_sync'",(str(t),)); c.commit(); c.close()
        c=None
        print(f'keeper sync: {len(prices)} prices, {synced} API keys, {removed} removed')
        return {'price_count':len(prices),'key_count':synced,'removed_count':removed,'synced_at':iso_time(t)}
    except Exception as e:
        if c is not None:
            try: c.rollback(); c.close()
            except Exception: pass
        print(f'keeper sync failed (cached metadata retained): {e}',file=sys.stderr)
        return None

def sync_keeper_metadata():
    """Serialize Keeper syncs so manual and background refreshes cannot race."""
    if not KEEPER_SYNC_LOCK.acquire(blocking=False):
        return None
    try:
        return _sync_keeper_metadata()
    finally:
        KEEPER_SYNC_LOCK.release()

def backfill_keeper_month():
    """One-time, idempotent import of the current Keeper month."""
    source=sqlite3.connect(f'file:{KEEPER_DB}?mode=ro',uri=True); source.row_factory=sqlite3.Row
    keys={r['api_key']:(r['key_alias'] or '') for r in source.execute('SELECT api_key,key_alias FROM cpa_api_keys WHERE is_deleted=0')}
    rows=source.execute("SELECT event_key,api_group_key,request_id,timestamp,model,model_alias,input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,failed FROM usage_events WHERE substr(timestamp,1,7)=? ORDER BY id",(period_key(),)).fetchall()
    c=db(); imported=0; missing=set(); c.execute('BEGIN IMMEDIATE')
    try:
        for key,alias in keys.items():
            kh=key_hash(key); ensure_policy(c,kh,key)
            if alias: c.execute('UPDATE policies SET alias=?,updated_at=? WHERE key_hash=?',(alias,now(),kh))
        for e in rows:
            key=(e['api_group_key'] or '').strip()
            if not key or key not in keys: continue
            kh=key_hash(key); p=ensure_policy(c,kh,key); alias=keys[key]
            if alias: c.execute('UPDATE policies SET alias=?,updated_at=? WHERE key_hash=?',(alias,now(),kh))
            model=(e['model'] or 'unknown').strip(); cm=cost_micro(c,model,e['input_tokens'],e['output_tokens'],e['cache_read_tokens'],e['cache_creation_tokens'])
            if cm is None and e['model_alias']: cm=cost_micro(c,e['model_alias'],e['input_tokens'],e['output_tokens'],e['cache_read_tokens'],e['cache_creation_tokens'])
            if cm is None: missing.add(model); continue
            rid=(e['request_id'] or '').strip(); eid=event_id(kh,rid,(e['event_key'] or '')+'|'+(e['timestamp'] or ''))
            inserted=c.execute("INSERT OR IGNORE INTO ledger(event_id,key_hash,period,model,request_id,input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_micro,failed,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(eid,kh,period_key(),model,rid,e['input_tokens'],e['output_tokens'],e['cache_read_tokens'],e['cache_creation_tokens'],max(0,cm),e['failed'],now())).rowcount
            if inserted:
                c.execute('INSERT OR IGNORE INTO periods(key_hash,period) VALUES(?,?)',(kh,period_key())); c.execute('UPDATE periods SET spent_micro=spent_micro+? WHERE key_hash=? AND period=?',(max(0,cm),kh,period_key())); imported+=1
        c.commit()
    except Exception: c.rollback(); raise
    finally: c.close(); source.close()
    print(json.dumps({'seeded_keys':len(keys),'backfilled':imported,'examined':len(rows),'missing_price_models':sorted(missing)},ensure_ascii=False))
def metadata_loop():
    interval=min(KEEPER_SYNC_INTERVAL_SECONDS,KEEPER_USAGE_SYNC_INTERVAL_SECONDS)
    while True:
        time.sleep(interval)
        maybe_sync_keeper_metadata()
        sync_keeper_usage()

class QuotaHTTPServer(ThreadingHTTPServer):
    # Match the local kernel backlog so bursts of long-lived requests are not
    # rejected before ThreadingHTTPServer can dispatch them.
    request_queue_size=4096
    # Active handlers may be blocked indefinitely on an upstream socket. They
    # must not prevent the listener process from restarting after SIGTERM.
    daemon_threads=True
    block_on_close=False

def main():
    global DB_PATH, FAILOVER_GATE
    ap=argparse.ArgumentParser(); ap.add_argument('--check',action='store_true'); ap.add_argument('--backfill-keeper-month',action='store_true'); ap.add_argument('--sync-keeper',action='store_true'); ap.add_argument('--sync-keeper-usage',action='store_true'); args=ap.parse_args(); init_db(); sync_keeper_metadata(); load_concurrency_configuration()
    if args.check:
        print(json.dumps({
            'ok': True,
            'db': DB_PATH,
            'upstream': UPSTREAM,
            'period': period_key(),
            'default_budget_usd': DEFAULT_LIMIT / MICRO,
            'pro_failover_enabled': PRO_FAILOVER_ENABLED,
            'pro_failover_window_seconds': PRO_FAILOVER_WINDOW_SECONDS,
            'upstream_header_retry_attempts': UPSTREAM_HEADER_RETRY_ATTEMPTS,
            'upstream_header_retry_budget_seconds': UPSTREAM_HEADER_RETRY_BUDGET_SECONDS,
            'upstream_header_attempt_timeout_seconds': UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS,
            'upstream_retry_max_backoff_seconds': UPSTREAM_RETRY_MAX_BACKOFF_SECONDS,
            'upstream_failure_cooldown_seconds': UPSTREAM_FAILURE_COOLDOWN_SECONDS,
            'upstream_transient_recovery_seconds': UPSTREAM_TRANSIENT_RECOVERY_SECONDS,
            'upstream_max_attempts_per_request': UPSTREAM_MAX_ATTEMPTS_PER_REQUEST,
            'upstream_request_fingerprint_ttl_seconds': UPSTREAM_REQUEST_FINGERPRINT_TTL_SECONDS,
            'upstream_large_request_bytes': UPSTREAM_LARGE_REQUEST_BYTES,
            'upstream_large_request_header_timeout_seconds': UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS,
            'upstream_large_request_retry_attempts': UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS,
            'upstream_large_request_retry_budget_seconds': UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS,
            'default_rpm': DEFAULT_RPM,
            'default_concurrency': DEFAULT_CONCURRENCY,
            'global_concurrency': UPSTREAM_QUEUE.snapshot()['limit'],
            'max_request_body_bytes': MAX_REQUEST_BODY_BYTES,
            'reservation_ttl_seconds': RESERVATION_TTL,
            'standby': GATEWAY_STANDBY,
        }))
        return
    if args.backfill_keeper_month: backfill_keeper_month(); return
    if args.sync_keeper: return
    if args.sync_keeper_usage: sync_keeper_usage(force=True); return
    sync_keeper_usage(force=True)
    if PRO_FAILOVER_ENABLED:
        FAILOVER_GATE=ProFailoverGate(CPA_CONFIG_PATH,setting_get,setting_set,PRO_FAILOVER_WINDOW_SECONDS)
        FAILOVER_GATE.reconcile()
    if not GATEWAY_STANDBY:
        threading.Thread(target=metadata_loop,daemon=True).start(); UsageSubscriber().start()
    else:
        print("quota gateway standby mode: background metadata and usage subscribers disabled", flush=True)
    host=env('LISTEN_HOST','127.0.0.1'); port=int_env('LISTEN_PORT',8320)
    SHUTDOWN_EVENT.clear()
    threading.Thread(target=monitor_clients, name='client-cancellation-monitor', daemon=True).start()
    server=QuotaHTTPServer((host,port),Gateway)
    shutdown_started=threading.Event()

    def request_shutdown(signum, _frame):
        if shutdown_started.is_set():
            return
        shutdown_started.set()
        SHUTDOWN_EVENT.set()
        with ACTIVE_REQUESTS_LOCK:
            for request in ACTIVE_REQUESTS.values():
                request.cancel()
        print(
            f'quota gateway shutdown signal={signum}; stopping retries',
            flush=True,
        )
        # BaseServer.shutdown() must run outside serve_forever()'s thread.
        threading.Thread(
            target=server.shutdown,
            name='quota-gateway-shutdown',
            daemon=True,
        ).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    print(f'quota gateway listening on {host}:{port}, upstream {UPSTREAM}',flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
if __name__=='__main__': main()

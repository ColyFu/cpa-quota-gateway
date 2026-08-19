#!/usr/bin/env python3
"""Small, independent quota gateway for CLIProxyAPI.

The gateway intentionally has no import or source dependency on cpa-usage-keeper.
It snapshots the Keeper pricing API into its own SQLite database and keeps all
policy and accounting data there, so upgrading Keeper does not replace quotas.
"""
import argparse, base64, calendar, hashlib, http.client, json, os, re, secrets, socket, sqlite3, sys, threading, time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

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
 keeper_active INTEGER NOT NULL DEFAULT 1);
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

DB_PATH = env("QUOTA_DB", str(BASE_DIR / "data" / "quota.db"))
UPSTREAM = env("UPSTREAM_URL", "http://127.0.0.1:8317")
KEEPER_URL = env("KEEPER_URL", "http://127.0.0.1:8318")
KEEPER_DB = env("KEEPER_DB", str(BASE_DIR.parent / "cpa-usage-keeper" / "data" / "app.db"))
KEEPER_PASSWORD = env("KEEPER_LOGIN_PASSWORD", env("LOGIN_PASSWORD"))
CPA_MANAGEMENT_KEY = env("CPA_MANAGEMENT_KEY")
DEFAULT_LIMIT = int(round(float(env("DEFAULT_BUDGET_USD", env("DEFAULT_MONTHLY_BUDGET_USD", "1000"))) * MICRO))
DEFAULT_RPM = int_env("DEFAULT_RPM", 120)
DEFAULT_CONCURRENCY = int_env("DEFAULT_CONCURRENCY", 20)
RESERVATION = int(round(float(env("RESERVATION_USD", "2")) * MICRO))
RESERVATION_TTL = int_env("RESERVATION_TTL_SECONDS", 3600)
DEFAULT_MAX_OUTPUT_TOKENS = int_env("DEFAULT_MAX_OUTPUT_TOKENS", 128000)
BILLING_TIMEZONE = env("BILLING_TIMEZONE", "Asia/Shanghai")
try: BILLING_ZONE = ZoneInfo(BILLING_TIMEZONE)
except Exception: BILLING_ZONE = timezone.utc
ADMIN_TOKEN = env("QUOTA_ADMIN_TOKEN")
if not ADMIN_TOKEN:
    ADMIN_TOKEN = secrets.token_urlsafe(32)
    print("QUOTA_ADMIN_TOKEN was not set; generated token (set it in the service environment to keep it stable)", file=sys.stderr)

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
    c.close()

def model_price(c, model):
    r = c.execute("SELECT * FROM prices WHERE model=?", (model,)).fetchone()
    return r
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
    rows=c.execute("SELECT key_hash,period,sum(amount_micro) amount FROM reservations WHERE state='active' AND expires_at<? GROUP BY key_hash,period",(t,)).fetchall()
    c.execute("UPDATE reservations SET state='expired' WHERE state='active' AND expires_at<?",(t,))
    for r in rows:
        c.execute("UPDATE periods SET reserved_micro=max(0,reserved_micro-?) WHERE key_hash=? AND period=?",(r['amount'],r['key_hash'],r['period']))

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
        items.append({'key_hash':p['key_hash'],'key_hint':p['key_hint'],'alias':p['alias'],'limit_usd':p['monthly_limit_micro']/MICRO,'spent_usd':spent/MICRO,'reserved_usd':reserved/MICRO,'used_usd':(spent+reserved)/MICRO,'remaining_usd':max(0,p['monthly_limit_micro']-spent-reserved)/MICRO,'rpm':p['rpm'],'concurrency':p['concurrency'],'enabled':bool(p['enabled']),'cycle_type':p['cycle_type'],'cycle_days':p['cycle_days'],'cycle_label':cycle_label(p),'period_id':p['period_id'],'period_started_at':iso_time(p['period_started_at']),'period_ends_at':iso_time(p['period_ends_at']),'keeper_active':bool(p['keeper_active'])})
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
    def resp_read(self, f):
        p = f.read(1)
        if not p: raise ConnectionError("redis closed")
        if p == b'*':
            n = int(f.readline()); return [self.resp_read(f) for _ in range(n)]
        if p == b'$':
            n = int(f.readline());
            if n < 0: return None
            v=f.read(n); f.read(2); return v
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
    def settle(self, event):
        key=str(event.get('api_key') or '')
        if not key: return
        kh=key_hash(key); model=str(event.get('model') or 'unknown'); tok=event.get('tokens') or {}
        it=int(tok.get('input_tokens') or event.get('input_tokens') or 0); ot=int(tok.get('output_tokens') or event.get('output_tokens') or 0)
        crt=int(tok.get('cache_read_tokens') or event.get('cache_read_tokens') or 0); cwt=int(tok.get('cache_creation_tokens') or event.get('cache_creation_tokens') or 0)
        rid=str(event.get('request_id') or ''); stamp=str(event.get('timestamp') or '')
        eid=event_id(kh,rid,stamp+'|'+model+'|'+str(it)+'|'+str(ot))
        c=db(); t=now(); c.execute('BEGIN IMMEDIATE')
        try:
            p=c.execute('SELECT * FROM policies WHERE key_hash=? AND keeper_active=1',(kh,)).fetchone()
            if not p:
                c.rollback(); return
            p=refresh_policy_period(c,p,t)
            r=c.execute("SELECT id,amount_micro,period FROM reservations WHERE key_hash=? AND state='active' ORDER BY created_at LIMIT 1",(kh,)).fetchone()
            per=r['period'] if r else p['period_id']
            c.execute("INSERT OR IGNORE INTO periods(key_hash,period) VALUES(?,?)",(kh,per))
            cm=cost_micro(c,model,it,ot,crt,cwt)
            if cm is None and event.get('alias'): cm=cost_micro(c,str(event['alias']),it,ot,crt,cwt)
            if cm is None: raise ValueError(f'missing Keeper price for model {model}')
            cm=max(0,cm)
            inserted=c.execute("INSERT OR IGNORE INTO ledger(event_id,key_hash,period,model,request_id,input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_micro,failed,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",(eid,kh,per,model,rid,it,ot,crt,cwt,cm,1 if event.get('failed') else 0,t)).rowcount
            if inserted:
                c.execute("UPDATE periods SET spent_micro=spent_micro+? WHERE key_hash=? AND period=?",(cm,kh,per))
            if r:
                c.execute("UPDATE reservations SET state='settled' WHERE id=?",(r['id'],)); c.execute("UPDATE periods SET reserved_micro=max(0,reserved_micro-?) WHERE key_hash=? AND period=?",(r['amount_micro'],kh,r['period']))
            c.commit()
        except Exception:
            c.rollback(); raise
        finally: c.close()

class Gateway(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self, fmt,*args): print(f"{self.client_address[0]} {fmt%args}", file=sys.stderr)
    def auth_key(self):
        m=re.match(r'\s*Bearer\s+(.+)\s*$', self.headers.get('Authorization',''), re.I)
        return m.group(1).strip() if m else ''
    def acquire(self, key, model, amount):
        kh=key_hash(key); c=db(); t=now(); rid=secrets.token_hex(16); c.execute('BEGIN IMMEDIATE')
        try:
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
            if p['rpm']>0 and recent>=p['rpm']: raise TimeoutError('rpm')
            if p['concurrency']>0 and inflight>=p['concurrency']: raise TimeoutError('concurrency')
            if amount and used['spent_micro']+used['reserved_micro']+amount>p['monthly_limit_micro']: raise PermissionError('quota')
            c.execute("INSERT INTO request_ticks(key_hash,created_at) VALUES(?,?)",(kh,t))
            if amount:
                c.execute("INSERT INTO reservations VALUES(?,?,?,?,?,?,?,?)",(rid,kh,per,amount,'active',model,t,t+RESERVATION_TTL)); c.execute("UPDATE periods SET reserved_micro=reserved_micro+? WHERE key_hash=? AND period=?",(amount,kh,per))
            c.commit(); return rid,p
        except Exception: c.rollback(); raise
        finally: c.close()
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
        return False
    def do_admin(self):
        if not self.admin_authenticated(): json_response(self,401,{'error':'admin authentication required'},{'WWW-Authenticate':'Basic realm="CPA Quota"'}); return True
        if self.command in ('POST','PUT','PATCH','DELETE') and self.headers.get('X-Quota-Admin-Request')!='1':
            json_response(self,403,{'error':'missing admin request confirmation header'}); return True
        path=self.path.split('?',1)[0]
        if path in ('/admin','/admin/') and self.command=='GET':
            body=Path(__file__).with_name('admin.html').read_bytes(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Content-Length',str(len(body))); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(body); return True
        c=db()
        if path=='/admin/health':
            sync={r['key']:r['value'] for r in c.execute("SELECT key,value FROM settings WHERE key IN ('last_price_sync','last_key_sync')")}
            json_response(self,200,{'ok':True,'db':DB_PATH,'last_price_sync':int(sync.get('last_price_sync','0')),'last_key_sync':int(sync.get('last_key_sync','0'))}); c.close(); return True
        if path=='/admin/sync' and self.command=='POST':
            c.close(); result=sync_keeper_metadata()
            if result is None: json_response(self,502,{'error':'Keeper metadata sync failed'}); return True
            json_response(self,200,result); return True
        if path=='/admin/policies' and self.command=='GET':
            rows=[dict(x) for x in c.execute('SELECT key_hash,key_hint,alias,monthly_limit_micro/1000000.0 limit_usd,rpm,concurrency,enabled,updated_at FROM policies ORDER BY key_hint')]; json_response(self,200,{'policies':rows}); c.close(); return True
        if path=='/admin/summary' and self.command=='GET':
            c.execute('BEGIN IMMEDIATE'); rows=policy_summaries(c); c.commit()
            sync={r['key']:int(r['value']) for r in c.execute("SELECT key,value FROM settings WHERE key IN ('last_price_sync','last_key_sync')")}
            latest_sync=max(sync.values()) if sync else 0
            meta={'total_keys':len(rows),'active_keys':sum(1 for x in rows if x['keeper_active']),'enabled_keys':sum(1 for x in rows if x['enabled'] and x['keeper_active']),'used_usd':sum(x['used_usd'] for x in rows if x['keeper_active']),'limit_usd':sum(x['limit_usd'] for x in rows if x['keeper_active']),'price_count':c.execute('SELECT count(*) n FROM prices').fetchone()['n'],'last_sync_at':iso_time(latest_sync) if latest_sync>0 else ''}
            json_response(self,200,{'timezone':BILLING_TIMEZONE,'meta':meta,'items':rows}); c.close(); return True
        if path=='/admin/policy' and self.command=='PUT':
            n=int(self.headers.get('Content-Length','0')); body=json.loads(self.rfile.read(min(n,16384))); key=str(body.get('api_key') or '')
            kh=str(body.get('key_hash') or (key_hash(key) if key else '')); c.execute('BEGIN IMMEDIATE'); row=c.execute('SELECT * FROM policies WHERE key_hash=?',(kh,)).fetchone()
            if not row: c.rollback(); c.close(); json_response(self,404,{'error':'policy not found; send api_key once to create'}); return True
            limit_value=body.get('limit_usd',body.get('monthly_limit_usd')); cycle_type=str(body.get('cycle_type') or row['cycle_type']); cycle_days=int(body.get('cycle_days') or row['cycle_days'])
            if (limit_value is not None and float(limit_value)<0) or ('rpm' in body and int(body['rpm'])<0) or ('concurrency' in body and int(body['concurrency'])<0) or cycle_type not in ('calendar_month','fixed_days') or not 1<=cycle_days<=3650:
                c.rollback(); c.close(); json_response(self,400,{'error':'invalid policy values'}); return True
            cols=[]; vals=[]
            for name, val in [('monthly_limit_micro', (float(limit_value)*MICRO if limit_value is not None else None)),('rpm',body.get('rpm')),('concurrency',body.get('concurrency')),('enabled',body.get('enabled'))]:
                if val is not None: cols.append(name+'=?'); vals.append(int(val))
            cycle_changed=cycle_type!=row['cycle_type'] or (cycle_type=='fixed_days' and cycle_days!=row['cycle_days'])
            if 'cycle_type' in body or 'cycle_days' in body:
                cols.extend(('cycle_type=?','cycle_days=?')); vals.extend((cycle_type,cycle_days if cycle_type=='fixed_days' else 30))
            if cols:
                vals += [now(),kh]; c.execute('UPDATE policies SET '+','.join(cols)+',updated_at=? WHERE key_hash=?',vals)
                if cycle_changed: reset_policy_periods(c,[kh])
                c.commit(); out=next(x for x in policy_summaries(c) if x['key_hash']==kh); json_response(self,200,out)
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
        if self.path.startswith('/admin/'): self.do_admin(); return
        if self.path.split('?',1)[0] in ('/v1/limit','/v1/usage'): self.do_user_query(); return
        self.proxy()
    def do_GET(self): self.dispatch()
    def do_POST(self): self.dispatch()
    def do_PUT(self): self.dispatch()
    def do_DELETE(self): self.dispatch()
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
        body=b''; n=int(self.headers.get('Content-Length','0') or 0)
        if n>0:
            if n>64*1024*1024: json_response(self,413,{'error':'request too large'}); return
            body=self.rfile.read(n)
        elif 'chunked' in self.headers.get('Transfer-Encoding','').lower():
            chunks=[]; total=0
            while True:
                line=self.rfile.readline().strip(); size=int(line.split(b';',1)[0],16)
                if size==0:
                    self.rfile.readline(); break
                total+=size
                if total>64*1024*1024: json_response(self,413,{'error':'request too large'}); return
                chunks.append(self.rfile.read(size)); self.rfile.read(2)
            body=b''.join(chunks)
        model='unknown'; amount=0
        if body:
            try:
                q=json.loads(body); model=str(q.get('model') or 'unknown'); maxout=int(q.get('max_output_tokens') or q.get('max_completion_tokens') or DEFAULT_MAX_OUTPUT_TOKENS)
                c=db(); p=model_price(c,model); c.close()
                if not p:
                    json_response(self,503,{'error':{'message':'model price is not configured in CPA Usage Keeper','type':'server_error','code':'model_price_unavailable'}}); return
                output_hold=(maxout*p['completion_micro_per_1m']*p['multiplier_ppm'])//MICRO//MICRO
                input_hold=((len(body)//3)*p['prompt_micro_per_1m']*p['multiplier_ppm'])//MICRO//MICRO
                amount=max(RESERVATION,output_hold+input_hold+RESERVATION//4)
            except (ValueError, TypeError, json.JSONDecodeError): amount=RESERVATION
        try: rid,_=self.acquire(key,model,amount)
        except KeyError: json_response(self,401,{'error':{'message':'invalid API key','type':'authentication_error','code':'invalid_api_key'}}); return
        except PermissionError: json_response(self,429,{'error':{'message':'cycle quota exceeded','type':'quota_exceeded','code':'quota_exceeded'}}, {'Retry-After':'3600'}); return
        except TimeoutError as e: json_response(self,429,{'error':{'message':str(e),'type':'rate_limit_error','code':str(e)}},{'Retry-After':'1'}); return
        except ValueError: json_response(self,403,{'error':{'message':'API key disabled','type':'permission_error'}}); return
        except Exception as e: print(f'acquire failed: {e}',file=sys.stderr); json_response(self,503,{'error':{'message':'quota ledger unavailable','type':'server_error'}}); return
        upstream_status=None; conn=None
        try:
            target=urlsplit(UPSTREAM); path=self.path
            hdr={k:v for k,v in self.headers.items() if k.lower() not in ('host','content-length','connection')}; hdr['Host']=target.netloc; hdr['Content-Length']=str(len(body)); hdr['X-Forwarded-For']=self.client_address[0]
            conn=http.client.HTTPConnection(target.hostname,target.port or 80,timeout=900); conn.request(self.command,path,body=body,headers=hdr); resp=conn.getresponse()
            upstream_status=resp.status
            self.send_response(resp.status)
            for k,v in resp.getheaders():
                if k.lower() not in ('connection','transfer-encoding','content-length'): self.send_header(k,v)
            self.send_header('X-Quota-Gateway','active'); self.send_header('Connection','close'); self.close_connection=True; self.end_headers()
            while True:
                b=resp.read(65536)
                if not b: break
                self.wfile.write(b); self.wfile.flush()
        except Exception as e: print(f'proxy failed: {e}',file=sys.stderr)
        finally:
            if conn:
                try: conn.close()
                except Exception: pass
            # Successful requests remain reserved until their usage event settles them.
            # CPA failures and pre-response transport errors cannot consume quota.
            if upstream_status is None or not 200 <= upstream_status < 300:
                try: self.release(rid)
                except Exception as e: print(f'reservation release failed: {e}',file=sys.stderr)

def sync_keeper_metadata():
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
    while True:
        time.sleep(300)
        sync_keeper_metadata()

class QuotaHTTPServer(ThreadingHTTPServer):
    request_queue_size=256
    daemon_threads=True

def main():
    global DB_PATH
    ap=argparse.ArgumentParser(); ap.add_argument('--check',action='store_true'); ap.add_argument('--backfill-keeper-month',action='store_true'); ap.add_argument('--sync-keeper',action='store_true'); args=ap.parse_args(); init_db(); sync_keeper_metadata()
    if args.check: print(json.dumps({'ok':True,'db':DB_PATH,'upstream':UPSTREAM,'period':period_key(),'default_budget_usd':DEFAULT_LIMIT/MICRO})); return
    if args.backfill_keeper_month: backfill_keeper_month(); return
    if args.sync_keeper: return
    threading.Thread(target=metadata_loop,daemon=True).start(); UsageSubscriber().start()
    host=env('LISTEN_HOST','127.0.0.1'); port=int_env('LISTEN_PORT',8320); print(f'quota gateway listening on {host}:{port}, upstream {UPSTREAM}',flush=True)
    QuotaHTTPServer((host,port),Gateway).serve_forever()
if __name__=='__main__': main()

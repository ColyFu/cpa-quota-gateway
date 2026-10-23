# CPA Quota Gateway

See [the integration guide](INTEGRATION.zh-CN.md) for the unified Keeper console, account routing and reproducible builds. The previous production baseline remains on `main` (`2408701`).

[简体中文](README.zh-CN.md) | English

`cpa-quota-gateway` is an independent quota and rate-limit gateway for
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI). It uses
[CPA Usage Keeper](https://github.com/Willxup/cpa-usage-keeper) as the source
for API keys, user aliases, and model prices without modifying Keeper itself.

```text
Internet -> Nginx :443 -> CPA Quota Gateway :8320 -> CLIProxyAPI :8317
                                  |
                                  +-> SQLite quota ledger

CPA Usage Keeper :8318 -> API keys / aliases / model prices
CLIProxyAPI :8317      -> authenticated usage event subscription
```

Keeper and CLIProxyAPI can be upgraded independently. Gateway policies,
billing periods, reservations, and settled usage are stored in the gateway's
own SQLite database.

## Features

- Optional delayed Pro failover: a backup Codex API is exposed only after
  Codex OAuth requests fail continuously for the configured window; any OAuth
  success immediately disables the backup again
- Per-user USD quota with atomic pre-authorization before forwarding a request
- Independent billing cycle for every API key
- Reset one user at any time; the next cycle starts at the reset timestamp
- Reset all active Keeper users with one confirmed action
- Calendar-month or fixed-day cycles, with configurable quota, RPM, concurrency, and enable state
- API keys, aliases, and model prices synchronized from Keeper at the configured
  `KEEPER_SYNC_INTERVAL_SECONDS` interval (60 seconds in the production setup)
- Actual token cost settled from CLIProxyAPI's authenticated `usage` subscription
- Immediate reservation release when CLIProxyAPI returns a non-success status or the connection fails before a response
- Non-streaming and SSE streaming proxy support
- SQLite WAL ledger with deduplication by API key and request ID
- Loopback-only administration UI with HTTP Basic authentication
- Raw API keys are used in memory for hashing but are never stored in the quota database
- Bearer-authenticated endpoints expose the caller's cycle quota and the cached Codex weekly limit

## Period Semantics

Each user has a separate `period_started_at` and `period_ends_at`.

- The default calendar cycle starts on the first day of the current month and resets on the first day of the next month.
- Resetting a user at `2026-08-17 10:30` starts a new cycle at that exact time.
- On a calendar cycle, that user's next boundary is `2026-09-01 00:00`.
- Fixed cycles run from their start timestamp for 7 days, 30 days, or any custom 1-3650 day duration.
- Changing the cycle rule immediately starts a new period.
- Resetting starts a zero-usage period but does not delete historical ledger entries.
- Requests already in flight at reset time remain attached to their original period.

## Requirements

- Linux with Python 3.10 or newer
- Python `requests` and `PyYAML` packages
- CLIProxyAPI with `usage-statistics-enabled: true`
- CPA Usage Keeper with authentication enabled
- Nginx or another TLS reverse proxy

The gateway expects CLIProxyAPI's authenticated RESP-compatible usage channel:

```text
AUTH <CPA_MANAGEMENT_KEY>
SUBSCRIBE usage
```

Use subscription mode. Do not consume Keeper's queue with `LPOP` because that
would compete with Keeper for events.

## Installation

Install the runtime and clone the project:

```bash
sudo apt update
sudo apt install -y python3 python3-requests nginx
sudo git clone https://github.com/ColyFu/cpa-quota-gateway.git /opt/cpa-quota-gateway
sudo chown -R root:root /opt/cpa-quota-gateway
```

Create the service configuration from the example and set the three secrets:

```bash
sudo install -m 0600 /opt/cpa-quota-gateway/gateway.env.example /etc/cpa-quota-gateway.env
sudo editor /etc/cpa-quota-gateway.env
```

Required secrets:

- `KEEPER_LOGIN_PASSWORD`: Keeper administrator login password
- `CPA_MANAGEMENT_KEY`: CLIProxyAPI management key
- `QUOTA_ADMIN_TOKEN`: a new random admin password, generated with `openssl rand -hex 32`

Install and start the service:

```bash
sudo install -m 0644 /opt/cpa-quota-gateway/cpa-quota-gateway.service /etc/systemd/system/cpa-quota-gateway.service
sudo systemctl daemon-reload
sudo systemctl enable --now cpa-quota-gateway.service
sudo systemctl status cpa-quota-gateway.service
```

The provided unit uses a systemd dynamic user and writes only to
`/var/lib/cpa-quota-gateway`.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `LISTEN_HOST` | `127.0.0.1` | Gateway bind address; keep it private |
| `LISTEN_PORT` | `8320` | Gateway proxy and admin port |
| `UPSTREAM_URL` | `http://127.0.0.1:8317` | CLIProxyAPI upstream |
| `UPSTREAM_HEADER_RETRY_ATTEMPTS` | `2` | Retries before forwarding response headers for upstream 500/502/503/504 or transport errors; `0` retries indefinitely, positive values add that many retries, and `-1` disables local replay |
| `UPSTREAM_HEADER_RETRY_BUDGET_SECONDS` | `0` | Total header-retry budget; `0` waits indefinitely |
| `UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS` | `0` | Maximum wait for headers on one upstream attempt; `0` waits indefinitely |
| `UPSTREAM_STREAM_TIMEOUT_SECONDS` | `0` | Socket timeout after response headers have been forwarded; `0` waits indefinitely |
| `UPSTREAM_MAX_ATTEMPTS_PER_REQUEST` | `3` | Shared attempt ceiling across client reconnects; `0` removes the ceiling but still prevents concurrent duplicate execution |
| `UPSTREAM_RETRY_MAX_BACKOFF_SECONDS` | `30` | Maximum exponential retry backoff, with jitter |
| `UPSTREAM_FAILURE_COOLDOWN_SECONDS` | `60` | Suppress identical failed requests before allowing a fresh attempt budget |
| `UPSTREAM_LARGE_REQUEST_BYTES` | `2097152` | Request size at which the long-header policy applies |
| `UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS` | `0` | Header wait for one large-request attempt; `0` waits indefinitely |
| `UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS` | `2` | Retries for a large request before the first response byte; `0` retries indefinitely, `-1` disables replay, and no POST is replayed after the first byte |
| `UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS` | `0` | Total large-request header wait budget; `0` waits indefinitely |
| `CPA_CONFIG_PATH` | adjacent CPA `config.yaml` | CPA configuration controlled by delayed failover |
| `PRO_FAILOVER_ENABLED` | `true` | Enable delayed Pro OAuth failover gating |
| `PRO_FAILOVER_WINDOW_SECONDS` | `600` | Required span from first to latest failure; any success resets it |
| `KEEPER_URL` | `http://127.0.0.1:8318` | Keeper admin API |
| `KEEPER_SYNC_INTERVAL_SECONDS` | `60` | Interval for refreshing Keeper API keys and model prices while running; minimum 5 seconds |
| `KEEPER_LOGIN_PASSWORD` | required | Keeper admin password for metadata sync |
| `CPA_MANAGEMENT_KEY` | required | CLIProxyAPI usage subscription authentication |
| `QUOTA_DB` | `data/quota.db` | Durable quota SQLite path |
| `BILLING_TIMEZONE` | `Asia/Shanghai` | Period boundary display and calendar arithmetic |
| `DEFAULT_BUDGET_USD` | `1000` | Initial cycle quota for newly synchronized users |
| `DEFAULT_RPM` | `0` | Initial per-key request rate; `0` means unlimited, and a positive value queues for capacity instead of returning 429 |
| `DEFAULT_CONCURRENCY` | `0` | Initial personal allowance under contention; spare global slots may be borrowed, and `0` means only the global limit applies |
| `GLOBAL_CONCURRENCY` | `10` | Initial total executing request limit; the value saved in the admin UI takes precedence, and `0` disables the global ceiling |
| `RESERVATION_USD` | `2` | Minimum request pre-authorization |
| `RESERVATION_TTL_SECONDS` | `0` | Keep reservations until usage settlement; a positive value enables orphan cleanup after that many seconds |
| `DEFAULT_MAX_OUTPUT_TOKENS` | `128000` | Conservative bound when the request omits a limit |
| `QUOTA_ADMIN_TOKEN` | required | Admin UI/API password |
| `KEEPER_USAGE_SYNC_INTERVAL_SECONDS` | `30` | Interval for replaying durable Keeper usage events; minimum 5 seconds |
| `KEEPER_USAGE_BATCH_SIZE` | `250` | Keeper usage events replayed per batch |
| `KEEPER_DB` | sibling Keeper DB | Keeper SQLite path used for durable usage replay and history backfill |

Changing a default affects only API keys synchronized for the first time.
Existing user policies remain unchanged.

### Reliability Semantics

- The gateway and Nginx impose no request-body size cap. Long Codex histories are read and forwarded in full.
- A zero timeout or time budget means unlimited waiting for a live attempt. Recoverable failures use at most three shared attempts by default, with exponential backoff and a 60-second cooldown.
- A disconnect after bytes have been sent cannot be replayed safely as a POST. The gateway releases the reservation; clients can retry with the same `X-Client-Request-Id` when their API supports it.
- Identical bodies for the same key, method, and path share a fingerprint even if the client request ID changes. Concurrent duplicates receive `409 duplicate_request_in_flight`; failed requests in cooldown receive `409 duplicate_retry_limit` with `Retry-After`.
- A background monitor cancels upstream work when the downstream client disconnects. Zero-token failure events are observed for failover but do not write the quota ledger or release an unrelated live reservation; successful and billable events retain normal settlement.
- The cycle USD quota remains an intentional accounting boundary and returns `429 quota_exceeded` when exhausted. RPM and concurrency settings never reject a request; positive values only make it wait for capacity.
- The global queue holds at most ten executing requests by default, including their complete streams and retry loops. Requests first wait for a personal permit, then join the global queue. Personal permits cover executing requests and requests waiting in the global queue, independently of unsettled billing reservations. Waiting requests do not reserve quota or call CPA. Disconnected clients leave either queue, and duplicate fingerprints are rejected before queuing. `/admin/health` exposes the concurrency limit, executing count, and both queue counts. The limit applies to one gateway process; run one production instance.

### Concurrency and Priority Controls

The admin page includes a global concurrency input, live executing/personal-waiting/global-waiting counts, a personal concurrency input for each key, and an immediately saved superuser switch. Global limits and personal policies persist in SQLite and apply immediately, including to already queued requests. Reducing the global limit lets existing requests finish before admitting more. A personal limit of `0` means only the global limit applies.

Requests wait in personal FIFO order until a personal permit is available, then join the tail of the global queue. The global queue gives superusers priority and preserves global-entry order within each priority class, not original request arrival order. For example, A arrives at 09:45 but cannot leave its personal queue until 09:48; B enters the global queue at 09:47. With equal priority, B executes before A.

When no other user is waiting in either queue, a user may bypass its personal allowance to borrow free global slots. For example, with a global limit of `4` and a personal allowance of `2`, one user can execute four requests while other users are idle; a fifth waits. Borrowing never exceeds the global limit or interrupts existing work. Once another user queues, borrowing stops until no other user is waiting; eligible global requests receive the next released slots. New and migrated users default to normal priority, and enabled status and quotas still apply. Continuous eligible superuser traffic can delay normal users.

`GET /admin/concurrency` returns `limit`, `active`, `queued`, `personal_queued`, and `global_queued` (`queued` is the sum of both stages); `PUT /admin/concurrency` accepts `{"global_limit":10}`. `PUT /admin/policy` accepts `concurrency` (non-negative integer) and `is_superuser` (boolean), together with `key_hash`. Mutations require the usual admin authentication and `X-Quota-Admin-Request: 1` header.

## Keeper Synchronization

The gateway signs in to Keeper locally and reads:

```text
GET /api/v1/usage/api-keys/settings
GET /api/v1/pricing
GET /api/v1/quota/inspection
POST /api/v1/quota/cache
```

The first endpoint supplies API keys and aliases. The second supplies the same
model price settings Keeper uses for cost estimates, including prompt,
completion, cache-read, cache-write, and the model multiplier.

The quota endpoints provide Keeper's cached Codex upstream limits. After a
successful synchronization, API keys removed from Keeper are deleted from the
gateway together with their policies, periods, reservations, and ledger rows.
A failed synchronization is rolled back and retains the last successful cache.

The gateway immediately hashes each API key with SHA-256. Only the hash, a
masked hint, and the alias are persisted. A failed Keeper sync retains the last
successful key and price snapshot, so temporary Keeper downtime does not stop
API traffic. Redis remains the low-latency usage path; the gateway also replays
Keeper's durable `usage_events` table by monotonically increasing `id`, with a
checkpoint in its own `settings` table, so subscriber disconnects and restarts
do not leave reservations stuck.

Run an immediate synchronization with:

```bash
sudo systemctl restart cpa-quota-gateway.service
```

The administration UI also provides a `同步 Keeper` command.

## Nginx

Expose only Nginx to the Internet. Keep ports `8317`, `8318`, and `8320`
blocked in the cloud firewall/security group.

```nginx
location /v1/ {
    proxy_pass http://127.0.0.1:8320;
    proxy_http_version 1.1;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_connect_timeout 75s;
    proxy_send_timeout 365d;
    proxy_read_timeout 365d;
    proxy_next_upstream off;

    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
}
```

After editing Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## Administration UI

The admin page is intentionally not published by the example Nginx location.
Open an SSH tunnel:

```bash
ssh -L 8320:127.0.0.1:8320 user@your-server
```

Then open `http://127.0.0.1:8320/admin/` and sign in with:

- Username: `admin`
- Password: the configured `QUOTA_ADMIN_TOKEN`

The UI can edit each user's quota, cycle rule, RPM, concurrency, and enable
state. A cycle can be a calendar month, 7 days, 30 days, or a custom 1-3650
days. Aliases are read-only because Keeper is their source of truth. Positive
RPM/concurrency values queue requests instead of returning `429`; keep them at
`0` when no artificial throttling is desired.

## User Query Endpoints

Both endpoints authenticate with the caller's own Bearer API key:

```bash
curl -H "Authorization: Bearer $API_KEY" https://SERVER/v1/limit
curl -H "Authorization: Bearer $API_KEY" https://SERVER/v1/usage
```

`GET /v1/limit` returns only the caller's quota, usage, reset time, and cycle:

```json
{"limit_usd":1000.0,"used_usd":12.5,"reset_at":"2026-09-01T00:00:00+08:00","cycle":{"type":"calendar_month","days":null}}
```

`GET /v1/usage` returns the Codex primary weekly window from Keeper's cache:

```json
{"used_percent":63.0,"reset_at":"2026-08-20T16:07:18+08:00"}
```

The current deployment has one Codex upstream account. With multiple accounts,
the endpoint returns the first cached seven-day primary window; it does not
aggregate accounts.

### Reset Behavior

`重置` starts a new period only for the selected user. `重置全部额度` starts a
new period for every API key currently active in Keeper. Both operations keep
old periods and ledger rows for auditing.

The equivalent API calls are:

```bash
curl -u "admin:$QUOTA_ADMIN_TOKEN" \
  -H 'X-Quota-Admin-Request: 1' \
  -H 'Content-Type: application/json' \
  -d '{"key_hash":"<sha256>"}' \
  http://127.0.0.1:8320/admin/policy/reset

curl -u "admin:$QUOTA_ADMIN_TOKEN" \
  -H 'X-Quota-Admin-Request: 1' \
  -X POST http://127.0.0.1:8320/admin/reset-all
```

## Existing-Month Backfill

For an existing Keeper installation, the optional command imports the current
calendar month's historical events and seeds all active Keeper keys:

```bash
python3 gateway.py --backfill-keeper-month
```

Set `KEEPER_DB` to Keeper's SQLite path and run it with filesystem access to
both databases. The import is idempotent. Normal operation uses the Redis
subscription for low latency and Keeper's SQLite event log for durable catch-up.

## Verification

Run the unit tests:

```bash
python3 test_gateway.py
```

Check metadata and proxy health:

```bash
python3 gateway.py --check
curl -u "admin:$QUOTA_ADMIN_TOKEN" http://127.0.0.1:8320/admin/health
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8320/v1/models
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8320/v1/limit
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8320/v1/usage
```

## Upgrade and Rollback

Keep the database and `/etc/cpa-quota-gateway.env` outside the repository:

```bash
cd /opt/cpa-quota-gateway
sudo git pull --ff-only
sudo systemctl restart cpa-quota-gateway.service
```

To bypass the gateway during rollback, point Nginx back to
`http://127.0.0.1:8317`, validate the configuration, and reload Nginx. Do not
open port 8317 publicly.

## Security Notes

- Never publish `/admin/` through the public Nginx server.
- Keep `gateway.env`, Keeper passwords, management keys, and API keys out of Git.
- Keep CLIProxyAPI's public port blocked or users can bypass quota enforcement.
- Back up the SQLite database before upgrades that change accounting behavior.
- A strict final token cost is unknowable before model output completes. The
  gateway makes a conservative reservation first, then settles actual usage.

## License

MIT

## Recovery from transient upstream failures

Set `UPSTREAM_TRANSIENT_RECOVERY_SECONDS=600` to start a ten-minute recovery
window after the first transient failure. Set both header retry-attempt options
and `UPSTREAM_MAX_ATTEMPTS_PER_REQUEST` to `0` to use this window instead of
an attempt ceiling. A 15-second initial backoff and 60-second cap are recommended;
valid upstream `Retry-After` delays are honored. Initially healthy long turns
keep their existing timeout policy.

Retries stop on client cancellation and only occur before downstream output.
Authentication and quota errors are forwarded directly. If recovery fails,
the last upstream HTTP status and error body are retained, including reconnects
during cooldown. Error responses over 16 KiB are forwarded without retrying.
A gateway-generated error is used only when no upstream response is available.

CC Switch's Codex first-byte timeout must exceed the recovery window (900 seconds
is recommended). Increase shorter idle timeouts as well; the actual Codex provider
can use `stream_idle_timeout_ms = 900000`. The gateway cannot resume a turn that
the client has already ended.

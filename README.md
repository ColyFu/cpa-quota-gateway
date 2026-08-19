# CPA Quota Gateway

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

- Per-user USD quota with atomic pre-authorization before forwarding a request
- Independent billing cycle for every API key
- Reset one user at any time; the next cycle starts at the reset timestamp
- Reset all active Keeper users with one confirmed action
- Configurable cycle length in calendar months, quota, RPM, concurrency, and enable state
- API keys and aliases synchronized from Keeper every five minutes
- Model prices synchronized from Keeper every five minutes and cached locally
- Actual token cost settled from CLIProxyAPI's authenticated `usage` subscription
- Immediate reservation release when CLIProxyAPI returns a non-success status or the connection fails before a response
- Non-streaming and SSE streaming proxy support
- SQLite WAL ledger with deduplication by API key and request ID
- Loopback-only administration UI with HTTP Basic authentication
- Raw API keys are used in memory for hashing but are never stored in the quota database

## Period Semantics

Each user has a separate `period_started_at` and `period_ends_at`.

- A user initialized on a normal calendar cycle starts on the first day of the current month.
- Resetting a user at `2026-08-17 10:30` starts a new cycle at that exact time.
- With a one-month cycle, that user's next boundary is `2026-09-17 10:30`.
- Calendar arithmetic is used, so a reset on the last day of a month is capped to the last valid day of the target month.
- Resetting starts a zero-usage period but does not delete historical ledger entries.
- Requests already in flight at reset time remain attached to their original period.

## Requirements

- Linux with Python 3.10 or newer
- Python `requests` package
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
| `KEEPER_URL` | `http://127.0.0.1:8318` | Keeper admin API |
| `KEEPER_LOGIN_PASSWORD` | required | Keeper admin password for metadata sync |
| `CPA_MANAGEMENT_KEY` | required | CLIProxyAPI usage subscription authentication |
| `QUOTA_DB` | `data/quota.db` | Durable quota SQLite path |
| `BILLING_TIMEZONE` | `Asia/Shanghai` | Period boundary display and calendar arithmetic |
| `DEFAULT_MONTHLY_BUDGET_USD` | `1000` | Initial quota for newly synchronized users |
| `DEFAULT_RPM` | `120` | Initial per-key request limit per minute; `0` disables it |
| `DEFAULT_CONCURRENCY` | `20` | Initial concurrent in-flight limit; `0` disables it |
| `RESERVATION_USD` | `2` | Minimum request pre-authorization |
| `RESERVATION_TTL_SECONDS` | `3600` | Stale reservation lifetime |
| `DEFAULT_MAX_OUTPUT_TOKENS` | `128000` | Conservative bound when the request omits a limit |
| `QUOTA_ADMIN_TOKEN` | required | Admin UI/API password |
| `KEEPER_DB` | sibling Keeper DB | Optional one-time history import source |

Changing a default affects only API keys synchronized for the first time.
Existing user policies remain unchanged.

## Keeper Synchronization

The gateway signs in to Keeper locally and reads:

```text
GET /api/v1/usage/api-keys/settings
GET /api/v1/pricing
```

The first endpoint supplies API keys and aliases. The second supplies the same
model price settings Keeper uses for cost estimates, including prompt,
completion, cache-read, cache-write, and the model multiplier.

The gateway immediately hashes each API key with SHA-256. Only the hash, a
masked hint, and the alias are persisted. A failed Keeper sync retains the last
successful key and price snapshot, so temporary Keeper downtime does not stop
API traffic.

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
    proxy_connect_timeout 10s;
    proxy_send_timeout 900s;
    proxy_read_timeout 900s;

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

The UI can edit each user's quota, cycle length, RPM, concurrency, and enable
state. Aliases are read-only because Keeper is their source of truth.

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
both databases. The import is idempotent. It is only a migration adapter;
normal real-time operation uses Keeper's HTTP API and CLIProxyAPI's usage
subscription rather than reading Keeper's database.

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

# CPA Quota Gateway

Keeper 整合控制台、账号调度与构建说明见 [整合版说明](INTEGRATION.zh-CN.md)。旧版生产基线保留在 `main` 分支（`2408701`）。

简体中文 | [English](README.md)

`cpa-quota-gateway` 是部署在 CLIProxyAPI 前面的独立额度和限流网关。它从
[CPA Usage Keeper](https://github.com/Willxup/cpa-usage-keeper) 读取 API Key、
用户别名和模型价格，但不修改 Keeper 的程序、前端或数据库结构。

```text
公网 -> Nginx :443 -> CPA Quota Gateway :8320 -> CLIProxyAPI :8317
                              |
                              +-> 独立 SQLite 配额数据库

CPA Usage Keeper :8318 -> API Key、别名、模型价格
CLIProxyAPI :8317      -> 实时 usage 事件订阅
```

升级 Keeper 或 CLIProxyAPI 时，配额策略、用户周期和计费流水不会被覆盖。

## 功能

- 可选的 Pro 延迟故障转移门控：只有 Codex OAuth 请求连续失败达到配置窗口，
  才解除备用 Codex API 的模型禁用；任一 OAuth 请求成功后立即重新禁用备用 API
- 每个 API Key 单独设置美元额度、RPM、并发数和启停状态
- 每个用户拥有独立的开始时间、结束时间和周期规则
- 单独重置用户额度，重置时刻就是新周期的开始时间
- 二次确认后一键重置 Keeper 中所有有效用户
- 按 `KEEPER_SYNC_INTERVAL_SECONDS` 从 Keeper 同步 API Key、别名和模型价格（生产默认 60 秒）
- Keeper 暂时不可用时继续使用最后一次成功同步的缓存
- 请求转发前原子预占额度，结束后按实际 token 成本结算
- CLIProxyAPI 返回非成功状态或取得响应前连接失败时立即释放预占，避免失败重试占满并发
- 支持普通 JSON 响应和 SSE 流式响应
- SQLite WAL 流水，按 API Key 和请求 ID 防止重复计费
- 独立管理页面，不修改 Keeper 前端
- 配额数据库不保存明文 API Key，只保存 SHA-256、掩码和别名
- 用户可用自己的 API Key 查询本周期额度及 Codex 上游周限额

## 独立周期和重置规则

每个用户分别保存 `period_started_at` 和 `period_ends_at`。

- 默认使用自然月，新用户从当月第一天开始计算，下月第一天重置。
- 用户在 `2026-08-17 10:30` 重置后，新周期从该时刻开始。
- 周期为自然月时，结束时间是 `2026-09-01 00:00`。
- 固定天数周期从开始时刻起计算，支持 7 天、30 天或自定义 1–3650 天。
- 修改周期规则会立即从修改时刻开启一个新周期。
- 重置不会删除历史周期和历史流水，只会新建一个用量为零的周期。
- 重置时已经在处理的请求仍结算到旧周期，避免账目串期。

## 环境要求

- Linux
- Python 3.10 或更高版本
- Python `requests` 和 `PyYAML` 软件包
- CLIProxyAPI 开启 `usage-statistics-enabled: true`
- CPA Usage Keeper 开启登录认证
- Nginx 或其他 HTTPS 反向代理

网关使用 CLIProxyAPI 的认证订阅协议实时接收用量：

```text
AUTH <CPA_MANAGEMENT_KEY>
SUBSCRIBE usage
```

网关使用订阅模式，不会通过 `LPOP` 抢走 Keeper 的用量事件。

## 安装

```bash
sudo apt update
sudo apt install -y python3 python3-requests nginx
sudo git clone https://github.com/ColyFu/cpa-quota-gateway.git /opt/cpa-quota-gateway
sudo chown -R root:root /opt/cpa-quota-gateway
```

创建配置文件：

```bash
sudo install -m 0600 /opt/cpa-quota-gateway/gateway.env.example /etc/cpa-quota-gateway.env
sudo editor /etc/cpa-quota-gateway.env
```

必须填写三个密钥：

- `KEEPER_LOGIN_PASSWORD`：Keeper 管理员登录密码
- `CPA_MANAGEMENT_KEY`：CLIProxyAPI 管理密钥
- `QUOTA_ADMIN_TOKEN`：网关管理页面密码，可用 `openssl rand -hex 32` 生成

安装 systemd 服务：

```bash
sudo install -m 0644 /opt/cpa-quota-gateway/cpa-quota-gateway.service /etc/systemd/system/cpa-quota-gateway.service
sudo systemctl daemon-reload
sudo systemctl enable --now cpa-quota-gateway.service
sudo systemctl status cpa-quota-gateway.service
```

示例单元使用 systemd 动态用户，数据库保存在
`/var/lib/cpa-quota-gateway/quota.db`。

## 配置说明

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `LISTEN_HOST` | `127.0.0.1` | 监听地址，应保持为本机回环地址 |
| `LISTEN_PORT` | `8320` | API 代理和管理页面端口 |
| `UPSTREAM_URL` | `http://127.0.0.1:8317` | CLIProxyAPI 地址 |
| `UPSTREAM_HEADER_RETRY_ATTEMPTS` | `2` | 仅在向客户端发送响应头前，对上游 500/502/503/504 或传输错误再试的次数；`0` 表示无限重试，正数表示重试次数，`-1` 表示不做本地重放 |
| `UPSTREAM_HEADER_RETRY_BUDGET_SECONDS` | `0` | 响应头重试总预算；`0` 表示不限时等待 |
| `UPSTREAM_HEADER_ATTEMPT_TIMEOUT_SECONDS` | `0` | 单次等待上游响应头的最长时间；`0` 表示不限时等待 |
| `UPSTREAM_STREAM_TIMEOUT_SECONDS` | `0` | 已发送响应头后，SSE 数据读取的 socket 超时；`0` 表示不限时等待 |
| `UPSTREAM_MAX_ATTEMPTS_PER_REQUEST` | `3` | 客户端重连共享的尝试次数上限；`0` 表示不限制次数，但仍阻止重复请求并行执行 |
| `UPSTREAM_RETRY_MAX_BACKOFF_SECONDS` | `30` | 带随机抖动的指数退避上限 |
| `UPSTREAM_FAILURE_COOLDOWN_SECONDS` | `60` | 同一失败请求的冷却期，期满后获得新的尝试预算 |
| `UPSTREAM_LARGE_REQUEST_BYTES` | `2097152` | 启用大请求长响应头等待策略的请求大小阈值 |
| `UPSTREAM_LARGE_REQUEST_HEADER_TIMEOUT_SECONDS` | `0` | 单次大请求等待响应头的最长时间；`0` 表示不限时等待 |
| `UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS` | `2` | 大请求在首个响应字节前允许的重试次数；`0` 表示无限重试，`-1` 表示不重放；首字节后不会透明重放 |
| `UPSTREAM_LARGE_REQUEST_RETRY_BUDGET_SECONDS` | `0` | 大请求等待响应头的总预算；`0` 表示不限时等待 |
| `CPA_CONFIG_PATH` | 相邻 CPA 的 `config.yaml` | 延迟故障转移需要修改的 CPA 配置文件 |
| `PRO_FAILOVER_ENABLED` | `true` | 启用 Pro OAuth 延迟故障转移门控 |
| `PRO_FAILOVER_WINDOW_SECONDS` | `600` | 首次到最近一次失败必须持续的时间；任一成功都会清零 |
| `KEEPER_URL` | `http://127.0.0.1:8318` | Keeper 管理 API 地址 |
| `KEEPER_SYNC_INTERVAL_SECONDS` | `60` | 运行中自动同步 Keeper API Key 和模型价格的间隔，最小 5 秒 |
| `KEEPER_LOGIN_PASSWORD` | 必填 | Keeper 管理员密码 |
| `CPA_MANAGEMENT_KEY` | 必填 | CLIProxyAPI 用量订阅认证密钥 |
| `QUOTA_DB` | `data/quota.db` | 独立 SQLite 数据库路径 |
| `BILLING_TIMEZONE` | `Asia/Shanghai` | 周期计算和页面显示时区 |
| `DEFAULT_BUDGET_USD` | `1000` | 新同步用户的默认周期额度 |
| `DEFAULT_RPM` | `0` | 新用户默认每分钟请求数；`0` 表示不限制，正数只会等待容量释放，不会因速率返回 429 |
| `DEFAULT_CONCURRENCY` | `0` | 新用户在有竞争时的个人并发基准；空闲总名额可借用，`0` 表示只受总并发限制 |
| `GLOBAL_CONCURRENCY` | `10` | 首次启动的总并发初始值，之后以管理页保存的数据库设置为准；`0` 表示关闭全局上限 |
| `RESERVATION_USD` | `2` | 单次请求最低预占金额 |
| `RESERVATION_TTL_SECONDS` | `0` | 保持预占直到 usage 事件结算；正数才会在对应秒数后清理孤立预占 |
| `DEFAULT_MAX_OUTPUT_TOKENS` | `128000` | 请求未指定输出上限时的保守估计 |
| `QUOTA_ADMIN_TOKEN` | 必填 | 管理页面和管理 API 密码 |
| `KEEPER_USAGE_SYNC_INTERVAL_SECONDS` | `30` | 从 Keeper SQLite 追赶 usage 事件的间隔，最小 5 秒 |
| `KEEPER_USAGE_BATCH_SIZE` | `250` | 每批追赶的 Keeper usage 事件数 |
| `KEEPER_DB` | Keeper sibling DB | 运行时 durable usage 追赶和历史回填使用的 SQLite 路径 |

修改默认值只影响之后第一次同步的新 API Key，不会覆盖现有用户策略。

### 请求可靠性语义

- 网关和 Nginx 不设置请求体大小上限；超长上下文会完整读入并转发。
- 等待超时和时间预算仍可用 `0` 表示无限，保留成功长请求的执行时间；可恢复失败默认最多共享尝试三次，使用指数退避，失败后冷却 60 秒。
- 已经向客户端发送数据后发生的断流不能安全地自动重放 POST；网关会释放预占，客户端可以使用相同的 `X-Client-Request-Id` 重新发起请求。
- 同一 Key、方法、路径和完全相同的请求体共享指纹，客户端更换请求 ID 也不能绕过并发去重；重复执行返回 `409 duplicate_request_in_flight`，失败冷却期间返回 `409 duplicate_retry_limit` 和 `Retry-After`。
- 后台监测客户端断开并取消上游请求。零 Token 失败仍用于故障转移观察，但不写配额账本、不提前消费其他活跃预占；成功和有 Token 的事件继续完整结算。
- 周期美元额度是业务账务边界，额度耗尽仍会返回 `429 quota_exceeded`；RPM/并发策略本身不会拒绝请求。
- 全局默认同时执行最多 10 个请求，名额保持到完整流式响应或重试流程结束。请求先等个人名额，获准后进入总队列；个人名额包含执行中和总队列等待的请求，不依赖等待结算的账单预占。排队期间不预占额度、不请求 CPA；客户端断开后退出所在队列，相同请求指纹在入队前去重。`/admin/health` 显示并发上限、执行数和两阶段排队数。上限作用于单个网关进程，生产应保持单实例。

### 并发设置与超级用户

管理页面顶部可保存总并发，并分别显示执行数、个人排队数和总排队数；每个用户行可以保存个人并发，超级用户开关即时保存。设置持久化到 SQLite，保存后立即影响队列，重启后保留。降低总并发不会中断已有请求，等执行数降下来后再放行。个人并发为 `0` 时只受总并发限制。

请求先在个人队列按到达顺序等待个人名额，获得名额后追加到总队列末尾。总队列按超级用户优先、同级按进入总队列的时间先进先出，不追溯原始请求时间。例如 A 在 9:45 发起请求、9:48 才出个人队列，B 在 9:47 已进入总队列；同优先级时 B 先执行。

只有当两个队列中都没有其他用户等待时，才允许突破个人并发借用空闲总名额。例如总并发 `4`、个人并发 `2`：只有一个用户提交请求时可同时执行 `4` 个，第 `5` 个等待；有其他用户排队后停止借用，已放行的请求不被打断，总队列中可执行的请求优先获得下一次释放的名额。超级用户仍受总并发、启用状态和额度约束。新用户与迁移用户默认为普通用户；持续不断的超级用户请求可能延长普通用户的等待。

`GET /admin/concurrency` 返回 `limit/active/queued/personal_queued/global_queued`，其中 `queued` 是两阶段等待数之和；`PUT /admin/concurrency` 接收 `{"global_limit":10}`。`PUT /admin/policy` 可设置 `concurrency`（非负整数）和 `is_superuser`（布尔值），通过 `key_hash` 指定用户；写操作沿用管理员认证与 `X-Quota-Admin-Request: 1`。

## Keeper 数据同步

网关通过 Keeper 的本机管理 API 读取：

```text
GET /api/v1/usage/api-keys/settings
GET /api/v1/pricing
GET /api/v1/quota/inspection
POST /api/v1/quota/cache
```

第一项提供 API Key 和别名，第二项提供普通输入、输出、缓存读取、缓存写入
以及价格倍率。后两项提供 Keeper 已缓存的 Codex 上游限额。API Key 在内存中
立即计算 SHA-256，配额数据库不会保存明文。

Keeper 成功同步后，Keeper 已删除的 API Key 会从网关删除，其策略、周期、
reservation 和计费流水也会一并清理。Keeper 同步失败时事务会回滚，继续使用
最后一次成功同步的缓存，不会因暂时不可用而删除用户。

服务启动时立即同步并从 Keeper usage 事件表追赶，之后分别按
`KEEPER_SYNC_INTERVAL_SECONDS`（元数据，生产默认 60 秒）和
`KEEPER_USAGE_SYNC_INTERVAL_SECONDS`（usage，生产默认 30 秒）运行。Redis
订阅用于低延迟，Keeper 的 durable 事件追赶用于断线和重启恢复。管理页面的
`同步 Keeper` 按钮可立即同步元数据。

## Nginx 配置

公网只开放 Nginx 的 443 端口。云安全组应继续禁止公网访问 8317、8318 和
8320，防止绕过限额网关。

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

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 管理页面

管理页面默认不对公网开放。通过 SSH 隧道访问：

```bash
ssh -L 8320:127.0.0.1:8320 user@your-server
```

浏览器打开 `http://127.0.0.1:8320/admin/`：

- 用户名：`admin`
- 密码：配置文件中的 `QUOTA_ADMIN_TOKEN`

页面可以设置每个用户的额度、周期规则、RPM、并发和启停状态。启停开关切换后立即保存，失败时恢复原状态；其他策略字段仍需点击“保存”。

“关闭全部 Key / 打开全部 Key”经确认后原子更新当前所有 Key，不受搜索过滤影响，也包含邓钊和傅军；不改变额度、周期或历史用量。批量接口为 `PUT /admin/policies/enabled`，请求体为 `{"enabled":false}` 或 `{"enabled":true}`，需要管理员认证和确认请求头。关闭阻止后续新请求，已经开始的请求可能继续完成；之后新增的 Key 仍按默认策略启用。

周期可选自然月、
固定 7 天、固定 30 天或自定义 1–3650 天。用户别名以 Keeper 为唯一来源，因此
在网关页面中只读。

RPM 和并发字段保留用于账务/运维兼容；设置为正数时网关排队等待容量，不会主动返回
`429 rate_limit_error`。不需要人为节流时保持为 `0`。

`重置` 只为当前用户开启新周期；`重置全部额度` 为 Keeper 当前所有有效用户
开启新周期。两个操作都保留历史数据。

所有修改类管理 API 还必须携带 `X-Quota-Admin-Request: 1` 请求头，用于阻止
浏览器跨站请求伪造；管理页面会自动添加该请求头。

## 用户查询接口

两个接口均使用调用者自己的 Bearer API Key，不需要管理令牌：

```bash
curl -H "Authorization: Bearer $API_KEY" https://SERVER/v1/limit
curl -H "Authorization: Bearer $API_KEY" https://SERVER/v1/usage
```

`GET /v1/limit` 只返回当前 Key 的周期额度、已用金额、重置时间和周期规则：

```json
{"limit_usd":1000.0,"used_usd":12.5,"reset_at":"2026-09-01T00:00:00+08:00","cycle":{"type":"calendar_month","days":null}}
```

`GET /v1/usage` 从 Keeper 配额缓存读取 Codex 主周窗口，只返回周限额已用百分比
和重置时间：

```json
{"used_percent":63.0,"reset_at":"2026-08-20T16:07:18+08:00"}
```

当前部署只有一个 Codex 上游账号；存在多个账号时，接口返回 Keeper 缓存中的
第一个 7 天主窗口，不做跨账号汇总。

## 现有月份历史回填

已有 Keeper 数据时，可选执行一次：

```bash
python3 gateway.py --backfill-keeper-month
```

需要设置 `KEEPER_DB` 并确保命令能够读取 Keeper 数据库、写入网关数据库。
导入支持重复执行，不会重复计费。运行中的网关也会通过同一数据库按
`usage_events.id` 增量追赶漏掉的事件，checkpoint 保存在网关 `settings` 表。

## 测试和检查

```bash
python3 test_gateway.py
python3 gateway.py --check
curl -u "admin:$QUOTA_ADMIN_TOKEN" http://127.0.0.1:8320/admin/health
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8320/v1/models
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8320/v1/limit
curl -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8320/v1/usage
```

## 更新和回滚

```bash
cd /opt/cpa-quota-gateway
sudo git pull --ff-only
sudo systemctl restart cpa-quota-gateway.service
```

数据库和 `/etc/cpa-quota-gateway.env` 位于仓库外，更新项目不会覆盖用户额度。

需要临时绕过网关时，把 Nginx 上游改回 `http://127.0.0.1:8317`，执行
`nginx -t` 后重新加载。不要因此对公网开放 8317。

## 安全说明

- 不要把 `/admin/` 暴露到公网。
- 不要提交 `gateway.env`、Keeper 密码、管理密钥或 API Key。
- 8317 对公网开放会导致用户绕过额度控制。
- 修改计费逻辑或升级前先备份网关 SQLite 数据库。
- 模型返回前无法知道最终 token 成本，网关会先保守预占，再按实际成本结算。

## License

MIT

## 上游临时失败自动恢复

`UPSTREAM_TRANSIENT_RECOVERY_SECONDS=600` 会在首次可重试的上游错误后开启
10 分钟恢复窗口。将 `UPSTREAM_HEADER_RETRY_ATTEMPTS`、
`UPSTREAM_LARGE_REQUEST_RETRY_ATTEMPTS` 和 `UPSTREAM_MAX_ATTEMPTS_PER_REQUEST`
设为 `0`，用时间窗口代替固定 3 次上限。建议初始退避 15 秒、最大退避 60 秒；
服务器返回 `Retry-After` 时遵守该延迟。正常长任务不会因为本恢复窗口而提前结束。

只在尚未向客户端转发输出时重试 500/502/503/504 或连接错误。用户断开连接后停止
等待。401/403/429 直接转发。恢复失败时保留最后一个上游错误的 HTTP 状态和正文；
冷却期间重连也保留该错误，不再以重试预算耗尽的 409 覆盖原因。错误缓存最大 16 KiB；
更大的错误响应直接透传。没有上游响应时才使用网关自己的连接错误。

客户端必须允许等待恢复窗口：CC Switch 的 Codex 首字节超时建议至少 900 秒；
如果其空闲超时也较短，同样调到 900 秒。可在实际使用的 Codex provider 配置中设置
`stream_idle_timeout_ms = 900000`。客户端仍可能因取消、网络断开或更短的总超时结束；
网关不能恢复客户端已经结束的回合。

# CPA Quota Gateway

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

- 每个 API Key 单独设置美元额度、RPM、并发数和启停状态
- 每个用户拥有独立的开始时间、结束时间和周期规则
- 单独重置用户额度，重置时刻就是新周期的开始时间
- 二次确认后一键重置 Keeper 中所有有效用户
- 每五分钟从 Keeper 同步 API Key、别名和模型价格
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
- Python `requests` 软件包
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
| `KEEPER_URL` | `http://127.0.0.1:8318` | Keeper 管理 API 地址 |
| `KEEPER_LOGIN_PASSWORD` | 必填 | Keeper 管理员密码 |
| `CPA_MANAGEMENT_KEY` | 必填 | CLIProxyAPI 用量订阅认证密钥 |
| `QUOTA_DB` | `data/quota.db` | 独立 SQLite 数据库路径 |
| `BILLING_TIMEZONE` | `Asia/Shanghai` | 周期计算和页面显示时区 |
| `DEFAULT_BUDGET_USD` | `1000` | 新同步用户的默认周期额度 |
| `DEFAULT_RPM` | `120` | 新用户默认每分钟请求数，`0` 表示关闭 |
| `DEFAULT_CONCURRENCY` | `20` | 新用户默认并发数，`0` 表示关闭 |
| `RESERVATION_USD` | `2` | 单次请求最低预占金额 |
| `RESERVATION_TTL_SECONDS` | `3600` | 异常预占的释放时间 |
| `DEFAULT_MAX_OUTPUT_TOKENS` | `128000` | 请求未指定输出上限时的保守估计 |
| `QUOTA_ADMIN_TOKEN` | 必填 | 管理页面和管理 API 密码 |
| `KEEPER_DB` | 可选 | 仅用于首次历史回填的 Keeper SQLite 路径 |

修改默认值只影响之后第一次同步的新 API Key，不会覆盖现有用户策略。

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

服务启动时立即同步，之后每五分钟同步一次。管理页面的 `同步 Keeper` 按钮可
立即同步。

## Nginx 配置

公网只开放 Nginx 的 443 端口。云安全组应继续禁止公网访问 8317、8318 和
8320，防止绕过限额网关。

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

页面可以设置每个用户的额度、周期规则、RPM、并发和启停状态。周期可选自然月、
固定 7 天、固定 30 天或自定义 1–3650 天。用户别名以 Keeper 为唯一来源，因此
在网关页面中只读。

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
导入支持重复执行，不会重复计费。日常运行不直接读取 Keeper SQLite。

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

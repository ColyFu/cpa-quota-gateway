# 当前目录与数据存储

2026-10-23 更新。正式目录只有：

- 开发：`/home/ubuntu/cpa-integration-dev`，其中 `gateway/`、`keeper/`、`cpa-reference/` 是源码，`tools/` 是本地开发工具。
- Gateway 部署：`/home/ubuntu/cpa-stack/cpa-quota-gateway`。
- CPA 部署：`/home/ubuntu/cpa-stack/cli-proxy-api`。
- Clash：`/home/ubuntu/clashctl`。

Gateway 的 Python 转发进程是 `cpa-quota-gateway.service`；原 Keeper 的登录、统计、认证管理后端作为 `cpa-quota-analytics.service` 运行，程序在 Gateway 的 `bin/analytics`，配置在 `analytics.env`。旧 `cpa-usage-keeper.service` 和旧部署目录已移除。统计后端仍是独立进程，不能单独关闭而继续使用这些管理功能。

所有额度、计费和用量统计共用一个数据库：

```text
/home/ubuntu/cpa-stack/cpa-quota-gateway/data/app.db
```

`app.db-wal` 和 `app.db-shm` 是同一个 SQLite 数据库的运行文件，不是额外数据库。Gateway 的 `QUOTA_DB`、`KEEPER_DB` 和统计后端的 `WORK_DIR/app.db` 指向同一文件。CPA 上报实际用量，由统计后端入库；CPA 不再为带可信记录标识的 Gateway 请求生成完整正文日志或临时正文文件。

## 保存的数据

- `usage_events`：请求时间、用户、模型、认证账号、输入/输出/推理/缓存 Token、成功状态、首字和总耗时等。90 天以外的明细转入同库的 `usage_events_archive`，历史数据继续保留。
- `policies`、`periods`、`ledger`：额度策略、周期和账务。账务与统计用途不同，不以删除账务来缩小数据库。
- `request_records`、`request_attempts`：接收时间、用户、模型、接口、会话哈希、状态和 CPA 尝试 ID；重试仍使用不同的 CPA 请求 ID，关联实际用量。
- `user_prompts`：仅保存用户角色的文本，gzip 压缩、按内容去重。排除 system/developer 指令、助手回复、工具调用与结果、图片/音频二进制、请求头及其他请求参数。
- `request_payloads`：仅保存有序的用户提示词 ID 引用；不会保存完整 request body。相同历史提示词复用同一份文本，重复说相同内容的独立轮次仍保留引用顺序。
- `request_history`：按请求关联统计与上述记录的查询视图，不复制一份 Token 数据。未收到 CPA 用量时，统计字段为空。

支持 Chat/Claude 的 user 消息、Responses 的 user 消息或字符串 input、Gemini 的 user 文本、传统字符串 prompt。只按明确的协议角色提取文本，不推测文本含义。优先使用已有 session/conversation 标识关联会话；没有标识时保存独立请求记录，文本仍可去重。Token 统计始终使用 CPA 的完整实际用量，不按保留下来的用户文本重新估算。

请求事件中的日志预览/下载显示“请求信息”和“用户提示词”，继续要求管理员登录。未曾记录的历史提示词无法补回。初版短暂保存的完整 request body 已全部转换为提示词引用，旧 CPA 错误日志提取用户文本后清理；数据库已整理并截断旧 WAL。

## 维护与构建

- 自动数据库备份已关闭，历史回退文件已按用户要求删除。
- 修改源码后运行开发目录的 `build_release.sh`；发布包 `release/` 可重新生成，平时无需保留。
- GitHub 仓库的 `integration-patches/build.sh` 可从固定上游版本及补丁重建两个 Go 程序和前端。
- `migrate_shared_store.py` 用于离线合并旧统计库与额度库；要求先停止两个写入进程，输出路径不能已存在。脚本逐表核对所有行和数据库完整性。
- `deploy/` 保存当前服务及主机配置。生产环境、认证文件、数据库、程序和本地接入说明不提交 Git。

迁移验证保留了当时的 141,973 条用量明细、140,042 条账务记录和 26 个用户。清理的 122,502 条旧记录是已结算的额度预占记录。95 项 Python 回归、Go 请求记录/权限/日志测试、前端类型检查和构建、隔离完整链路及生产页面检查通过；用户额度和账号调度配置未重置。

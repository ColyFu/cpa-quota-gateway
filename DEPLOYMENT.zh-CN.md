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
- `request_records`、`request_attempts`：接收时间、用户、模型、接口、会话哈希、请求状态、输入判断和 CPA 尝试 ID；重试仍使用不同的 CPA 请求 ID，关联实际用量。
- `user_prompts`：保存清理已知客户端包装后的候选用户文本，gzip 压缩、按内容去重。排除 system/developer 指令、助手回复、工具调用与结果、图片/音频、请求头；额外过滤已知环境信息、压缩摘要、标题生成和记忆整理任务。附件只提取用户要求，响应批注只提取批注及用户要求。
- `request_payloads`：`prompt_refs_v2` 仅关联本次新增或待确认输入，不再重复关联整段历史。不会保存完整 request body。
- `prompt_conversation_state`：只保存文本哈希、出现次数和增量请求标记哈希，用于跨请求比较；不保存额外的文本副本。首次接入时的历史保留为会话基线，不冒充本次新增输入。
- `request_history`：按请求关联统计与上述记录的查询视图，不复制一份 Token 数据。未收到 CPA 用量时，统计字段为空。

支持 Chat/Claude 的 user 消息、Responses 的 user 消息或字符串 input、Gemini 的 user 文本、传统字符串 prompt。在已有 session/conversation 内比较清理后的消息历史，并检查末尾是否为用户消息；工具续接、历史重放和重试不再产生新输入。完整历史中同一句话的后续独立出现仍按轮次记录。Responses 增量请求额外比较 previous_response_id 的哈希标记。没有会话标识、首次看到多轮历史或旧数据缺少边界时，只标为待确认。Token 统计始终使用 CPA 的完整实际用量，不按保留下来的用户文本重新估算。

请求事件中的日志预览/下载显示“请求信息”和“输入判断”；仅有相关文本时显示“本次新增输入（推断）”或“待确认输入”。无新增输入的请求不显示提示词栏目，继续要求管理员登录。API 中的 user 角色不等于输入框事件：未知客户端注入、截断历史、缺少会话 ID 等场景无法保证准确识别；若要严格记录用户实际输入，需要客户端提供明确的输入事件标识。未曾记录的历史提示词无法补回。初版短暂保存的完整 request body 已全部转换为提示词引用，旧 CPA 错误日志提取用户文本后清理；数据库已整理并截断旧 WAL。

## 维护与构建

- 自动数据库备份已关闭，历史回退文件已按用户要求删除。
- 修改源码后运行开发目录的 `build_release.sh`；发布包 `release/` 可重新生成，平时无需保留。
- GitHub 仓库的 `integration-patches/build.sh` 可从固定上游版本及补丁重建两个 Go 程序和前端。
- `migrate_shared_store.py` 用于离线合并旧统计库与额度库；要求先停止两个写入进程，输出路径不能已存在。脚本逐表核对所有行和数据库完整性。
- `deploy/` 保存当前服务及主机配置。生产环境、认证文件、数据库、程序和本地接入说明不提交 Git。

迁移验证保留了当时的 141,973 条用量明细、140,042 条账务记录和 26 个用户。清理的 122,502 条旧记录是已结算的额度预占记录。95 项 Python 回归、Go 请求记录/权限/日志测试、前端类型检查和构建、隔离完整链路及生产页面检查通过；用户额度和账号调度配置未重置。

旧版提示词记录会按会话时间顺序重新分类，已知自动内容会移除；缺少原始消息边界的记录不会追认为已确认输入。用量、账务和路由表不参与这次迁移。

本次输入识别修正通过 101 项 Python 回归、Go 请求日志测试、前端类型检查/ESLint/构建，以及隔离和生产浏览器检查。隔离链路验证 4 次请求中仅 2 次为新增输入；工具步骤不显示提示词，预览与下载一致，完整 Token 用量继续记录。生产迁移逐表校验用量、账务、策略、路由和请求元数据不变。

# Keeper 与 CPA Quota Gateway 整合版

开发日期：2026-10-23。开发根目录：`/home/ubuntu/cpa-integration-dev`。

本版本保留 Gateway 原有队列、计费和故障恢复改动；Keeper 基于 v1.14.4，CPA 基于 v8.0.24。更新前基线已提交为 `2408701`，整合版位于 `keeper-integration` 分支。2026-10-23 已按用户要求切换生产服务，沿用现有配置与数据库；账号调度默认关闭。

## 已实现

- Gateway 提供统一管理入口，使用 Keeper 原有管理员登录与普通 API Key 只读登录。统计与历史事件继续由 Keeper 服务处理，额度和队列由 Gateway 处理。
- 社区排名、返回 CPA、检查更新、设置中的会话管理从统一入口移除。整合模式下 Keeper 不启动社区排名后台任务，也不注册上述管理接口。
- 本地排名放入分析页，保留原本的时间、指标、头像和别名编辑能力。
- API Key 卡片内加入周期额度、已用与剩余额度、周期、RPM、个人并发、启停、超级用户、优先认证账号和额度重置。保留 Key 显示/复制与别名编辑，提供全部启停、批量重置、同步和用量排序。
- 请求事件页增加执行中、总排队、个人排队三个池，每个任务有稳定 ID；小球标注用户名字，同一用户同色，悬停可查看模型、目标账号和时间。每秒读取实际队列，失败时保留最后状态并标记连接中断；切走页面会取消请求。个人池支持按用户筛选。
- 总并发移入认证文件页；新增单账号并发、溢出阈值、可选模型范围，以及默认 12 小时的 session 保持时间。

## 调度规则

1. 原个人队列 → 总队列、超级用户优先、同级 FIFO、空闲个人名额借用、额度预占/结算与重试策略继续保留。
2. 账号调度默认关闭。启用前 Gateway 验证 CPA 支持可信账号指定，并确认配置的认证文件真实存在；检查失败时不能开启。
3. 新会话先选择用户的优先账号。该账号执行名额已满且已有排队数达到阈值（默认 3）时，才选择没有排队且有空位的其他账号。阈值 0 表示账号满载时即可分流。
4. 已有有效 session 绑定不会因账号拥堵而切换。保持时间从最近一次请求计算，默认 12 小时，可配置 0.1–720 小时。修改保持时间立即适用于已有绑定。账号停用、失效或不再支持请求模型时，可重新选择可用账号。
5. session 绑定按用户 API Key 哈希隔离，存储在 Gateway SQLite 的 `routing_sessions` 表，重启保留；原始 session 与 API Key 不写入该表。支持常见 Codex/Claude session 头、`prompt_cache_key`、Responses conversation ID、`session_id`/`sessionId` 和 Claude `metadata.user_id` 中的 session。客户端未提供可识别 session 时，每次请求按账号偏好独立选择。
6. 单账号满载不会占住可供其他账号使用的总并发名额；已进入队列的请求若账号被禁用或移除，会清理队列并返回明确错误。已有执行中的请求保持原账号直至结束或取消。
7. 配置中的模型列表为空时由 CPA 判断该账号能否执行；混用不同提供商账号时应填写各自支持的模型名，避免分配到不支持模型的账号。当前新增功能在原有 HTTP/SSE 请求路径上验证。

## 为什么附带 CPA 补丁

v8.0.24 的普通 HTTP API 没有向外开放可信认证文件指定头。只添加 Gateway 设置无法保证实际使用的账号，因此附带最小 CPA 补丁，将 Gateway 选择的真实 auth ID 接入 CPA 现有 pinned-auth 执行链。

- 两端配置相同的私有 `QUOTA_ROUTING_TOKEN`。
- Gateway 覆盖并剥离客户端伪造的内部路由头；CPA 校验内部凭据，在请求日志复制头部之前删除凭据。
- 固定账号沿用 CPA 本身的流式、模型和重试处理，不修改原始请求体。
- CPA 未启用补丁或密钥不匹配时，Gateway 明确报错，不假装绑定成功。

## 独立预览

生产切换后，服务器上的合成预览已停止。以下命令仅用于再次启动隔离测试环境。

```bash
python3 /home/ubuntu/cpa-integration-dev/preview_control.py status
python3 /home/ubuntu/cpa-integration-dev/preview_control.py start
python3 /home/ubuntu/cpa-integration-dev/preview_control.py stop
```

入口：`http://127.0.0.1:28320/`。预览登录密码在 `/home/ubuntu/cpa-integration-dev/preview/credentials.json` 的 `password` 字段，文件权限 0600。这里的账号、API Key、请求和上游全部为合成测试数据，不会消耗真实账号额度。

如从自己的电脑访问这台服务器，可建立隧道（将主机名换成平常 SSH 使用的地址）：

```bash
ssh -N -L 28320:127.0.0.1:28320 ubuntu@服务器地址
```

然后打开浏览器的 `http://127.0.0.1:28320/`。预览使用 28317（模拟 CPA）、28318（独立 Keeper）、28319（模拟数据流）、28320（独立 Gateway），均只监听回环地址。数据在开发目录的 `preview/` 下。

## 验证记录

- Gateway：`python3 -m unittest -q test_gateway test_failover test_recovery test_routing`；现有 71 项加新增 17 项，共 88 项通过。
- 前端：TypeScript 类型检查、改动文件 ESLint、Vite 构建通过；队列组件、API Key 卡片、本地排名组件与 hook 共 45 项测试通过。
- Keeper：`go test ./internal/api ./internal/app/test` 通过，包含整合模式不启动社区排名且保留本地排名的测试。
- CPA：可信路由鉴权、私有头清除、真实执行 metadata 传递、已有流式 pinned-auth 回归通过；服务端构建通过；实际编译的 CPA 在空认证目录下通过能力握手、保留 API Key 鉴权、伪造路由拒绝及日志不泄露路由密钥的接口测试。
- 浏览器：真实 Keeper 登录、三池实时轮询、账号配置保存、用户额度保存、本地排名、390px 移动布局通过；没有页面脚本错误或失败接口。
- 测试记录位于开发根目录的 `gateway-tests.log`、`frontend-tests.log`、`keeper-tests.log`、`cpa-tests.log`、`cpa-contract.log`、`browser-tests.log`；截图位于 `preview/`。

开发验证使用本地隔离环境和模拟上游。生产切换后额外验证了真实管理员登录、26 个现有用户、三池展示、认证文件设置、本地排名和移动布局；公网模型列表与用户额度查询通过。现有额度、周期、并发策略和环境文件保持一致，真实用量继续入账。

## 构建与后续切换

`/home/ubuntu/cpa-integration-dev/build_release.sh` 可重新构建；`release/` 中是待切换程序、Gateway 代码、前端静态文件、许可证和 SHA256 清单，不含生产密钥、数据库或认证文件。

从 GitHub 重建时运行 `bash integration-patches/build.sh`。脚本获取两个固定上游提交并应用仓库内补丁，在 `.integration-build/release` 输出二进制和前端；需要 Node.js 22.22+、支持自动工具链下载的 Go、Git。前端源代码、后端修改及许可证都包含在 `integration-patches/` 中，仓库内 `console/` 为当前构建产物。脚本不安装或重启生产服务。

其他环境部署时，应以最新生产数据进行数据库快照和迁移，保留 Keeper/Gateway 配置及全部计费历史，不使用预览数据替代生产数据：

1. 安装附带补丁的 CPA，并为 CPA/Gateway 配置同一个新生成的 `QUOTA_ROUTING_TOKEN`。原业务配置与认证文件继续使用原值。
2. 将整合版 Keeper 作为内部统计服务运行，沿用生产数据库、登录配置及 CPA 连接；设置 `QUOTA_GATEWAY_INTEGRATION=1`，关闭社区后台任务和已移除的接口。
3. 将新 Gateway 的 `QUOTA_DB` 指向迁移后的生产额度库，`KEEPER_URL`/`KEEPER_DB` 指向对应 Keeper。设 `UNIFIED_CONSOLE=1`，`CONSOLE_DIST` 指向发布包的 `gateway/console`。除这些新增项外保留当前生产环境配置，尤其是已有重试与计费参数。
4. 维持单 Gateway 调度实例，在旧实例请求排空后切换，避免两个进程各自使用一套内存并发计数。先保持账号调度关闭，验证原有转发、历史统计和计费，再在认证文件页导入实际账号并开启调度。
5. 管理员可通过 SSH 隧道访问统一 Gateway 的根路径。若日后需要公开管理入口，再配置独立管理域名的代理；当前 `/v1/` 的业务入口无需为 UI 调整。

回退时恢复原程序和原环境设置。新增数据为 `settings.account_routing` 及 `routing_sessions`，原版 Gateway 不读取这些字段；计费表结构不变。回退应保留最新额度数据库，避免丢失切换后产生的真实用量。


## 2026-10-23 用户表格与认证账号别名更新

“API Key 与用户限额”采用一用户一行，共十列：可编辑别名、Key 末六位（点击复制完整值）、已用/剩余进度与重置时间、周期额度、计费周期（自然月/自定义天数）、个人并发、优先认证账号、启用 Key、超级用户、重置/保存。别名、已用、剩余和额度支持点击表头升降序排序，排序保留未保存的输入。RPM 维持已有策略，不在此表中编辑。

认证文件页直接列出已同步文件，提供单独的别名保存按钮；复用 Keeper 原有认证身份别名，与下方认证文件统计和优先账号选项使用同一份数据。未参与调度的认证文件也可改别名，勾选参与调度后保存账号调度即可加入候选列表。

此更新仅替换 Gateway 静态管理页面，不重启 CPA、Keeper 或 Gateway，也不修改现有账号路由、用户额度或计费策略。20 项相关前端测试、类型检查、ESLint 和构建通过；隔离浏览器验收覆盖完整保存/重置/复制/排序/别名联动，生产浏览器另做只读验收。旧前端在服务器 backups/user-quota-table-* 中保留。

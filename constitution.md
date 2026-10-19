# chain-indexer-service 项目开发宪法

Version: 1.2.0 · Ratified: 2026-10-17 · Last amended: 2026-10-20

本文件约束本仓库的架构判断、实现取舍与质量底线。`AGENTS.md` 是项目上下文入口；非简单任务开始前应主动读取本文和相关源码。本文高于普通协作建议，但不高于系统/平台规则，也不高于用户在当前任务中的明确指令。

---

## 第一条：分层边界原则 (Layer Boundaries First)

核心：链索引服务按命令入口、配置、领域、仓储、DAO 和数据库能力分层；不能因为未来需要 worker 就提前制造反向依赖。

- **1.1** `cmd/chain-indexer` 负责服务进程、信号和资源生命周期；`cmd/migrate` 独立负责迁移命令。两者不互相调用，也不在服务启动时自动运行 migration。
- **1.2** `cmd/chain-indexer/ioc` 按具体依赖提供 `InitX`；`main.go` 逐个装配和关闭。业务与领域代码不得引用命令包、IoC 或测试装配。
- **1.3** `internal/domain` 表达链数据值对象和规则，不带 ORM、YAML 或供应商 SDK 细节。`internal/repository` 的接口和返回值使用 domain 类型；`internal/repository/postgre` 以 entity/DAO 处理 GORM 与 SQL。
- **1.4** entity → domain 转换在 Repository 边界完成；不得把 GORM entity 或 `*gorm.DB` 透传给未来 Service/Handler。DAO 只负责持久化读写和驱动错误，不能承载链业务编排。
- **1.5** `internal/pkg/postgre` 只承载服务与迁移入口真实复用的 PostgreSQL 连接、schema gate 与 migration runner。新内容进入 `internal/pkg` 前，至少证明两个真实生产调用方和稳定职责；不是任意代码的兜底目录。
- **1.6** `pkg/evm/abi` 与 `pkg/xsql` 是工具包。ABI 解码函数存在不代表 RPC 监听、解析 worker 或链同步已经装配。

---

## 第二条：配置与启动契约 (Explicit Configuration and Lifecycle)

核心：配置必须显式、可校验、可脱敏；“进程启动成功”不能夸大成“索引正在运行”。

- **2.1** 服务配置与迁移配置分别加载。迁移命令只依赖数据库配置，不强制要求七链或 RPC 变量；服务配置必须满足完整链约束。
- **2.2** 服务配置必须包含七条内置链及固定 chainId、confirmBlocks。每条启用链显式给出非负 `startBlock`；`0` 是明确选择创世块，不是缺省。至少启用一条链。
- **2.3** HTTP RPC URL 仅接受 `http`/`https`，WebSocket RPC URL 仅接受 `ws`/`wss`。`${VAR}` 仅在配置实际引用时才需要；缺失或无效变量使加载失败。不得把真实 DSN、API key 写入仓库或日志。
- **2.4** 服务启动先连接并 ping PostgreSQL，再检查所需 schema migration 身份；失败时关闭已打开连接并返回错误。收到 SIGINT/SIGTERM 后有序关闭连接池。
- **2.5** 当前 P1 启动后的“ready”仅指配置、数据库连接与所需 schema 检查通过。没有 HTTP listener、RPC 连接、区块同步或事件解析时，日志、文档和测试不得声称这些能力已运行。
- **2.6** 必需依赖在构造/装配处保证有效；业务方法不得用 `if repo == nil { return nil, nil }` 之类的防御性 nil guard 吞掉装配错误。未来可选依赖在装配边界明确禁用或注入满足契约的实现，不能让业务层散落 nil 判断。

---

## 第三条：迁移与 schema 权威原则 (Migrations Are Authoritative)

核心：`script/postgre` 的版本化 SQL 是 schema 唯一来源，GORM entity 只是映射。

- **3.1** 每个 migration 使用六位版本号、同名 `.up.sql`/`.down.sql` 成对提交；文件名、内容、版本和名称必须与已应用历史一致。禁止在进程启动时 `AutoMigrate`。
- **3.2** `cmd/migrate up|down|status` 是唯一迁移入口。Up/Down 在专用连接上使用 PostgreSQL advisory lock 和事务，迁移 SQL 与 `schema_migrations` 历史记录保持原子性。
- **3.3** 已应用版本的顺序或名称漂移时应显式报错。现有 runner 不保存 SQL checksum，因此已发布的 migration 内容必须保持不变；新的 schema 变更添加新版本，不直接改写旧 SQL。
- **3.4** Down 必须说明数据影响，并在 E2E 中验证回滚后仍应保留的数据。不可因本地测试方便而在共享或生产库执行破坏性重置。
- **3.5** 服务 schema gate 校验其所需迁移身份；迁移命令可能管理更新版本。不要把“P1 检查 version 1”误写成“数据库不得有 version 2”。

---

## 第四条：链数据身份与游标原则 (Chain Data Identity and Cursors)

核心：链上数据不能只凭高度或交易哈希识别；重组、分页与业务消费必须保留完整身份。

- **4.1** 区块以 `(chain_id, block_hash)`、原始事件以 `(chain_id, block_hash, log_index)` 等 schema 自然键区分；同高度可能因重组出现不同哈希。新增查询或唯一约束时先检查 `script/postgre`，不要用高度代替不可变身份。
- **4.2** canonical/orphaned、chain status、reorg ID、chain version 等字段是数据契约。未来实现重组处理时必须考虑旧数据失效、游标回退策略和重复写入；当前 P1 没有运行重组流程，不得声称已有该能力。
- **4.3** 原始事件列表按自增 `id` 严格升序、使用 `id > cursor` 分页；不能假设 ID 连续，也不能改成 offset 分页后仍宣称具备稳定消费语义。
- **4.4** 业务游标以 `(biz, chain_family)` 隔离，持久化推进只前进不后退。新增消费者时需明确幂等、提交时机和失败重试；不能在处理失败前推进游标。
- **4.5** EVM 数值采用无符号 256 位语义，十进制解析拒绝空值、负数、非十进制和溢出。数据库 `numeric(78,0)`、JSONB 与 Go 值对象转换必须保留精度和原始数据，不经 float64 绕行。

---

## 第五条：测试先行铁律 (Test-First Imperative) — 不可协商

核心：所有新功能和 Bug 修复从可观察行为的失败测试开始，遵循 Red → Green → Refactor。

- **5.1 Red：**先写最小目标测试，运行并确认它因待实现行为缺失而失败。新 API 的编译失败可算 Red；网络、Docker、权限、测试自身错误不算。
- **5.2 Green：**写最小实现让同一测试通过；不得删除、跳过或弱化断言制造通过。需求变更时先修正测试契约并重新取得有效 Red。
- **5.3 Refactor：**保持测试通过，整理重复与命名。新增分支与错误路径必须有断言；Bug 修复要留下会复现旧故障的回归测试。
- **5.4 表格驱动：**单元与集成测试存在并列输入时优先 `tests := []struct{...}{...}` 加 `t.Run`；复杂迁移、生命周期和进程测试可使用更清楚的专门用例。
- **5.5 真实依赖：**配置、值对象和转换在普通测试中验证；真实 PostgreSQL/RPC/HTTP/WebSocket 等外部组件测试只放 `internal/test/integration`，使用 `//go:build e2e`。真实依赖有副作用或不可稳定提供时，可以使用接口替身，但不以 mock 调用次数替代业务结果。
- **5.6 数据断言：**测试应断言链 ID、自然键、游标、事件字段、迁移历史与错误语义；只断言退出码、状态码或“没有报错”不完整。迁移测试要覆盖 Up、Down、重复执行、数据保留和 schema 差异。
- **5.7 隔离：**`make e2e` 只使用其专用 Compose 项目和测试数据库，退出时清理容器与 volume；不得指向共享/生产数据库，不依赖测试顺序。不要为了“清空环境”操作无关资源。
- **5.8 证据：**交付写明 Red 失败原因、Green 命令与结果、未运行检查和风险。纯文档/格式变动不人为制造 Red；用户明确要求暂停验证时停止，不把未验证行为说成已通过。

---

## 第六条：显式错误与可观测性原则 (Explicit Errors and Observability)

核心：失败可定位且不泄露凭据；context 和日志反映真实运行状态。

- **6.1** 用 `fmt.Errorf("...: %w", err)` 保留需要向上判断的内部原因，用 `errors.Is` / `errors.As` 分类；不要比较错误字符串。对外/启动错误不得打印 DSN、RPC token 或供应商密钥。
- **6.2** 数据库查询、迁移、未来 RPC 与 worker 路径传递 `context.Context`，尊重取消与超时。资源获取成功后在失败路径关闭；迁移锁与事务在失败时释放或回滚。
- **6.3** 结构化日志使用 `log/slog`，记录命令、迁移版本和必要链身份，不记录完整连接串、私钥、API key 或原始供应商响应。P1 不具备 trace/exporter，不得把普通日志冒充链路追踪。
- **6.4** 用户/操作者可见文案应明确描述实际检查结果。schema gate 通过不代表 RPC、worker、provider 或重组流程健康。

---

## 第七条：最小必要变更原则 (Minimal Necessary Change)

核心：围绕当前需求和 P1 实现扩展，不把参考仓库的能力一次性移植进来。

- **7.1** 不为单一任务新增通用 CLI dispatcher、容器对象、空的 Provider/Worker、重试框架、HTTP/gRPC 服务或缓存层，除非需求和测试证明必要。
- **7.2** 修改接口时同步检查实现、转换、命令装配和测试装配；修改字段时从 SQL 权威契约检查到 GORM entity、domain 与查询测试。
- **7.3** 不修改与任务无关的文件，不回滚用户改动，不清理不相关未跟踪文件；脏工作区先看 `git status`，只处理本任务范围。
- **7.4** 参考 `ebook`、`wallet-aggregate-go`、`notification-platform`、`boilerplate-go-starter-service` 的分层和验证方式，但不复制其支付/钱包/通知业务、pgx 共存策略、FlushDB 做法或私有依赖。
- **7.5** 小字段和小结构体不得使用指针，包括 struct 字段、函数或方法参数、返回值以及构造器传参；应直接使用值语义。只有包含大量方法的实现对象，或持有大量依赖并负责复杂生命周期的对象（例如 app、server）才允许使用指针。不得借本条批量改写现有指针；未来触及既有指针时，不得破坏数据库 `NULL` 语义或第三方 API 契约。若指针禁用约束与这些既有契约发生冲突，必须明确说明冲突并交由用户决定，不得自行扩大、归纳或创建例外。

## 第八条：语义常量原则 (Semantic Constants)

核心：有限集合的业务语义必须由其 owner 包定义类型化常量，避免裸字面量扩散。

- **8.1** 状态机、业务类型、来源、动作和模式等有限集合使用命名类型与 typed constants；每个常量附清晰中文注释，同文不同义必须分类型。
- **8.2** 跨层链数据语义优先归 `internal/domain`，DAO 专属语义归 `internal/repository/postgre`，迁移方向归 `internal/pkg/postgre`，迁移命令 action 归 `cmd/migrate`；禁止新增全局 constants/consts 包。持久化字段继续使用稳定 wire 字符串，边界处用生产常量转换，避免改变既有 schema 与外部数据契约。
- **8.3** RPC/ABI 协议值、SQL 标识/SQLSTATE/constraint、配置键、日志与错误文案不因形式相似而常量化；历史 migration 内容不修改。协议 raw 值和用于验证非法输入的 sentinel 可以保留裸字面量。
- **8.4** 常量重构不得扩大测试边界：普通测试继续离线，真实 PostgreSQL/RPC/HTTP/WebSocket 只在 e2e 集成测试；支持值应复用生产常量，非法 sentinel/raw wire 输入可直接使用裸字面量以明确测试异常语义。
- **8.5** 命名常量类型应按实际底层类型提供明确的边界转换方法：`string` 使用 `ToString()`，`int` 使用 `ToInt()`，`int32` 使用 `ToInt32()`，`int64` 使用 `ToInt64()`，其他底层类型遵循同样规则。同类型内部比较或 `switch` 无需转换；只有进入基础类型字段、ORM、序列化、日志等边界时才转换，禁止用显式强制转换绕过约定。相同 wire 值在 domain 与 DAO 中也分别由各自 owner 定义，不跨层共享。

---

## 第九条：对外 README 原则 (Public README Contract)

核心：根目录 `README.md` 是面向外部读者的中英双语项目介绍，不是安装、运维、测试或内部验收使用手册。

- **9.1** README 的中文与英文必须表达相同的项目定位、当前能力、架构边界和限制；不得只翻译标题、遗漏一侧的关键约束，或让两种语言描述不同阶段的系统。
- **9.2** README 聚焦项目价值、架构、核心能力、模块职责、技术栈、数据一致性模型和明确非目标。启动命令、migration 操作、配置步骤、Docker 生命周期、内部 Task 编号及验收流水账应放入 `AGENTS.md` 或专门工程文档，不在 README 中展开成使用手册。
- **9.3** README 必须包含与当前生产装配一致的架构图。默认采用普通 Markdown 代码块内的 ASCII 图，使 GitHub、本地 IDE、终端和不支持 Mermaid 的阅读器都能显示；如改用 Mermaid，必须确认主要托管平台能够渲染，并同时提供可理解的文本说明。
- **9.4** 架构图和能力描述只能展示当前真实存在且已接入生产入口的组件与数据流。测试 fixture、mock、仅有 schema/entity 的预留字段或未来规划不得画成已运行能力；未上线能力必须明确标为当前范围外。
- **9.5** 修改服务入口、IoC、provider、listener、scanner、worker、repository/DAO、数据库边界或 migration 生命周期时，必须同步检查 README 架构图和双语说明。README 与真实实现冲突时，以代码和版本化 SQL 为事实，并在同一变更中修正文档。
- **9.6** README 不得包含 API key、DSN、完整私有 endpoint、内部环境路径、供应商原始响应或其他凭据；也不得把测试通过、源码存在或 schema 预留夸大为生产部署、链上 finality 或分布式强一致保证。

---

## 治理 (Governance)

- 本宪法指导 Agent 与工程师在本仓库的默认判断；`AGENTS.md` 提供具体操作入口。若两者普通建议冲突，以本文件的原则为准。
- 用户当前明确指令优先于普通项目约定，系统/平台安全规则优先于用户指令及本文。不能仅凭文档要求推断已获得额外权限。
- 修改本文件属于治理变更，应说明原因、受影响的目录和验证方式，并同步核对 `AGENTS.md`、README 和真实实现。

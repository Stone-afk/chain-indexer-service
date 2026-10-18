# chain-indexer-service 项目上下文入口

本文是维护本仓库的 Agent 和工程师的操作入口。非简单任务开始前，主动读取下面的上下文文件；路径列表本身不会自动展开。架构与质量底线见 `constitution.md`。用户当前的明确要求优先于普通仓库约定，系统和平台规则仍优先。

## 核心上下文与项目阶段

- `constitution.md`：分层、迁移、链数据、测试先行与协作原则。
- `README.md`：当前 P1 能力、运行方式和明确未实现的范围。
- `Makefile`、`scripts/test_docker_compose.yml`：唯一标准 E2E 入口和专用 PostgreSQL 环境。
- `config.example.yaml`、`config/config.local.yaml`、`internal/config/`：配置来源、七链约束、测试配置。
- `script/postgre/`、`internal/pkg/postgre/`：成对 SQL、迁移执行、连接生命周期和 schema gate。
- 目标包的接口、实现与相邻测试；跨层修改还要读 `cmd/chain-indexer/`、`cmd/migrate/` 或 `internal/test/ioc/` 中对应的装配代码。

本仓库当前是多 链索引服务的 **P1 基础阶段**：已有严格配置、PostgreSQL migration、GORM 映射、原始事件查询/业务游标仓储、服务进程生命周期和 ABI 解码工具；**尚未运行 RPC 监听、区块同步、事件解析、重组处理或索引 worker**。表结构和领域类型为后续阶段预留字段，不代表相关运行能力已经上线。不要根据目录名或 schema 推断功能已经实现。

## 技术栈与标准命令

| 范围 | 当前选择 |
| --- | --- |
| Go | `go.mod` 声明 Go 1.22；使用标准库 `context`、`log/slog` |
| 链数据 | go-ethereum ABI 工具；当前不连真实 RPC |
| 数据库 | PostgreSQL、GORM；E2E Compose 使用 PostgreSQL 14.14 |
| 配置 | `gopkg.in/yaml.v3`，严格解析与显式环境变量展开 |
| 装配 | 命令入口逐依赖调用 `InitX`，没有 Wire、容器对象或 `init` 注册 |

```sh
go test ./...                  # 不访问真实数据库或 RPC
go vet ./...
go build ./...
make e2e                      # 专用 Docker PostgreSQL，race + shuffle，退出时清理容器与 volume
go run ./cmd/migrate up --config ./config.yaml
go run ./cmd/migrate status --config ./config.yaml
go run ./cmd/migrate down --config ./config.yaml
go run ./cmd/chain-indexer --config ./config.yaml
```

`make e2e` 会创建并清理名为 `chain-indexer-service-e2e` 的专用 Compose 项目，不需要把测试 DSN 指向已有数据库。先确认 Docker 可用，再运行 E2E。没有 `make lint`、`make test` 或通用 `serve` 子命令；不要在文档或交付说明中虚构这些入口。

## 分层与目录责任

- `cmd/chain-indexer/main.go`：服务入口；读取完整服务配置，逐个调用 `cmd/chain-indexer/ioc` 的 `InitX`，持有资源至 SIGINT/SIGTERM 并显式关闭。当前仅实际初始化数据库。
- `cmd/migrate/`：独立可执行入口；解析 `up|down|status`、只读取迁移所需数据库配置、运行 migration 并关闭连接。不把迁移塞进服务启动路径。
- `internal/config/`：严格 YAML、环境变量展开和七条内置链校验。服务配置与迁移配置有不同边界；不要让迁移依赖完整链/RPC 配置。
- `internal/domain/`：纯 Go 链数据模型及值对象。`Uint256` 保持无符号 256 位与十进制解析语义，不引入 GORM/配置/网络依赖。
- `internal/repository/`：面向领域的仓储接口与 entity → domain 转换，隐藏 GORM 和 SQL；业务游标参数在这里校验。
- `internal/repository/postgre/`：GORM entity、DAO 接口与 SQL 实现。查询带 `context.Context`；保持链数据字段、可空值、自然键与 schema 一致。
- `internal/pkg/postgre/`：服务与迁移两个生产入口复用的 PostgreSQL 连接、schema gate 和 migration runner；不要放单一业务规则。
- `pkg/evm/abi/`：ABI 解码与类型；`pkg/xsql/`：SQL JSON 字段适配。它们是现有工具包，不表示 RPC/provider 或索引 worker 已装配。
- `internal/test/ioc/`：按配置、数据库、迁移职责构造测试依赖；不引用生产 `cmd/.../ioc`，不持有全局万能测试对象。
- `internal/test/integration/`：所有真实 PostgreSQL、RPC、HTTP、WebSocket 或其他外部组件测试的归属地，通过 `e2e` build tag 与 `make e2e` 运行。
- `script/postgre/`：schema 的唯一权威；六位版本号的 `.up.sql`/`.down.sql` 成对出现。GORM entity 是映射，不是迁移来源。

依赖方向保持 `cmd → config / IoC → repository → postgre DAO → PostgreSQL`，领域模型不反向依赖命令或数据库。`internal/pkg` 不是兜底目录：新公共能力必须至少有两个真实生产调用方、职责稳定，并证明不属于现有单一模块。

## 修改前后的检查清单

| 改动 | 先看 | 必须同步检查 |
| --- | --- | --- |
| 配置或链列表 | `internal/config`、两份示例配置 | 七链名称/chainId/confirmBlocks、显式 `startBlock`、URL scheme、变量只在被引用时才必需；服务/迁移加载边界 |
| 服务启动或 IoC | `cmd/chain-indexer/main.go`、`cmd/chain-indexer/ioc` | 失败时关闭已打开连接、schema gate、信号退出、不要声称未启动的 worker/listener |
| migration | `script/postgre`、`internal/pkg/postgre/migrations.go` | 成对文件、版本与名称、已应用历史、advisory lock、事务 Up/Down、回滚和现有数据保留 |
| entity/DAO | SQL、`internal/repository/postgre`、转换测试 | 列类型/NULL、主键与唯一键、`numeric(78,0)`、JSONB、context、分页顺序 |
| 原始事件/业务游标 | `internal/repository`、DAO、E2E | 原始事件按自增 ID 严格递增分页；游标按 `(biz, chain_family)` 隔离且只前进 |
| ABI/Uint256 | `pkg/evm/abi`、`internal/domain/uint256.go` | 超界、无效十进制、事件 topic 与数据解码；不提前加入 RPC 运行链路 |
| 新的共享包 | 现有调用方和模块边界 | 至少两个真实生产调用方、用途和生命周期；不把未来可能需要的抽象先建空壳 |

## 测试先行与验证证据

新功能或 Bug 修复遵守 `constitution.md` 的 Red → Green → Refactor：先写能因目标行为缺失而失败的测试，再写最小实现，最后整理。单元和集成测试有并列输入时优先表驱动；涉及状态、迁移和生命周期可用更清晰的专门用例。不要通过改弱断言、跳过测试或环境失败伪造 Red/Green。

普通测试必须离线可运行；真实组件测试只放 `internal/test/integration` 并使用 `//go:build e2e`。E2E 使用专用 Compose PostgreSQL，成功或失败都清理项目容器和 volume；不得重置共享/生产数据库。测试应断言链 ID、区块/事件身份、游标值、迁移历史、保留的数据和错误语义，而不只看命令退出码。无法运行的检查需说明具体原因和剩余范围。

## 协作与交付

- 保留用户已有修改；当前工作树可能有未提交改动，先看 `git status`，不要重置、清理或覆盖无关文件。
- 日志和错误不暴露数据库 DSN、API key、RPC 凭据或原始供应商响应；必要时保留内部错误原因，但对外信息应脱敏。
- 文档默认中文；用户要求中英双语时双语交付。不要把参考仓库的支付、钱包、通知、pgx 共存或 FlushDB 规则机械移植到本项目。
- Git 提交、推送、PR 以用户当前授权为准；只纳入本任务文件，提交信息优先用 `<type>(<scope>): <subject>`。
- 收尾写明实际修改、执行的验证与未验证范围。修改 `constitution.md` 时同步核对本文件、README 和真实实现。

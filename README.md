# chain-indexer-service

多 EVM 链索引服务。当前 P1 提供严格配置、PostgreSQL migration、GORM 数据映射和服务生命周期基础；尚不执行 RPC 监听、区块同步或事件解析。

## 前置条件

- Go 1.22 或更高版本（当前验证工具链为 Go 1.25.14）
- Docker 与 Docker Compose（本地 E2E 使用 PostgreSQL 14.14）

生产或部署配置可从 `config.example.yaml` 复制。每条启用链必须显式填写非负 `startBlock`；`startBlock: 0` 表示明确从创世块开始，不是缺省值。HTTP RPC 支持 `http`、`https`，WebSocket RPC 支持 `ws`、`wss`，可使用自建节点或任意供应商。模板中的 Infura 地址只是示例；只有 YAML 实际引用 `${INFURA_API_KEY}` 时才需要该环境变量。

例如运行模板配置前设置：

```bash
export POSTGRES_DSN='host=127.0.0.1 port=5432 user=postgres dbname=chain_indexer sslmode=disable'
export INFURA_API_KEY='<key>'
```

## 数据库 migration

Migration 是独立命令，不属于 `chain-indexer` 的子命令：

```bash
go run ./cmd/migrate up --config ./config.yaml
go run ./cmd/migrate status --config ./config.yaml
go run ./cmd/migrate down --config ./config.yaml
```

默认从 `script/postgre` 读取成对 SQL；可用 `--migrations-dir` 显式指定目录。服务启动不会自动执行 migration。

`internal/pkg/postgre` 负责 PostgreSQL 连接生命周期、migration runner 和 schema gate；`internal/repository/postgre` 仅保留数据库模型与映射。P1 不提前定义通用 repository 接口或业务 CRUD；`script/postgre` 中的 SQL 是 schema 的唯一权威，不使用 `AutoMigrate`。

`internal/pkg` 仅按需承载跨业务模块复用、职责稳定且内聚的项目内部公共能力。至少两个真实生产调用方是必要条件，还必须证明能力不属于 `config`、`domain` 或 `repository/postgre` 等单一模块；它不是通用兜底目录。`internal/pkg/postgre` 同时由 service 与 migrate 两个生产入口复用，因此符合这一边界。

## 启动服务

```bash
go run ./cmd/chain-indexer --config ./config.yaml
```

`chain-indexer` 没有 `serve` 或 `migrate` 子命令。P1 会校验完整七链配置、连接并 ping PostgreSQL、检查 schema version 1，然后持有真实数据库连接池直至收到 SIGINT/SIGTERM；它不声称 listener、worker 或链索引已经运行。

服务由 `cmd/chain-indexer/main.go` 直接启动，并继续采用逐依赖的显式 IoC：`main.go` 按具体依赖调用 `cmd/chain-indexer/ioc` 中对应的 `InitX` 函数并显式管理生命周期；独立的 `cmd/migrate` 入口保持不变。P1 目前只有 `InitDB`，不存在汇总所有依赖的 `Dependencies`、`Application` 或 `Container`。

## 测试

普通测试不访问 PostgreSQL、RPC 或其他真实外部资源：

```bash
go test ./...
go vet ./...
go build ./...
```

本地真实环境固定使用 `config/config.local.yaml`，由 `make e2e` 创建专用 Docker PostgreSQL、执行 `internal/test/integration` 下全部真实集成测试，并在成功或失败后删除容器与 volume：

```bash
make e2e
```

`internal/test/ioc` 按 config、PostgreSQL 等具体职责装配测试依赖；它不复用生产 command IoC，也不保存全局连接或万能测试对象。`internal/pkg` 的纯单元测试可与包同目录；任何 PostgreSQL、RPC、HTTP、WebSocket 或其他真实组件测试只能位于 `internal/test/integration`，由 `make e2e` 执行。

无需设置测试 DSN，也不要把本地 E2E 指向已有数据库。Docker 必须可从命令行访问；环境定义位于 `scripts/test_docker_compose.yml`。

## P1 边界

项目提供两个独立可执行入口：`cmd/chain-indexer` 直接启动服务，`cmd/migrate` 单独管理 migration。P1 不包含通用 CLI dispatcher、`serve` 子命令、listener/worker/provider 空壳、业务 repository CRUD、重试框架、RPC 主网可用性测试、区块同步、事件解析、重组处理或 internal trace。

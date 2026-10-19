# chain-indexer-service

面向多条 EVM 网络的链上数据索引服务，提供确认区块扫描、链重组恢复、标准代币转账解析，以及基于 PostgreSQL 的幂等持久化。

A multi-chain EVM indexing service for confirmed-block scanning, chain-reorganization recovery, standard token-transfer decoding, and idempotent PostgreSQL persistence.

## 项目概览 / Overview

`chain-indexer-service` 将实时区块头通知与权威链数据读取分离：WebSocket 只负责触发扫描，HTTP JSON-RPC 负责读取区块、交易和回执。Scanner 在确认深度之后串行处理区块，并在写入前再次核对同高度区块头；发生分叉时定位公共祖先、作废旧分支数据，再从 canonical 分支继续推进。

`chain-indexer-service` separates real-time head notifications from authoritative chain-data reads. WebSocket subscriptions trigger scans, while HTTP JSON-RPC supplies blocks, transactions, and receipts. The scanner processes blocks sequentially after the configured confirmation depth, rechecks the same-height header before persistence, and recovers from forks by locating the common ancestor, invalidating the displaced branch, and continuing from the canonical branch.

原始日志先作为链事实保存，再由独立的 transaction worker 按业务游标批量消费。Worker 通过共享 ABI 解码组件识别 ERC20、ERC721 和 ERC1155 转账，并将解析结果与游标推进放在同一数据库事务中。

Raw logs are persisted as chain facts before an independent transaction worker consumes them in cursor-based batches. The worker uses the shared ABI decoder to identify ERC20, ERC721, and ERC1155 transfers, committing parsed results and cursor advancement in one database transaction.

## 架构 / Architecture

```text
                                   EVM Networks
                    Ethereum · BSC · Polygon · Arbitrum
                      Optimism · Base · Avalanche C-Chain
                         │                          │
                HTTP JSON-RPC                WebSocket newHeads
                         │                          │
                         ▼                          ▼
              ┌──────────────────┐      ┌──────────────────────┐
              │ provider.Reader  │      │ HeadSubscriber       │
              │ blocks/receipts  │      │ ordered head signals │
              └────────┬─────────┘      └──────────┬───────────┘
                       │                           │
                       │                    ┌──────▼───────┐
                       │                    │  Listener    │
                       │                    │ serial Run   │
                       │                    └──────┬───────┘
                       │                           │ HeadHandler
                       └──────────────┬────────────┘
                                      ▼
                            ┌───────────────────┐
                            │      Scanner      │
                            │ confirmations     │
                            │ final-header check│
                            │ reorg recovery    │
                            └─────────┬─────────┘
                                      │ canonical batch
                                      ▼
                         ┌──────────────────────────┐
                         │ ChainRepository interface│
                         └────────────┬─────────────┘
                                      │
                           ┌──────────▼──────────┐
                           │ ChainDAO · GORM     │
                           │ PostgreSQL fencing  │
                           └──────────┬──────────┘
                                      │
       ┌──────────────────────────────▼──────────────────────────────┐
       │                         PostgreSQL                          │
       │ blocks · transactions · raw events · transfers · cursors  │
       └──────────────────────────────┬──────────────────────────────┘
                                      │ raw events by increasing ID
                                      ▼
                         ┌──────────────────────────┐
                         │ EventRepository interface│
                         └────────────┬─────────────┘
                                      │ batch + business cursor
                                      ▼
                         ┌──────────────────────────┐
                         │ Transaction Worker       │
                         │ pkg/evm/abi Decoder      │
                         │ ERC20 · ERC721 · ERC1155 │
                         └────────────┬─────────────┘
                                      │ transfers + cursor atomically
                                      └──────────────────────► PostgreSQL

       cmd/chain-indexer + ioc: explicit construction, lifecycle, cleanup
       cmd/migrate ──► migration runner ──► script/postgre ──► PostgreSQL
```

架构中的接口隔离链数据编排与存储实现：Scanner 和 Worker 只依赖 repository 契约，GORM 与 PostgreSQL 细节保留在 DAO 层。`cmd/chain-indexer/ioc` 显式构造所有生产依赖；migration 使用独立命令和版本化 SQL，不混入服务启动流程。

Repository interfaces isolate chain-data orchestration from storage implementation. The scanner and worker depend only on repository contracts, while GORM and PostgreSQL details remain in the DAO layer. `cmd/chain-indexer/ioc` constructs production dependencies explicitly; migrations use a separate command and versioned SQL rather than running during service startup.

## 核心能力 / Core Capabilities

- **多链配置 / Multi-chain configuration** — 内置 Ethereum、BSC、Polygon PoS、Arbitrum、Optimism、Base 和 Avalanche C-Chain 的链身份与确认深度约束。Built-in chain identity and confirmation-depth constraints for seven EVM networks.
- **HTTP 与 WebSocket 职责分离 / Split HTTP and WebSocket responsibilities** — WebSocket 提供有序的新区块头信号，HTTP 提供用于持久化的完整链数据。WebSocket supplies ordered head signals; HTTP supplies the complete data used for persistence.
- **确认区块扫描 / Confirmed-block scanning** — 从显式 `startBlock` 或 checkpoint 继续，逐块读取交易和回执，失败时不越过当前高度。Resumes from an explicit `startBlock` or checkpoint and never advances past a failed height.
- **重组恢复 / Reorg recovery** — 基于区块哈希查找公共祖先，以 reorg ID 和持久化状态保护作废与恢复操作。Locates the common ancestor by block hash and fences invalidation or recovery with the reorg identity and persisted state.
- **严格重复处理 / Strict replay semantics** — canonical replay 只验证既有数据，不隐式补写或恢复已失效数据。Canonical replay validates existing data without silently backfilling or restoring invalidated rows.
- **标准转账解码 / Standard transfer decoding** — 支持 Native、ERC20、ERC721，以及 ERC1155 `TransferSingle`/`TransferBatch`。Supports Native, ERC20, ERC721, and ERC1155 `TransferSingle`/`TransferBatch` transfers.
- **幂等消费 / Idempotent consumption** — Worker 按 raw event 自增 ID 分页，重复业务数据由自然键约束忽略，整批完成后才推进 `(biz, chain_family)` 游标。The worker pages by increasing raw-event ID, relies on natural keys for duplicate suppression, and advances the `(biz, chain_family)` cursor only after the full batch succeeds.
- **版本化持久化 / Versioned persistence** — PostgreSQL schema 由成对的 Up/Down SQL migration 管理，服务启动只执行 schema identity gate。PostgreSQL schema is governed by paired Up/Down SQL migrations; service startup performs only a schema identity gate.

## 数据一致性 / Data Consistency

本项目采用 optimistic consistency，而不是把 RPC 节点和 PostgreSQL 描述成分布式事务。Scanner 的最终区块头复核能够缩小观察窗口；数据库唯一约束、状态 fencing、原子 checkpoint 和后续 head/reorg 处理共同维持本地一致性。

The project uses optimistic consistency and does not present an RPC node plus PostgreSQL as a distributed transaction. The scanner's final-header observation narrows the race window, while database uniqueness, state fencing, atomic checkpoints, and subsequent head/reorg processing preserve local consistency.

被分叉淘汰的数据不会被普通保存路径自动恢复。显式恢复必须匹配原始 reorg ID、失效时间和完整从属数据集合；任何身份、状态或集合不一致都会使整个事务失败。

Data displaced by a fork is never restored by the ordinary save path. Explicit recovery must match the original reorg ID, invalidation timestamp, and complete dependent-data set; any identity, state, or set mismatch fails the entire transaction.

## 模块边界 / Module Boundaries

| Module | Responsibility / 职责 |
| --- | --- |
| `cmd/chain-indexer` | 服务生命周期、信号处理和显式依赖装配 / Service lifecycle, signals, and explicit dependency wiring |
| `cmd/migrate` | 独立数据库迁移入口 / Standalone database migration entry point |
| `internal/config` | 严格 YAML、环境变量展开和链配置校验 / Strict YAML, environment expansion, and chain validation |
| `internal/domain` | 不依赖基础设施的链数据模型和值对象 / Infrastructure-independent chain models and value objects |
| `internal/rpc/provider` | 标准 EVM HTTP reader 与 WebSocket subscriber / Standard EVM HTTP reader and WebSocket subscriber |
| `internal/listener` | 有序消费新区块头并调用 scanner / Ordered new-head consumption and scanner dispatch |
| `internal/scanner` | 确认区块同步、映射、最终身份复核与重组恢复 / Confirmed-block synchronization, mapping, final identity checks, and reorg recovery |
| `internal/worker/transaction` | 批量消费 raw event、生成 transfer 并推进业务游标 / Batch raw-event consumption, transfer production, and business-cursor advancement |
| `internal/repository` | 面向领域的数据访问接口与映射 / Domain-facing data-access contracts and mapping |
| `internal/repository/postgre` | GORM entity、DAO、事务和 PostgreSQL fencing / GORM entities, DAOs, transactions, and PostgreSQL fencing |
| `pkg/evm/abi` | 通用 ABI 事件声明、解码和标准转账 catalog / Generic ABI event declarations, decoding, and standard transfer catalog |
| `script/postgre` | PostgreSQL schema 的版本化 SQL 权威 / Versioned SQL authority for the PostgreSQL schema |

## 技术栈 / Technology

- Go 1.22
- go-ethereum ABI and RPC tooling
- PostgreSQL 14+
- GORM
- YAML configuration
- Docker Compose-backed deterministic integration testing

## 当前范围 / Current Scope

当前 V1 聚焦链数据摄取、重组安全、标准转账解析和持久化正确性。项目尚未提供面向业务的查询 API、钱包地址归属、交易业务类型分类、RPC 节点池、供应商自动切换、无界重试或分布式强一致保证。

V1 focuses on chain-data ingestion, reorg safety, standard transfer decoding, and persistence correctness. It does not yet provide a business-facing query API, wallet-address ownership, business transaction classification, RPC node pools, provider failover, unbounded retries, or distributed strong consistency.

## License

This project is licensed under the terms in [LICENSE](./LICENSE).

# chain-indexer-service

多 EVM 链索引服务。仓库当前处于 V1 验收准备阶段：已有配置、migration、repository、scanner/worker 及 E2E 验收源码；T042 已审核冻结，T041/T043/T044 仅有 compile/list 证据，T045 仅作 static 审计。真实 PostgreSQL、RPC、WebSocket、真实 binary 与测试体执行统一 deferred 到 T046。不要将源码存在、测试枚举或旧的局部历史证据写成整体 GREEN。

## 当前边界

- 支持七链配置：Ethereum、BSC、Polygon PoS、Arbitrum、Optimism、Base、Avalanche C-Chain；BSC 的供应商支持属于 Open Beta，Avalanche 仅覆盖 C-Chain。
- `scanner` 负责通过 HTTP 获取权威区块/事件数据，并持久化 transaction 与 Native 数据；WS 仅触发新的 head 通知。`worker` 负责读取 raw event、解码 token transfer，并与业务 cursor 原子提交。
- 当前验收中的 reorg 证据分三层：Infura 固定历史 canonical 只读参考（LE-01–13）、T026 repository/A→B→A/worker 交错历史证据，以及 LE-22–29 完整 runtime authority（动态 HTTP/WSS fork fixture、真实 service binary + PostgreSQL，执行仍 deferred）。LE-30–35 单列为 worker persistence 权威；各层职责不同，不互相替代。
- 当前不承诺生产部署、节点池、供应商 fallback、内部无界重试或已完成的 live/mainnet 验收。

## 配置与安全

可从 [`config.example.yaml`](config.example.yaml) 复制配置。每条启用链必须显式填写非负 `startBlock`；`startBlock: 0` 是明确选择，不是默认值。HTTP URL 只接受 `http`/`https`，WebSocket URL 只接受 `ws`/`wss`。仓库不保存 key、完整 endpoint、DSN 或原始供应商响应。

Infura live 验收只读取本地 `config/config.local.yaml`。该文件必须保持 ignored、未被 Git 跟踪，并且只读使用；不要把凭据写入命令参数、日志、测试输出、manifest 或提交。固定历史样本来自无密钥 manifest，运行时不得用 `latest`、块 0、公共 RPC 或动态换样本补足覆盖。

## Migration 与启动

Migration 是独立命令，不属于 `chain-indexer` 子命令：

```bash
go run ./cmd/migrate up --config ./config.yaml
go run ./cmd/migrate status --config ./config.yaml
go run ./cmd/migrate down --config ./config.yaml
```

服务入口：

```bash
go run ./cmd/chain-indexer --config ./config.yaml
```

启动阶段只表示配置、PostgreSQL 连接及 schema gate 检查通过；不要据此声称 RPC listener、区块同步、reorg 或 worker 已在生产运行。服务不会自动执行 migration。

## 测试与验收命令

普通检查离线运行，不访问 PostgreSQL 或 RPC：

```bash
go test ./...
go vet ./...
go build ./...
```

### 本地确定性验收

```bash
make e2e
```

该命令使用动态标准 HTTP/WSS fixture、真实 PostgreSQL 14.14 与真实 service binary，Phase A 固定负责 LE-14–35（包括 LE-22–29 fork/reorg、LE-30–35 worker persistence）。它不读取 Infura endpoint，不需要测试 DSN 参数；Compose 使用专用项目并在成功、失败、信号和超时路径清理容器与 volume。固定预算为最多 22,400 个计费请求额度（本地 fixture 不产生供应商额度），并受有界超时约束。

### Infura 只读参考（显式 opt-in）

```bash
CHAIN_INDEXER_LIVE_E2E=1 make e2e-live
```

该命令只在显式 opt-in 且本地配置文件存在时运行，Phase B 固定负责 LE-01–13：七链 chainId、HTTP/WSS 生命周期及 manifest 中的固定历史 block/transaction/receipt/log 只读核对。单 key 串行、总预算最多 22,400 个计费 RPC 请求、受有界超时约束；不签名、不广播、不调用 `eth_send*`，也不使用公共 RPC fallback。失败、取消、超时或 catalog/summary 错误都会先清理专用 Compose 项目与 volume；未取得真实 test action 的场景保持 `NOT_RUN`，不得伪造 PASS。

`make e2e-live` 的辅助目标 `e2e-live-up`、`e2e-live-run`、`e2e-live-summary`、`e2e-live-down` 是生命周期原语；通常只使用总入口。不要把 `-list`、catalog、compile 或 static ledger 检查当作场景 GREEN。

## 验收状态

T041 的 manifest/只读预检、T043 的 live 源码和 T044 的本地 runtime/fork/worker 源码目前只允许 compile/list；T045 只做 README 与 default-deny 静态审计。所有真实测试体、Docker/PG、HTTP/WSS、fork/reorg、worker persistence 和 36 个 LE 场景的最终 PASS/FAIL/NOT_RUN 状态，只能由获得最终授权的 T046 统一执行和记录。

因此，本 README 描述的是已实现的命令与验收边界，不是一次已经运行完成的验收报告。

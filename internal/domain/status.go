package domain

// BlockStatus 表示区块在重组语义下的状态。
type BlockStatus string

const (
	// BlockStatusCanonical 表示区块属于当前规范链。
	BlockStatusCanonical BlockStatus = "CANONICAL"
	// BlockStatusOrphaned 表示区块已被重组淘汰。
	BlockStatusOrphaned BlockStatus = "ORPHANED"
)

// Finality 表示区块的最终性阶段。
type Finality string

const (
	// FinalityConfirmed 表示区块已达到确认数要求。
	FinalityConfirmed Finality = "CONFIRMED"
)

// ChainStatus 表示链上记录是否仍处于有效链。
type ChainStatus string

const (
	// ChainStatusActive 表示记录仍属于有效链。
	ChainStatusActive ChainStatus = "ACTIVE"
	// ChainStatusReverted 表示记录因重组被回滚。
	ChainStatusReverted ChainStatus = "REVERTED"
)

// ExecutionStatus 表示交易执行结果。
type ExecutionStatus string

const (
	// ExecutionStatusSuccess 表示交易执行成功。
	ExecutionStatusSuccess ExecutionStatus = "SUCCESS"
	// ExecutionStatusFailed 表示交易执行失败。
	ExecutionStatusFailed ExecutionStatus = "FAILED"
)

// TransferSource 表示转账记录的来源。
type TransferSource string

const (
	// TransferSourceTransactionValue 表示交易原生币 value 转账。
	TransferSourceTransactionValue TransferSource = "TRANSACTION_VALUE"
	// TransferSourceEventLog 表示来自链上事件日志的转账。
	TransferSourceEventLog TransferSource = "EVENT_LOG"
)

// TransferType 表示转账资产类型。
type TransferType string

// TransferSourceIndex 表示转账来源的稳定索引值。
type TransferSourceIndex string

const (
	// TransferSourceIndexTransactionValue 是原生交易 value 转账的稳定来源索引。
	TransferSourceIndexTransactionValue TransferSourceIndex = "tx-value"
)

const (
	// TransferTypeNative 表示原生币转账。
	TransferTypeNative TransferType = "NATIVE"
	// TransferTypeERC20 表示 ERC-20 代币转账。
	TransferTypeERC20 TransferType = "ERC20"
	// TransferTypeERC721 表示 ERC-721 非同质化代币转账。
	TransferTypeERC721 TransferType = "ERC721"
	// TransferTypeERC1155 表示 ERC-1155 多资产代币转账。
	TransferTypeERC1155 TransferType = "ERC1155"
)

// ReorgStatus 表示重组处理记录的状态。
type ReorgStatus string

const (
	// ReorgStatusCompleted 表示重组处理已完成。
	ReorgStatusCompleted ReorgStatus = "COMPLETED"
)

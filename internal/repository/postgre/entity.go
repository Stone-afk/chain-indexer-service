package postgre

import "github.com/Stone-afk/chain-indexer-service/pkg/xsql"

// Export entity names for the repository mapping boundary.
type ChainCheckpoint = chainCheckpointRow
type ChainBlock = chainBlockRow
type ChainTransaction = chainTransactionRow
type ChainTransfer = chainTransferRow
type ChainReorg = chainReorgRow

type EventTransferResult struct {
	RawEventID int64
	Transfers  []ChainTransfer
}

type CanonicalBatch struct {
	Block           ChainBlock
	Transactions    []ChainTransaction
	RawEvents       []ChainRawEvent
	NativeTransfers []ChainTransfer
	Checkpoint      ChainCheckpoint
}

type chainCheckpointRow struct {
	ChainID             int64  `gorm:"column:chain_id;primaryKey;autoIncrement:false"`
	LastProcessedHeight int64  `gorm:"column:last_processed_height"`
	LastProcessedHash   string `gorm:"column:last_processed_hash"`
	CreatedAt           int64  `gorm:"column:created_at"`
	UpdatedAt           int64  `gorm:"column:updated_at"`
}

func (chainCheckpointRow) TableName() string { return "chain_checkpoints" }

type chainBlockRow struct {
	ChainID          int64           `gorm:"column:chain_id;primaryKey"`
	BlockNumber      int64           `gorm:"column:block_number"`
	BlockHash        string          `gorm:"column:block_hash;primaryKey"`
	ParentHash       string          `gorm:"column:parent_hash"`
	BlockTime        int64           `gorm:"column:block_time"`
	Status           string          `gorm:"column:status;default:CANONICAL"`
	Finality         string          `gorm:"column:finality;default:CONFIRMED"`
	TransactionCount int             `gorm:"column:transaction_count"`
	StateRoot        *string         `gorm:"column:state_root"`
	TransactionsRoot *string         `gorm:"column:transactions_root"`
	ReceiptsRoot     *string         `gorm:"column:receipts_root"`
	GasLimit         *string         `gorm:"column:gas_limit;type:numeric(78,0)"`
	GasUsed          *string         `gorm:"column:gas_used;type:numeric(78,0)"`
	BaseFeePerGas    *string         `gorm:"column:base_fee_per_gas;type:numeric(78,0)"`
	RawBlock         *xsql.JSONField `gorm:"column:raw_block;type:jsonb"`
	RawReceipts      *xsql.JSONField `gorm:"column:raw_receipts;type:jsonb"`
	ReorgID          *string         `gorm:"column:reorg_id;type:uuid"`
	OrphanedAt       *int64          `gorm:"column:orphaned_at"`
	ChainVersion     int64           `gorm:"column:chain_version;default:1"`
	CreatedAt        int64           `gorm:"column:created_at"`
}

func (chainBlockRow) TableName() string { return "chain_blocks" }

type ChainRawEvent struct {
	ID              int64          `gorm:"column:id"`
	ChainID         int64          `gorm:"column:chain_id;primaryKey"`
	BlockNumber     int64          `gorm:"column:block_number"`
	BlockTime       int64          `gorm:"column:block_time;->"`
	BlockHash       string         `gorm:"column:block_hash;primaryKey"`
	TxHash          string         `gorm:"column:tx_hash"`
	TxIndex         *int           `gorm:"column:tx_index"`
	LogIndex        int            `gorm:"column:log_index;primaryKey"`
	ContractAddress string         `gorm:"column:contract_address"`
	Topic0          *string        `gorm:"column:topic0"`
	Topic1          *string        `gorm:"column:topic1"`
	Topic2          *string        `gorm:"column:topic2"`
	Topic3          *string        `gorm:"column:topic3"`
	EventData       *string        `gorm:"column:event_data"`
	RawLog          xsql.JSONField `gorm:"column:raw_log;type:jsonb"`
	ParseStatus     string         `gorm:"column:parse_status;default:PENDING"`
	ParserVersion   *int           `gorm:"column:parser_version"`
	ParseError      *string        `gorm:"column:parse_error"`
	ChainStatus     string         `gorm:"column:chain_status;default:ACTIVE"`
	ReorgID         *string        `gorm:"column:reorg_id;type:uuid"`
	CreatedAt       int64          `gorm:"column:created_at"`
	ParsedAt        *int64         `gorm:"column:parsed_at"`
	RevertedAt      *int64         `gorm:"column:reverted_at"`
}

func (ChainRawEvent) TableName() string { return "chain_raw_events" }

type chainEventBizCursor struct {
	ID          int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Biz         string `gorm:"column:biz"`
	ChainFamily string `gorm:"column:chain_family"`
	Cursor      int64  `gorm:"column:cursor"`
}

func (chainEventBizCursor) TableName() string { return "chain_event_biz_cursors" }

type chainTransactionRow struct {
	ChainID           int64           `gorm:"column:chain_id;primaryKey"`
	BlockNumber       int64           `gorm:"column:block_number"`
	BlockHash         string          `gorm:"column:block_hash;primaryKey"`
	TxHash            string          `gorm:"column:tx_hash;primaryKey"`
	TxIndex           int             `gorm:"column:tx_index"`
	FromAddress       string          `gorm:"column:from_address"`
	ToAddress         *string         `gorm:"column:to_address"`
	Nonce             int64           `gorm:"column:nonce"`
	Value             string          `gorm:"column:value;type:numeric(78,0);default:0"`
	Input             *string         `gorm:"column:input"`
	MethodID          *string         `gorm:"column:method_id"`
	MethodName        *string         `gorm:"column:method_name"`
	GasLimit          *string         `gorm:"column:gas_limit;type:numeric(78,0)"`
	GasUsed           *string         `gorm:"column:gas_used;type:numeric(78,0)"`
	GasPrice          *string         `gorm:"column:gas_price;type:numeric(78,0)"`
	EffectiveGasPrice *string         `gorm:"column:effective_gas_price;type:numeric(78,0)"`
	TransactionFee    *string         `gorm:"column:transaction_fee;type:numeric(78,0)"`
	ExecutionStatus   string          `gorm:"column:execution_status"`
	ContractAddress   *string         `gorm:"column:contract_address"`
	ChainStatus       string          `gorm:"column:chain_status;default:ACTIVE"`
	ReorgID           *string         `gorm:"column:reorg_id;type:uuid"`
	RawTransaction    *xsql.JSONField `gorm:"column:raw_transaction;type:jsonb"`
	RawReceipt        *xsql.JSONField `gorm:"column:raw_receipt;type:jsonb"`
	BlockTime         int64           `gorm:"column:block_time"`
	CreatedAt         int64           `gorm:"column:created_at"`
	RevertedAt        *int64          `gorm:"column:reverted_at"`
}

func (chainTransactionRow) TableName() string { return "chain_transactions" }

type chainTransferRow struct {
	ID           int64           `gorm:"column:id;primaryKey;autoIncrement"`
	ChainID      int64           `gorm:"column:chain_id"`
	BlockNumber  int64           `gorm:"column:block_number"`
	BlockHash    string          `gorm:"column:block_hash"`
	TxHash       string          `gorm:"column:tx_hash"`
	TxIndex      *int            `gorm:"column:tx_index"`
	SourceType   string          `gorm:"column:source_type"`
	SourceIndex  string          `gorm:"column:source_index"`
	TransferType string          `gorm:"column:transfer_type"`
	TokenAddress *string         `gorm:"column:token_address"`
	FromAddress  string          `gorm:"column:from_address"`
	ToAddress    string          `gorm:"column:to_address"`
	Amount       *string         `gorm:"column:amount;type:numeric(78,0)"`
	TokenID      *string         `gorm:"column:token_id;type:numeric(78,0)"`
	BatchIndex   int             `gorm:"column:batch_index"`
	ChainStatus  string          `gorm:"column:chain_status;default:ACTIVE"`
	ReorgID      *string         `gorm:"column:reorg_id;type:uuid"`
	RawData      *xsql.JSONField `gorm:"column:raw_data;type:jsonb"`
	BlockTime    int64           `gorm:"column:block_time"`
	CreatedAt    int64           `gorm:"column:created_at"`
	RevertedAt   *int64          `gorm:"column:reverted_at"`
}

func (chainTransferRow) TableName() string { return "chain_transfers" }

type chainReorgRow struct {
	ReorgID        string  `gorm:"column:reorg_id;primaryKey;type:uuid"`
	ChainID        int64   `gorm:"column:chain_id"`
	AncestorHeight int64   `gorm:"column:ancestor_height"`
	AncestorHash   string  `gorm:"column:ancestor_hash"`
	OldHeadHeight  int64   `gorm:"column:old_head_height"`
	OldHeadHash    string  `gorm:"column:old_head_hash"`
	NewHeadHeight  int64   `gorm:"column:new_head_height"`
	NewHeadHash    string  `gorm:"column:new_head_hash"`
	ReorgDepth     int     `gorm:"column:reorg_depth"`
	Status         string  `gorm:"column:status"`
	DetectedAt     int64   `gorm:"column:detected_at"`
	CompletedAt    *int64  `gorm:"column:completed_at"`
	ErrorMessage   *string `gorm:"column:error_message"`
}

func (chainReorgRow) TableName() string { return "chain_reorgs" }

package domain

import "encoding/json"

type StoredBlock struct {
	ChainID          int64
	BlockNumber      int64
	BlockHash        string
	ParentHash       string
	BlockTime        int64
	Status           string
	Finality         string
	TransactionCount int
	StateRoot        *string
	TransactionsRoot *string
	ReceiptsRoot     *string
	GasLimit         *string
	GasUsed          *string
	BaseFeePerGas    *string
	RawBlock         json.RawMessage
	RawReceipts      json.RawMessage
	ReorgID          *string
	OrphanedAt       *int64
	ChainVersion     int64
	CreatedAt        int64
}

type CanonicalBatch struct {
	Block           StoredBlock
	Transactions    []StoredTransaction
	RawEvents       []RawEvent
	NativeTransfers []Transfer
	Checkpoint      Checkpoint
}

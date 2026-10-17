package domain

import "encoding/json"

type StoredTransaction struct {
	ChainID           int64
	BlockNumber       int64
	BlockHash         string
	TxHash            string
	TxIndex           int
	FromAddress       string
	ToAddress         *string
	Nonce             int64
	Value             string
	Input             *string
	MethodID          *string
	MethodName        *string
	TxType            *int16
	GasLimit          *string
	GasUsed           *string
	GasPrice          *string
	EffectiveGasPrice *string
	TransactionFee    *string
	ExecutionStatus   string
	ContractAddress   *string
	ChainStatus       string
	ReorgID           *string
	RawTransaction    json.RawMessage
	RawReceipt        json.RawMessage
	BlockTime         int64
	CreatedAt         int64
	RevertedAt        *int64
}

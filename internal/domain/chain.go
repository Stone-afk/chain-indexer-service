package domain

import "encoding/json"

type BlockHeader struct {
	Number     int64
	Hash       string
	ParentHash string
	Timestamp  int64
}

type Block struct {
	Header           BlockHeader
	TransactionsRoot string
	ReceiptsRoot     string
	StateRoot        string
	GasUsed          int64
	GasLimit         int64
	BaseFeePerGas    string
	Transactions     []Transaction
	RawBlock         json.RawMessage
}

type Transaction struct {
	Hash           string
	Index          int
	From           string
	To             *string
	Nonce          int64
	Value          string
	Input          string
	Type           int
	GasLimit       int64
	GasPrice       string
	RawTransaction json.RawMessage
}

type Receipt struct {
	TxHash            string
	BlockNumber       int64
	BlockHash         string
	TxIndex           int
	Status            int
	GasUsed           int64
	EffectiveGasPrice string
	ContractAddress   *string
	Logs              []Log
	RawReceipt        json.RawMessage
}

type Log struct {
	BlockNumber int64
	BlockHash   string
	TxHash      string
	TxIndex     int
	Index       int
	Address     string
	Topics      []string
	Data        string
	RawLog      json.RawMessage
}

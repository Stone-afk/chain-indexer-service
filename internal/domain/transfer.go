package domain

import "encoding/json"

type Transfer struct {
	ID           int64
	ChainID      int64
	BlockNumber  int64
	BlockHash    string
	TxHash       string
	TxIndex      *int
	SourceType   string
	SourceIndex  string
	TransferType string
	TokenAddress *string
	FromAddress  string
	ToAddress    string
	Amount       *string
	TokenID      *string
	BatchIndex   int
	ChainStatus  string
	ReorgID      *string
	RawData      json.RawMessage
	BlockTime    int64
	CreatedAt    int64
	RevertedAt   *int64
}

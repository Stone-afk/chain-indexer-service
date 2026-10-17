package domain

import "encoding/json"

// EventTransferResult is the deterministic decode result for one consumed event.
// Empty Transfers still records that the event was consumed.
type EventTransferResult struct {
	RawEventID int64
	Transfers  []Transfer
}

type RawEvent struct {
	ID              int64
	ChainID         int64
	BlockNumber     int64
	BlockHash       string
	TxHash          string
	TxIndex         *int
	LogIndex        int
	ContractAddress string
	Topic0          *string
	Topic1          *string
	Topic2          *string
	Topic3          *string
	EventData       *string
	RawLog          json.RawMessage
	ChainStatus     string
	ReorgID         *string
	RevertedAt      *int64
}

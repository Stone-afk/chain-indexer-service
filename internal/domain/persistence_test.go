package domain

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCanonicalPersistenceDomain(t *testing.T) {
	batch := CanonicalBatch{Block: StoredBlock{ChainID: 1, BlockNumber: 12, BlockHash: "b", Status: "CANONICAL", Finality: "CONFIRMED", RawBlock: json.RawMessage(`{"number":12}`)}, Transactions: []StoredTransaction{{ChainID: 1, BlockHash: "b", TxHash: "tx", Value: "123", ExecutionStatus: "SUCCESS"}}, RawEvents: []RawEvent{{ChainID: 1, BlockHash: "b", LogIndex: 2}}, NativeTransfers: []Transfer{{SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", BatchIndex: 0, Amount: persistencePtr("123")}}, Checkpoint: Checkpoint{ChainID: 1, LastProcessedHeight: 12, LastProcessedHash: "b"}}
	if batch.Block.BlockHash != batch.Checkpoint.LastProcessedHash || batch.Transactions[0].Value != "123" || *batch.NativeTransfers[0].Amount != "123" {
		t.Fatalf("batch=%+v", batch)
	}
	reorg := Reorg{ReorgID: "uuid", ChainID: 1, AncestorHeight: 10, AncestorHash: "a", OldHeadHeight: 12, OldHeadHash: "b", NewHeadHeight: 13, NewHeadHash: "c", ReorgDepth: 2, DetectedAt: 42}
	if reorg.AncestorHeight != 10 || reorg.ReorgDepth != 2 {
		t.Fatalf("reorg=%+v", reorg)
	}
	for _, value := range []any{batch.Block, batch.Transactions[0], batch.NativeTransfers[0], batch.Checkpoint, reorg} {
		typ := reflect.TypeOf(value)
		for i := 0; i < typ.NumField(); i++ {
			if typ.Field(i).Tag.Get("gorm") != "" {
				t.Fatalf("domain has ORM tag: %v", typ)
			}
		}
	}
}
func persistencePtr(s string) *string { return &s }

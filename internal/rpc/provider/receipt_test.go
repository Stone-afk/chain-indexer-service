package provider

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

type receiptIdentityAPI struct{}

func (receiptIdentityAPI) GetTransactionReceipt(context.Context, string) (map[string]any, error) {
	return map[string]any{
		"transactionHash":   "0xfeed",
		"blockNumber":       "0x2a",
		"blockHash":         "0xbeef",
		"transactionIndex":  "0x7",
		"status":            "0x1",
		"gasUsed":           "0x5208",
		"effectiveGasPrice": "0x5",
		"contractAddress":   nil,
		"logs":              []any{},
	}, nil
}

func TestEVMProviderReceiptIdentity(t *testing.T) {
	server := rpc.NewServer()
	if err := server.RegisterName("eth", receiptIdentityAPI{}); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	reader := &client{rpc: rpc.DialInProc(server)}
	defer reader.rpc.Close()

	receipt, err := reader.ReceiptByTransactionHash(context.Background(), "0xfeed")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.BlockNumber != 42 || receipt.BlockHash != "0xbeef" || receipt.TxIndex != 7 || receipt.TxHash != "0xfeed" {
		t.Fatalf("receipt identity = %+v", receipt)
	}
	if len(receipt.Logs) != 0 {
		t.Fatalf("logs = %+v; want none", receipt.Logs)
	}
	if !json.Valid(receipt.RawReceipt) {
		t.Fatal("raw receipt is not valid JSON")
	}
	var raw map[string]any
	if err := json.Unmarshal(receipt.RawReceipt, &raw); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"transactionHash":   "0xfeed",
		"blockNumber":       "0x2a",
		"blockHash":         "0xbeef",
		"transactionIndex":  "0x7",
		"status":            "0x1",
		"gasUsed":           "0x5208",
		"effectiveGasPrice": "0x5",
		"contractAddress":   nil,
		"logs":              []any{},
	}
	if !reflect.DeepEqual(raw, want) {
		t.Fatalf("raw receipt = %#v; want %#v", raw, want)
	}
}

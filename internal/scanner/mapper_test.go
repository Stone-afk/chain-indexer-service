package scanner

import (
	"encoding/json"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"reflect"
	"testing"
)

func TestMapperCompleteCanonicalBatch(t *testing.T) {
	b, rs := blockFixture(4)
	before, _ := json.Marshal([]any{b, rs})
	got, err := mapCanonicalBatch(1, b, rs, 999)
	require(t, !(err != nil), err)
	require(t, !(got.Checkpoint != (domain.Checkpoint{ChainID: 1, LastProcessedHeight: 4, LastProcessedHash: "hash-4"})), got.Checkpoint)
	v := got.Block
	require(t, v.ChainID == 1 && v.BlockNumber == 4 && v.BlockHash == "hash-4" && v.ParentHash == "hash-3" && v.BlockTime == 104 && v.Status == "CANONICAL" && v.Finality == "CONFIRMED" && v.TransactionCount == 1 && v.CreatedAt == 999, v)
	var rawReceipts, expectedReceipts any
	require(t, json.Unmarshal(v.RawReceipts, &rawReceipts) == nil, string(v.RawReceipts))
	require(t, json.Unmarshal(rs[0].RawReceipt, &expectedReceipts) == nil, string(rs[0].RawReceipt))
	require(t, reflect.DeepEqual(rawReceipts, []any{expectedReceipts}), rawReceipts)
	if v.StateRoot == nil || *v.StateRoot != "state" || v.TransactionsRoot == nil || *v.TransactionsRoot != "txroot" || v.ReceiptsRoot == nil || *v.ReceiptsRoot != "receiptroot" || v.GasLimit == nil || *v.GasLimit != "30000" || v.GasUsed == nil || *v.GasUsed != "21000" || v.BaseFeePerGas == nil || *v.BaseFeePerGas != "7" || !reflect.DeepEqual(v.RawBlock, b.RawBlock) {
		t.Fatalf("block payload %+v", v)
	}
	require(t, len(got.Transactions) == 1 && len(got.RawEvents) == 1 && len(got.NativeTransfers) == 1, got)
	tx := got.Transactions[0]
	if tx.ChainID != 1 || tx.BlockNumber != 4 || tx.BlockHash != "hash-4" || tx.TxHash != "tx-4" || tx.TxIndex != 0 || tx.FromAddress != "sender" || tx.ToAddress == nil || *tx.ToAddress != "recipient" || tx.Nonce != 4 || tx.Value != "10" || tx.ExecutionStatus != "SUCCESS" || tx.ChainStatus != "ACTIVE" || tx.BlockTime != 104 || tx.CreatedAt != 999 {
		t.Fatalf("tx %+v", tx)
	}
	if tx.TransactionFee == nil || *tx.TransactionFee != "189000" || tx.EffectiveGasPrice == nil || *tx.EffectiveGasPrice != "9" || tx.GasUsed == nil || *tx.GasUsed != "21000" || tx.GasPrice == nil || *tx.GasPrice != "11" || tx.GasLimit == nil || *tx.GasLimit != "22000" || tx.Input == nil || *tx.Input != "0x12345678abcd" || tx.MethodID == nil || *tx.MethodID != "0x12345678" || tx.TxType == nil || *tx.TxType != 2 || !reflect.DeepEqual(tx.RawTransaction, b.Transactions[0].RawTransaction) || !reflect.DeepEqual(tx.RawReceipt, rs[0].RawReceipt) {
		t.Fatalf("tx payload %+v", tx)
	}
	e := got.RawEvents[0]
	if e.ChainID != 1 || e.BlockNumber != 4 || e.BlockHash != "hash-4" || e.TxHash != "tx-4" || e.TxIndex == nil || *e.TxIndex != 0 || e.LogIndex != 3 || e.ContractAddress != "token" || e.ChainStatus != "ACTIVE" || e.Topic0 == nil || *e.Topic0 != "a" || e.Topic1 == nil || *e.Topic1 != "b" || e.Topic2 == nil || *e.Topic2 != "c" || e.Topic3 == nil || *e.Topic3 != "d" || e.EventData == nil || *e.EventData != "0xbeef" || !reflect.DeepEqual(e.RawLog, rs[0].Logs[0].RawLog) {
		t.Fatalf("event %+v", e)
	}
	n := got.NativeTransfers[0]
	if n.ChainID != 1 || n.BlockNumber != 4 || n.BlockHash != "hash-4" || n.TxHash != "tx-4" || n.TxIndex == nil || *n.TxIndex != 0 || n.SourceType != "TRANSACTION_VALUE" || n.SourceIndex != "tx-value" || n.BatchIndex != 0 || n.TransferType != "NATIVE" || n.FromAddress != "sender" || n.ToAddress != "recipient" || n.Amount == nil || *n.Amount != "10" || n.TokenAddress != nil || n.ChainStatus != "ACTIVE" || n.BlockTime != 104 || n.CreatedAt != 999 {
		t.Fatalf("native %+v", n)
	}
	after, _ := json.Marshal([]any{b, rs})
	require(t, !(string(before) != string(after)), "mapper mutated input")
}
func TestMapperReceiptIdentityRejectsWholeBatch(t *testing.T) {
	for _, name := range []string{"missing", "extra", "duplicate", "tx hash", "log block number", "log block hash", "log tx hash", "log tx index"} {
		t.Run(name, func(t *testing.T) {
			b, rs := blockFixture(4)
			switch name {
			case "missing":
				rs = nil
			case "extra":
				rs = append(rs, domain.Receipt{TxHash: "extra"})
			case "duplicate":
				b.Transactions = append(b.Transactions, domain.Transaction{Hash: "second", Index: 1})
				rs = append(rs, rs[0])
			case "tx hash":
				rs[0].TxHash = "wrong"
			case "log block number":
				rs[0].Logs[0].BlockNumber = 5
			case "log block hash":
				rs[0].Logs[0].BlockHash = "wrong"
			case "log tx hash":
				rs[0].Logs[0].TxHash = "wrong"
			case "log tx index":
				rs[0].Logs[0].TxIndex = 1
			}
			got, err := mapCanonicalBatch(1, b, rs, 999)
			require(t, err != nil && reflect.DeepEqual(got, domain.CanonicalBatch{}), got, err)
		})
	}
}

func TestMapperReceiptIdentityWithoutLogs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Block, *domain.Receipt)
	}{
		{name: "valid", mutate: func(_ *domain.Block, _ *domain.Receipt) {}},
		{name: "block number", mutate: func(_ *domain.Block, r *domain.Receipt) { r.BlockNumber++ }},
		{name: "block hash", mutate: func(_ *domain.Block, r *domain.Receipt) { r.BlockHash = "wrong" }},
		{name: "transaction index", mutate: func(_ *domain.Block, r *domain.Receipt) { r.TxIndex++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, rs := blockFixture(4)
			rs[0].Logs = nil
			tc.mutate(&b, &rs[0])
			got, err := mapCanonicalBatch(1, b, rs, 999)
			if tc.name == "valid" {
				require(t, err == nil, err)
				return
			}
			require(t, err != nil && reflect.DeepEqual(got, domain.CanonicalBatch{}), got, err)
		})
	}
}

func TestMapperUint256Boundaries(t *testing.T) {
	const max = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	const maxPlusOne = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	for _, tc := range []struct {
		name    string
		mutate  func(*domain.Block, *domain.Transaction, *domain.Receipt)
		wantErr bool
	}{
		{name: "value max", mutate: func(_ *domain.Block, tx *domain.Transaction, _ *domain.Receipt) { tx.Value = max }},
		{name: "value max plus one", mutate: func(_ *domain.Block, tx *domain.Transaction, _ *domain.Receipt) { tx.Value = maxPlusOne }, wantErr: true},
		{name: "value plus one", mutate: func(_ *domain.Block, tx *domain.Transaction, _ *domain.Receipt) { tx.Value = "+1" }, wantErr: true},
		{name: "gas price max", mutate: func(_ *domain.Block, tx *domain.Transaction, r *domain.Receipt) {
			tx.GasPrice = max
			r.EffectiveGasPrice = ""
			r.GasUsed = 1
		}},
		{name: "gas price max plus one", mutate: func(_ *domain.Block, tx *domain.Transaction, r *domain.Receipt) {
			tx.GasPrice = maxPlusOne
			r.EffectiveGasPrice = ""
			r.GasUsed = 1
		}, wantErr: true},
		{name: "gas price plus one", mutate: func(_ *domain.Block, tx *domain.Transaction, r *domain.Receipt) {
			tx.GasPrice = "+1"
			r.EffectiveGasPrice = ""
			r.GasUsed = 1
		}, wantErr: true},
		{name: "gas price invalid despite effective", mutate: func(_ *domain.Block, tx *domain.Transaction, _ *domain.Receipt) { tx.GasPrice = "+1" }, wantErr: true},
		{name: "effective gas price max", mutate: func(_ *domain.Block, _ *domain.Transaction, r *domain.Receipt) {
			r.EffectiveGasPrice = max
			r.GasUsed = 1
		}},
		{name: "effective gas price max plus one", mutate: func(_ *domain.Block, _ *domain.Transaction, r *domain.Receipt) {
			r.EffectiveGasPrice = maxPlusOne
			r.GasUsed = 1
		}, wantErr: true},
		{name: "effective gas price plus one", mutate: func(_ *domain.Block, _ *domain.Transaction, r *domain.Receipt) {
			r.EffectiveGasPrice = "+1"
			r.GasUsed = 1
		}, wantErr: true},
		{name: "base fee max", mutate: func(b *domain.Block, _ *domain.Transaction, _ *domain.Receipt) { b.BaseFeePerGas = max }},
		{name: "base fee max plus one", mutate: func(b *domain.Block, _ *domain.Transaction, _ *domain.Receipt) { b.BaseFeePerGas = maxPlusOne }, wantErr: true},
		{name: "base fee plus one", mutate: func(b *domain.Block, _ *domain.Transaction, _ *domain.Receipt) { b.BaseFeePerGas = "+1" }, wantErr: true},
		{name: "fee max times one", mutate: func(_ *domain.Block, _ *domain.Transaction, r *domain.Receipt) {
			r.EffectiveGasPrice = max
			r.GasUsed = 1
		}},
		{name: "fee max times two", mutate: func(_ *domain.Block, _ *domain.Transaction, r *domain.Receipt) {
			r.EffectiveGasPrice = max
			r.GasUsed = 2
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, rs := blockFixture(4)
			tc.mutate(&b, &b.Transactions[0], &rs[0])
			got, err := mapCanonicalBatch(1, b, rs, 999)
			require(t, (err != nil) == tc.wantErr, err)
			if tc.wantErr {
				require(t, reflect.DeepEqual(got, domain.CanonicalBatch{}), got)
			}
		})
	}
}

func TestMapperOptionalGasPricesRequireOnePrice(t *testing.T) {
	b, rs := blockFixture(4)
	b.Transactions[0].GasPrice = ""
	rs[0].EffectiveGasPrice = ""
	got, err := mapCanonicalBatch(1, b, rs, 999)
	require(t, err != nil && reflect.DeepEqual(got, domain.CanonicalBatch{}), got, err)
}
func TestMapperNullableNumeric(t *testing.T) {
	for _, value := range []string{"", "0"} {
		t.Run("value="+value, func(t *testing.T) {
			b, rs := blockFixture(4)
			b.BaseFeePerGas = value
			b.Transactions[0].GasPrice = value
			got, err := mapCanonicalBatch(1, b, rs, 999)
			require(t, err == nil, err)
			for _, field := range []*string{got.Block.BaseFeePerGas, got.Transactions[0].GasPrice} {
				if value == "" {
					require(t, field == nil, field)
				} else {
					require(t, field != nil && *field == value, field)
				}
			}
		})
	}
}

func TestMapperNativeAndFee(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		status      int
		creation    bool
		recipient   string
		want        int
	}{
		{"ordinary", "10", 1, false, "recipient", 1}, {"zero", "0", 1, false, "", 0}, {"failed", "10", 0, false, "", 0}, {"creation", "10", 1, true, "created", 1},
		{"precision", "9007199254740993123456789", 1, false, "recipient", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, rs := blockFixture(4)
			b.Transactions[0].Value = tc.value
			rs[0].Status = tc.status
			rs[0].EffectiveGasPrice = "9007199254740993"
			rs[0].GasUsed = 3
			if tc.creation {
				b.Transactions[0].To = nil
				to := "created"
				rs[0].ContractAddress = &to
			}
			got, err := mapCanonicalBatch(1, b, rs, 999)
			require(t, !(err != nil), err)
			require(t, !(len(got.NativeTransfers) != tc.want), got.NativeTransfers)
			if tc.want == 1 {
				n := got.NativeTransfers[0]
				require(t, !(n.ToAddress != tc.recipient || n.Amount == nil || *n.Amount != tc.value), n)
			}
			tx := got.Transactions[0]
			wantStatus := "SUCCESS"
			if tc.status == 0 {
				wantStatus = "FAILED"
			}
			require(t, !(tx.ExecutionStatus != wantStatus || tx.TransactionFee == nil || *tx.TransactionFee != "27021597764222979"), tx)
			require(t, !(tc.creation && (tx.ToAddress != nil || tx.ContractAddress == nil || *tx.ContractAddress != "created")), tx)
		})
	}
}

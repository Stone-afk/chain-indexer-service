package transaction

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
)

func pword(v uint64) string { return fmt.Sprintf("0x%064x", v) }
func pptr(v string) *string { return &v }
func batchPayload(ids, values []uint64) string {
	words := []uint64{64, uint64(64 + 32 + 32*len(ids)), uint64(len(ids))}
	words = append(words, ids...)
	words = append(words, uint64(len(values)))
	words = append(words, values...)
	var out string
	for _, word := range words {
		out += fmt.Sprintf("%064x", word)
	}
	return "0x" + out
}
func mustCatalog(t *testing.T) abi.TransferEvents {
	t.Helper()
	c, e := abi.NewTransferEvents()
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func mustParser(t *testing.T) *Parser {
	t.Helper()
	p, e := NewParser()
	if e != nil {
		t.Fatal(e)
	}
	return p
}

func setRawEventBlockTime(t *testing.T, event *domain.RawEvent, want int64) {
	t.Helper()
	v := reflect.ValueOf(event).Elem().FieldByName("BlockTime")
	if v.IsValid() && v.CanSet() {
		v.SetInt(want)
	}
}

func transferBlockTime(tr domain.Transfer) int64 {
	v := reflect.ValueOf(tr).FieldByName("BlockTime")
	if v.IsValid() {
		return v.Int()
	}
	return 0
}

func TestParserStandardTransfersAndDomainMapping(t *testing.T) {
	c := mustCatalog(t)
	p := mustParser(t)
	from, to := pword(1), pword(2)
	raw := json.RawMessage(`{"log":1}`)
	batchData := batchPayload([]uint64{7, 7}, []uint64{11, 13})
	cases := []struct {
		name       string
		e          domain.RawEvent
		n          int
		typ        string
		id, amount string
		idx        int
	}{
		{"erc20", domain.RawEvent{ID: 1, BlockHash: "bh1", ChainID: 1, BlockNumber: 10, TxHash: "tx", TxIndex: pptrInt(3), LogIndex: 4, ContractAddress: "token", Topic0: &c.ERC20Transfer.Topic0, Topic1: pptr(from), Topic2: pptr(to), EventData: pptr("0x" + fmt.Sprintf("%064x", 42)), RawLog: raw, ChainStatus: "ACTIVE", ReorgID: pptr("r1")}, 1, "ERC20", "", "42", 0},
		{"erc721", domain.RawEvent{ID: 2, BlockHash: "bh2", ChainID: 1, BlockNumber: 10, TxHash: "tx", TxIndex: pptrInt(3), LogIndex: 5, ContractAddress: "token", Topic0: &c.ERC721Transfer.Topic0, Topic1: pptr(from), Topic2: pptr(to), Topic3: pptr(pword(7)), EventData: pptr("0x"), RawLog: raw, ChainStatus: "REVERTED", ReorgID: pptr("r2"), RevertedAt: pptrInt64(99)}, 1, "ERC721", "7", "1", 0},
		{"single", domain.RawEvent{ID: 3, BlockHash: "bh3", ChainID: 1, BlockNumber: 10, TxHash: "tx", TxIndex: pptrInt(3), LogIndex: 6, ContractAddress: "token", Topic0: &c.ERC1155TransferSingle.Topic0, Topic1: pptr(pword(3)), Topic2: pptr(from), Topic3: pptr(to), EventData: pptr("0x" + fmt.Sprintf("%064x%064x", 7, 11)), RawLog: raw, ChainStatus: "ACTIVE"}, 1, "ERC1155", "7", "11", 0},
		{"batch", domain.RawEvent{ID: 4, BlockHash: "bh4", ChainID: 1, BlockNumber: 10, TxHash: "tx", TxIndex: pptrInt(3), LogIndex: 7, ContractAddress: "token", Topic0: &c.ERC1155TransferBatch.Topic0, Topic1: pptr(pword(3)), Topic2: pptr(from), Topic3: pptr(to), EventData: pptr(batchData), RawLog: raw, ChainStatus: "REVERTED", ReorgID: pptr("r4"), RevertedAt: pptrInt64(100)}, 2, "ERC1155", "7", "11", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRawEventBlockTime(t, &tc.e, 1234)
			got, err := p.Parse(tc.e)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Transfers) != tc.n {
				t.Fatalf("count=%d", len(got.Transfers))
			}
			if got.RawEventID != tc.e.ID {
				t.Fatalf("raw id=%d", got.RawEventID)
			}
			for i, tr := range got.Transfers {
				if transferBlockTime(tr) != 1234 {
					t.Fatalf("block time=%d", tr.BlockTime)
				}
				if tr.BlockHash != tc.e.BlockHash || tr.ChainID != 1 || tr.BlockNumber != 10 || tr.TxHash != "tx" || tr.TxIndex == nil || *tr.TxIndex != 3 || tr.SourceType != "EVENT_LOG" || tr.SourceIndex != fmt.Sprint(tc.e.LogIndex) || tr.TransferType != tc.typ || tr.TokenAddress == nil || *tr.TokenAddress != "token" || tr.FromAddress != "0x0000000000000000000000000000000000000001" || tr.ToAddress != "0x0000000000000000000000000000000000000002" || string(tr.RawData) != string(raw) || tr.ChainStatus != tc.e.ChainStatus {
					t.Fatalf("transfer=%+v", tr)
				}
				if (tr.ReorgID == nil) != (tc.e.ReorgID == nil) || (tr.ReorgID != nil && *tr.ReorgID != *tc.e.ReorgID) || (tr.RevertedAt == nil) != (tc.e.RevertedAt == nil) || (tr.RevertedAt != nil && *tr.RevertedAt != *tc.e.RevertedAt) {
					t.Fatalf("metadata=%+v", tr)
				}
				if tc.name == "batch" && tr.BatchIndex != i {
					t.Fatalf("batch index=%d want=%d", tr.BatchIndex, i)
				}
				if i == 0 {
					if tc.id != "" && (tr.TokenID == nil || *tr.TokenID != tc.id) {
						t.Fatalf("id=%v", tr.TokenID)
					}
					if tr.Amount == nil || *tr.Amount != tc.amount {
						t.Fatalf("amount=%v", tr.Amount)
					}
				} else if tr.BatchIndex != 1 || tr.TokenID == nil || *tr.TokenID != "7" || tr.Amount == nil || *tr.Amount != "13" {
					t.Fatalf("batch=%+v", tr)
				}
			}
		})
	}
}

func TestParserUnknownMalformedAndBatchShapeAreEmpty(t *testing.T) {
	c := mustCatalog(t)
	p := mustParser(t)
	bad := []domain.RawEvent{{Topic0: pptr("0xdead"), EventData: pptr("0x")}, {Topic0: &c.ERC20Transfer.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), EventData: pptr("0x")}, {Topic0: &c.ERC1155TransferBatch.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), Topic3: pptr(pword(3)), EventData: pptr("0x" + fmt.Sprintf("%064x%064x%064x", 128, 256, 1))}}
	bad = append(bad,
		domain.RawEvent{ID: 99, Topic0: &c.ERC1155TransferBatch.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), Topic3: pptr(pword(3)), EventData: pptr(batchPayload(nil, nil))},
		domain.RawEvent{ID: 100, Topic0: &c.ERC1155TransferBatch.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), Topic3: pptr(pword(3)), EventData: pptr(batchPayload(nil, []uint64{7}))},
	)
	for _, e := range bad {
		got, err := p.Parse(e)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Transfers) != 0 {
			t.Fatalf("partial=%+v", got)
		}
		if got.RawEventID != e.ID {
			t.Fatalf("raw id=%d", got.RawEventID)
		}
	}
}

func TestParserMapDecodedBatchCheckedConversionIsAtomic(t *testing.T) {
	c := mustCatalog(t)
	p := mustParser(t)
	raw := domain.RawEvent{ID: 201, BlockHash: "bh", ChainID: 1, BlockNumber: 2, TxHash: "tx", LogIndex: 3, ContractAddress: "token", RawLog: json.RawMessage(`{"i":201}`), ChainStatus: "ACTIVE"}
	base := func(ids, values any) map[string]any {
		return map[string]any{"operator": "0x0000000000000000000000000000000000000003", "from": "0x0000000000000000000000000000000000000001", "to": "0x0000000000000000000000000000000000000002", "ids": ids, "values": values}
	}
	for _, tc := range []struct {
		name    string
		decoded map[string]any
	}{
		{"wrong Go array type", base([]uint64{7, 9}, []*big.Int{big.NewInt(11), big.NewInt(13)})},
		{"bad later item", base([]*big.Int{big.NewInt(7), nil}, []*big.Int{big.NewInt(11), big.NewInt(13)})},
		{"negative later item", base([]*big.Int{big.NewInt(7), big.NewInt(-1)}, []*big.Int{big.NewInt(11), big.NewInt(13)})},
		{"wrong values Go array type", base([]*big.Int{big.NewInt(7), big.NewInt(9)}, []uint64{11, 13})},
		{"nil later values item", base([]*big.Int{big.NewInt(7), big.NewInt(9)}, []*big.Int{big.NewInt(11), nil})},
		{"negative later values item", base([]*big.Int{big.NewInt(7), big.NewInt(9)}, []*big.Int{big.NewInt(11), big.NewInt(-1)})},
		{"wrong scalar type", map[string]any{"operator": 3, "from": "0x0000000000000000000000000000000000000001", "to": "0x0000000000000000000000000000000000000002", "ids": []*big.Int{big.NewInt(7)}, "values": []*big.Int{big.NewInt(11)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := p.mapDecoded(raw, c.ERC1155TransferBatch, tc.decoded)
			if got.RawEventID != raw.ID || len(got.Transfers) != 0 {
				t.Fatalf("result=%+v", got)
			}
		})
	}
}

func pptrInt(v int) *int       { return &v }
func pptrInt64(v int64) *int64 { return &v }

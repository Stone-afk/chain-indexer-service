package abi

import (
	"math/big"
	"reflect"
	"testing"
)

func TestTransferEvents(t *testing.T) {
	catalog, err := NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	var _ TransferEvents = catalog
	const from = "0x0000000000000000000000000000000000000001"
	const to = "0x0000000000000000000000000000000000000002"
	const operator = "0x0000000000000000000000000000000000000003"
	tests := []struct {
		name, declaration, eventName string
		event                        ParsedEvent
		names, types                 []string
		indexed                      []bool
		topicWords                   []uint64
		data                         string
		want                         map[string]any
	}{
		{"ERC20", "event Transfer(address indexed from, address indexed to, uint256 value)", "Transfer", catalog.ERC20Transfer, []string{"from", "to", "value"}, []string{"address", "address", "uint256"}, []bool{true, true, false}, []uint64{1, 2}, "0x" + fmtWord(42), map[string]any{"from": from, "to": to, "value": big.NewInt(42)}},
		{"ERC721", "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)", "Transfer", catalog.ERC721Transfer, []string{"from", "to", "tokenId"}, []string{"address", "address", "uint256"}, []bool{true, true, true}, []uint64{1, 2, 7}, "0x", map[string]any{"from": from, "to": to, "tokenId": big.NewInt(7)}},
		{"ERC1155 Single", "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)", "TransferSingle", catalog.ERC1155TransferSingle, []string{"operator", "from", "to", "id", "value"}, []string{"address", "address", "address", "uint256", "uint256"}, []bool{true, true, true, false, false}, []uint64{3, 1, 2}, "0x" + fmtWord(7) + fmtWord(11), map[string]any{"operator": operator, "from": from, "to": to, "id": big.NewInt(7), "value": big.NewInt(11)}},
		{"ERC1155 Batch", "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)", "TransferBatch", catalog.ERC1155TransferBatch, []string{"operator", "from", "to", "ids", "values"}, []string{"address", "address", "address", "uint256[]", "uint256[]"}, []bool{true, true, true, false, false}, []uint64{3, 1, 2}, "0x" + fmtWord(64) + fmtWord(160) + fmtWord(2) + fmtWord(7) + fmtWord(9) + fmtWord(2) + fmtWord(11) + fmtWord(13), map[string]any{"operator": operator, "from": from, "to": to, "ids": []*big.Int{big.NewInt(7), big.NewInt(9)}, "values": []*big.Int{big.NewInt(11), big.NewInt(13)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseEventDeclaration(tt.declaration)
			if err != nil {
				t.Fatal(err)
			}
			if tt.event.Name != tt.eventName || tt.event.Event.Name != tt.eventName {
				t.Fatalf("event names = %q/%q", tt.event.Name, tt.event.Event.Name)
			}
			if !reflect.DeepEqual(tt.event.ParamNames, tt.names) || !reflect.DeepEqual(tt.event.IndexedFlags, tt.indexed) {
				t.Fatalf("metadata = %v/%v", tt.event.ParamNames, tt.event.IndexedFlags)
			}
			if tt.event.Topic0 != parsed.Topic0 || tt.event.Event.ID != parsed.Event.ID {
				t.Fatalf("topic differs from parsed declaration")
			}
			if len(tt.event.Event.Inputs) != len(tt.types) {
				t.Fatalf("input count = %d", len(tt.event.Event.Inputs))
			}
			for i, input := range tt.event.Event.Inputs {
				if input.Name != tt.names[i] || input.Type.String() != tt.types[i] || input.Indexed != tt.indexed[i] {
					t.Errorf("input %d = %+v", i, input)
				}
			}
			topics := []string{parsed.Topic0}
			for _, word := range tt.topicWords {
				topics = append(topics, "0x"+fmtWord(word))
			}
			got, err := NewDecoder().Decode(tt.event, topics, tt.data)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("decoded = %#v, want %#v", got, tt.want)
			}
		})
	}
	if catalog.ERC20Transfer.Topic0 != catalog.ERC721Transfer.Topic0 {
		t.Error("ERC20/ERC721 Transfer signatures differ")
	}
	if catalog.ERC1155TransferSingle.Topic0 == catalog.ERC1155TransferBatch.Topic0 || catalog.ERC1155TransferSingle.Topic0 == catalog.ERC20Transfer.Topic0 || catalog.ERC1155TransferBatch.Topic0 == catalog.ERC20Transfer.Topic0 {
		t.Error("distinct transfer signatures share topic0")
	}
}

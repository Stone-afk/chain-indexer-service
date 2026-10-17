package abi

import (
	"errors"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

func TestDecoderDecode(t *testing.T) {
	decoder := NewDecoder()
	word := func(n uint64) string { return "0x" + fmtWord(n) }
	address := "0x" + strings.Repeat("0", 39) + "1"
	paddedAddress := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(address, "0x")
	parse := func(declaration string) ParsedEvent {
		t.Helper()
		event, err := ParseEventDeclaration(declaration)
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	check := func(t *testing.T, got any, want any) {
		t.Helper()
		if reflect.TypeOf(got) != reflect.TypeOf(want) || !reflect.DeepEqual(got, want) {
			t.Fatalf("value = %#v (%T), want %#v (%T)", got, got, want, want)
		}
	}
	checkDecodeError := func(t *testing.T, err error) {
		t.Helper()
		var decodeErr *DecodeError
		if err == nil || !errors.As(err, &decodeErr) || decodeErr == nil {
			t.Fatalf("error = %v, want a classifiable *DecodeError", err)
		}
	}

	t.Run("indexed address and uint256", func(t *testing.T) {
		event := parse("event Item(address indexed owner, uint256 indexed id)")
		got, err := decoder.Decode(event, []string{event.Topic0, paddedAddress, word(42)}, "0x")
		if err != nil {
			t.Fatal(err)
		}
		check(t, got["owner"], address)
		check(t, got["id"], big.NewInt(42))
	})

	t.Run("non-indexed uint256", func(t *testing.T) {
		event := parse("event Value(uint256 amount)")
		got, err := decoder.Decode(event, []string{event.Topic0}, word(42))
		if err != nil {
			t.Fatal(err)
		}
		check(t, got["amount"], big.NewInt(42))
	})

	t.Run("ERC1155 Batch uint256 arrays", func(t *testing.T) {
		event := parse("event TransferBatch(uint256[] ids, uint256[] values)")
		// Two offsets followed by two equal-length dynamic arrays.
		data := "0x" + fmtWord(64) + fmtWord(160) +
			fmtWord(2) + fmtWord(7) + fmtWord(9) +
			fmtWord(2) + fmtWord(11) + fmtWord(13)
		got, err := decoder.Decode(event, []string{event.Topic0}, data)
		if err != nil {
			t.Fatal(err)
		}
		check(t, got["ids"], []*big.Int{big.NewInt(7), big.NewInt(9)})
		check(t, got["values"], []*big.Int{big.NewInt(11), big.NewInt(13)})
	})

	t.Run("topic mismatch is unsupported", func(t *testing.T) {
		event := parse("event Value(uint256 amount)")
		got, err := decoder.Decode(event, []string{"0x" + strings.Repeat("0", 64)}, word(1))
		if got != nil || err != nil {
			t.Fatalf("got %#v, %v; want nil, nil", got, err)
		}
	})

	for _, tc := range []struct {
		name   string
		topics []string
		data   string
	}{
		{"topic count", nil, word(1)},
		{"malformed data", []string{""}, "0x01"},
	} {
		t.Run("malformed "+tc.name, func(t *testing.T) {
			event := parse("event Value(uint256 amount)")
			topics := tc.topics
			if tc.name == "malformed data" {
				topics = []string{event.Topic0}
			}
			_, err := decoder.Decode(event, topics, tc.data)
			checkDecodeError(t, err)
		})
	}

	t.Run("matched indexed topics are validated", func(t *testing.T) {
		event := parse("event Item(address indexed owner, uint256 indexed id)")
		for _, tc := range []struct {
			name   string
			topics []string
		}{
			{"missing indexed topic", []string{event.Topic0, paddedAddress}},
			{"extra indexed topic", []string{event.Topic0, paddedAddress, word(42), word(43)}},
			{"malformed indexed word", []string{event.Topic0, "0x01", word(42)}},
			{"nonzero indexed address padding", []string{event.Topic0, "0x01" + strings.Repeat("00", 31), word(42)}},
			{"non-hex indexed word", []string{event.Topic0, paddedAddress, "0x" + strings.Repeat("00", 31) + "gg"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := decoder.Decode(event, tc.topics, "0x")
				checkDecodeError(t, err)
			})
		}
	})

	for _, tc := range []struct {
		name, declaration, data string
	}{
		{"trailing data word", "event Value(uint256 amount)", "0x" + fmtWord(1) + fmtWord(2)},
		{"dynamic offset points into head", "event Values(uint256[] ids)", "0x" + fmtWord(0)},
		{"non-indexed address padding", "event Owner(address owner)", "0x01" + strings.Repeat("00", 30) + "01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := parse(tc.declaration)
			_, err := decoder.Decode(event, []string{event.Topic0}, tc.data)
			checkDecodeError(t, err)
		})
	}

	t.Run("inputs unchanged and values use approved Go types", func(t *testing.T) {
		event := parse("event Values(address indexed owner, uint256 count, bool enabled, bytes payload, uint256[] ids)")
		topics := []string{event.Topic0, paddedAddress}
		beforeTopics := append([]string(nil), topics...)
		// ABI head has four words; bytes and ids begin at offsets 128 and 192.
		data := "0x" + fmtWord(3) + fmtWord(1) + fmtWord(128) + fmtWord(192) + fmtWord(1) + "ff" + strings.Repeat("00", 31) + fmtWord(2) + fmtWord(5) + fmtWord(6)
		beforeData := data
		got, err := decoder.Decode(event, topics, data)
		if err != nil {
			t.Fatal(err)
		}
		check(t, got["owner"], address)
		check(t, got["count"], big.NewInt(3))
		check(t, got["enabled"], true)
		check(t, got["payload"], []byte{0xff})
		check(t, got["ids"], []*big.Int{big.NewInt(5), big.NewInt(6)})
		if !reflect.DeepEqual(topics, beforeTopics) || data != beforeData {
			t.Fatal("input changed")
		}
		for key, value := range got {
			switch value.(type) {
			case string, *big.Int, []*big.Int, bool, []byte:
			default:
				t.Errorf("%q has unapproved decoded type %T", key, value)
			}
		}
	})
}

func fmtWord(n uint64) string {
	h := big.NewInt(int64(n)).Text(16)
	return strings.Repeat("0", 64-len(h)) + h
}

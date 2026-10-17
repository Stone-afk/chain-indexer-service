package abi

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestDecodeTransfer(t *testing.T) {
	erc20, err := ParseEventDeclaration("event Transfer(address indexed from, address indexed to, uint256 value)")
	if err != nil {
		t.Fatal(err)
	}
	erc721, err := ParseEventDeclaration("event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)")
	if err != nil {
		t.Fatal(err)
	}
	from, to := "0x"+strings.Repeat("0", 39)+"1", "0x"+strings.Repeat("0", 39)+"2"
	padded := func(addr string) string { return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(addr, "0x") }
	word := func(n byte) string {
		h := strconv.FormatUint(uint64(n), 16)
		return "0x" + strings.Repeat("0", 64-len(h)) + h
	}
	t.Run("ERC20", func(t *testing.T) {
		topics := []string{erc20.Topic0, padded(from), padded(to)}
		got, status, err := DecodeTransfer(erc20, topics, word(42))
		if err != nil || status != DecodeStatusERC20 || got.From != from || got.To != to || got.Amount != "42" || got.TokenID != "" {
			t.Fatalf("got %#v, %v, %v", got, status, err)
		}
	})
	t.Run("ERC721", func(t *testing.T) {
		topics := []string{erc721.Topic0, padded(from), padded(to), word(42)}
		got, status, err := DecodeTransfer(erc721, topics, "0x")
		if err != nil || status != DecodeStatusERC721 || got.From != from || got.To != to || got.TokenID != "42" || got.Amount != "" {
			t.Fatalf("got %#v, %v, %v", got, status, err)
		}
	})
	t.Run("unsupported", func(t *testing.T) {
		_, status, err := DecodeTransfer(erc20, []string{"0x" + strings.Repeat("0", 64)}, "0x")
		if status != DecodeStatusUnsupported || err != nil {
			t.Fatalf("got %v, %v", status, err)
		}
	})
	t.Run("event topic is authoritative", func(t *testing.T) {
		customTopic := "0x" + strings.Repeat("a", 64)
		event := erc20
		event.Topic0 = customTopic
		got, status, err := DecodeTransfer(event, []string{customTopic, padded(from), padded(to)}, word(7))
		if err != nil || status != DecodeStatusERC20 || got.Amount != "7" {
			t.Fatalf("got %#v, %v, %v", got, status, err)
		}
	})
	bad := []struct {
		name   string
		event  ParsedEvent
		topics []string
		data   string
	}{
		{"topic count", erc20, []string{erc20.Topic0, padded(from)}, word(1)},
		{"address padding", erc20, []string{erc20.Topic0, "0x01" + strings.Repeat("0", 62), padded(to)}, word(1)},
		{"malformed address word", erc20, []string{erc20.Topic0, "0xzz" + strings.Repeat("0", 62), padded(to)}, word(1)},
		{"short uint word", erc20, []string{erc20.Topic0, padded(from), padded(to)}, "0x01"},
		{"long ERC20 data", erc20, []string{erc20.Topic0, padded(from), padded(to)}, word(1) + strings.Repeat("0", 64)},
		{"ERC721 data", erc721, []string{erc721.Topic0, padded(from), padded(to), word(1)}, "0x00"},
		{"short ERC721 topics", erc721, []string{erc721.Topic0, padded(from), padded(to)}, "0x"},
		{"long ERC721 topics", erc721, []string{erc721.Topic0, padded(from), padded(to), word(1), word(2)}, "0x"},
		{"malformed uint word", erc20, []string{erc20.Topic0, padded(from), padded(to)}, "0x" + strings.Repeat("z", 64)},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, status, err := DecodeTransfer(tt.event, tt.topics, tt.data)
			if status != DecodeStatusFailed || !errors.Is(err, ErrMalformedTransferLog) {
				t.Fatalf("got %v, %v", status, err)
			}
		})
	}
	t.Run("input immutability", func(t *testing.T) {
		topics := []string{erc20.Topic0, padded(from), padded(to)}
		before := append([]string(nil), topics...)
		data := word(9)
		_, _, _ = DecodeTransfer(erc20, topics, data)
		if !reflect.DeepEqual(topics, before) || data != word(9) {
			t.Fatal("input changed")
		}
	})
}

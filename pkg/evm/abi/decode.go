package abi

import (
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
)

type DecodeStatus string

const (
	DecodeStatusUnsupported DecodeStatus = "UNSUPPORTED"
	DecodeStatusERC20       DecodeStatus = "ERC20"
	DecodeStatusERC721      DecodeStatus = "ERC721"
	DecodeStatusFailed      DecodeStatus = "FAILED"
)

var ErrMalformedTransferLog = errors.New("malformed ERC20/ERC721 Transfer log")

type DecodedTransfer struct {
	From    string
	To      string
	Amount  string
	TokenID string
}

func DecodeTransfer(event ParsedEvent, topics []string, data string) (DecodedTransfer, DecodeStatus, error) {
	if len(topics) > 0 && !strings.EqualFold(topics[0], event.Topic0) {
		return DecodedTransfer{}, DecodeStatusUnsupported, nil
	}
	malformed := func() (DecodedTransfer, DecodeStatus, error) {
		return DecodedTransfer{}, DecodeStatusFailed, ErrMalformedTransferLog
	}
	if event.Name != "Transfer" || event.Event.Name != "Transfer" || len(event.ParamNames) != 3 || len(event.IndexedFlags) != 3 || len(event.Event.Inputs) != 3 || event.ParamNames[0] != "from" || event.ParamNames[1] != "to" || event.Event.Inputs[0].Type.String() != "address" || event.Event.Inputs[1].Type.String() != "address" || event.Event.Inputs[2].Type.String() != "uint256" {
		return malformed()
	}
	indexedValue := event.IndexedFlags[2]
	wantValueName := "value"
	if indexedValue {
		wantValueName = "tokenId"
	}
	if event.ParamNames[2] != wantValueName || indexedValue != event.Event.Inputs[2].Indexed || event.IndexedFlags[0] != event.Event.Inputs[0].Indexed || event.IndexedFlags[1] != event.Event.Inputs[1].Indexed || !event.IndexedFlags[0] || !event.IndexedFlags[1] {
		return malformed()
	}
	wantTopics := 3
	if indexedValue {
		wantTopics = 4
	}
	if len(topics) != wantTopics {
		return malformed()
	}
	from, ok := transferAddress(topics[1])
	if !ok {
		return malformed()
	}
	to, ok := transferAddress(topics[2])
	if !ok {
		return malformed()
	}
	if indexedValue {
		id, ok := transferWord(topics[3])
		if !ok || data != "0x" {
			return malformed()
		}
		return DecodedTransfer{From: from, To: to, TokenID: new(big.Int).SetBytes(id).String()}, DecodeStatusERC721, nil
	}
	value, ok := transferWord(data)
	if !ok {
		return malformed()
	}
	return DecodedTransfer{From: from, To: to, Amount: new(big.Int).SetBytes(value).String()}, DecodeStatusERC20, nil
}

func transferWord(s string) ([]byte, bool) {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") {
		return nil, false
	}
	b, err := hex.DecodeString(s[2:])
	return b, err == nil
}

func transferAddress(s string) (string, bool) {
	b, ok := transferWord(s)
	if !ok {
		return "", false
	}
	for _, v := range b[:12] {
		if v != 0 {
			return "", false
		}
	}
	return "0x" + hex.EncodeToString(b[12:]), true
}

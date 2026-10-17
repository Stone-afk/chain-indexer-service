package abi

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	ethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

type Decoder struct{}

func NewDecoder() *Decoder { return &Decoder{} }

func (d *Decoder) Decode(event ParsedEvent, topics []string, data string) (map[string]any, error) {
	if len(topics) > 0 && topics[0] != "" && !strings.EqualFold(topics[0], event.Topic0) {
		return nil, nil
	}
	fail := func(err error) (map[string]any, error) { return nil, &DecodeError{Err: err} }
	inputs := event.Event.Inputs
	if len(event.ParamNames) != len(inputs) {
		return fail(fmt.Errorf("parameter name count mismatch"))
	}
	wantTopics := 1
	for _, arg := range inputs {
		if arg.Indexed {
			wantTopics++
		}
	}
	if len(topics) != wantTopics || topics[0] == "" {
		return fail(fmt.Errorf("invalid topic count or missing signature"))
	}
	if !strings.HasPrefix(data, "0x") {
		return fail(fmt.Errorf("data must be hex prefixed"))
	}
	raw, err := hex.DecodeString(data[2:])
	if err != nil {
		return fail(err)
	}
	if len(raw)%32 != 0 || (len(inputs.NonIndexed()) == 0 && len(raw) != 0) {
		return fail(fmt.Errorf("invalid data length"))
	}
	values, err := inputs.NonIndexed().Unpack(raw)
	if err != nil {
		return fail(err)
	}
	// Unpack accepts trailing bytes, aliased offsets, and nonzero address padding.
	canonical, err := inputs.NonIndexed().Pack(values...)
	if err != nil {
		return fail(err)
	}
	if !bytes.Equal(raw, canonical) {
		return fail(fmt.Errorf("non-canonical event data"))
	}
	out := make(map[string]any, len(inputs))
	topicIndex, valueIndex := 1, 0
	for i, arg := range inputs {
		var value any
		if arg.Indexed {
			word, ok := transferWord(topics[topicIndex])
			if !ok {
				return fail(fmt.Errorf("invalid indexed word for %s", event.ParamNames[i]))
			}
			if arg.Type.T == ethabi.AddressTy {
				if _, ok := transferAddress(topics[topicIndex]); !ok {
					return fail(fmt.Errorf("invalid indexed address"))
				}
			}
			arg.Indexed = false
			decoded, err := (ethabi.Arguments{arg}).Unpack(word)
			if err != nil {
				return fail(err)
			}
			value = decoded[0]
			topicIndex++
		} else {
			value = values[valueIndex]
			valueIndex++
		}
		switch v := value.(type) {
		case common.Address:
			value = strings.ToLower(v.Hex())
		case string, *big.Int, []*big.Int, bool, []byte:
		default:
			return fail(fmt.Errorf("unsupported decoded type %T", value))
		}
		out[event.ParamNames[i]] = value
	}
	return out, nil
}

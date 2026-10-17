package abi

import (
	"fmt"
	"regexp"
	"strings"

	ethabi "github.com/ethereum/go-ethereum/accounts/abi"
)

var solidityIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type ParsedEvent struct {
	Name         string
	Event        ethabi.Event
	Topic0       string
	ParamNames   []string
	IndexedFlags []bool
}

func ParseEventDeclaration(declaration string) (ParsedEvent, error) {
	s := strings.TrimSpace(declaration)
	if !strings.HasPrefix(s, "event ") || strings.HasSuffix(s, ";") {
		return ParsedEvent{}, fmt.Errorf("invalid event declaration")
	}
	open := strings.IndexByte(s, '(')
	if open < len("event ")+1 || !strings.HasSuffix(s, ")") {
		return ParsedEvent{}, fmt.Errorf("invalid event declaration")
	}
	name := strings.TrimSpace(s[len("event "):open])
	if !solidityIdentifier.MatchString(name) {
		return ParsedEvent{}, fmt.Errorf("invalid event name")
	}
	body := s[open+1 : len(s)-1]
	var args ethabi.Arguments
	var names []string
	var flags []bool
	if strings.TrimSpace(body) != "" {
		for _, raw := range strings.Split(body, ",") {
			fields := strings.Fields(raw)
			if len(fields) < 2 || len(fields) > 3 {
				return ParsedEvent{}, fmt.Errorf("malformed parameter %q", raw)
			}
			indexed, pos := false, 1
			if fields[1] == "indexed" {
				indexed, pos = true, 2
			}
			if len(fields) != pos+1 {
				return ParsedEvent{}, fmt.Errorf("malformed parameter %q", raw)
			}
			if !solidityIdentifier.MatchString(fields[pos]) {
				return ParsedEvent{}, fmt.Errorf("invalid parameter name %q", fields[pos])
			}
			typ, err := ethabi.NewType(fields[0], "", nil)
			if err != nil {
				return ParsedEvent{}, err
			}
			if (typ.T == ethabi.UintTy || typ.T == ethabi.IntTy) && (typ.Size < 8 || typ.Size > 256 || typ.Size%8 != 0) {
				return ParsedEvent{}, fmt.Errorf("unsupported type %q", fields[0])
			}
			args = append(args, ethabi.Argument{Name: fields[pos], Type: typ, Indexed: indexed})
			names, flags = append(names, fields[pos]), append(flags, indexed)
		}
	}
	event := ethabi.NewEvent(name, name, false, args)
	return ParsedEvent{Name: name, Event: event, Topic0: event.ID.Hex(), ParamNames: names, IndexedFlags: flags}, nil
}

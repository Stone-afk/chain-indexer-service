package domain

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRPCDTOsUseStandardTypes(t *testing.T) {
	var visit func(reflect.Type)
	visit = func(typ reflect.Type) {
		if typ.PkgPath() != "" && typ.PkgPath() != "encoding/json" && typ.PkgPath() != "github.com/Stone-afk/chain-indexer-service/internal/domain" {
			t.Errorf("foreign RPC type: %v", typ)
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice:
			visit(typ.Elem())
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				visit(typ.Field(i).Type)
			}
		}
	}
	for _, dto := range []any{BlockHeader{}, Block{}, Transaction{}, Receipt{}, Log{}} {
		visit(reflect.TypeOf(dto))
	}
	// Raw payloads must remain lossless JSON rather than parsed maps or geth objects.
	for name, dto := range map[string]any{"RawBlock": Block{}, "RawTransaction": Transaction{}, "RawReceipt": Receipt{}, "RawLog": Log{}} {
		field, ok := reflect.TypeOf(dto).FieldByName(name)
		if !ok || field.Type != reflect.TypeOf(json.RawMessage{}) {
			t.Errorf("%T.%s must be json.RawMessage", dto, name)
		}
	}
}

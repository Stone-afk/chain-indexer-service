package postgre

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

func numericFromUint256(value domain.Uint256) string { return value.String() }

func uint256FromNumeric(value string) (domain.Uint256, error) {
	return domain.ParseUint256Decimal(value)
}

type jsonDocument json.RawMessage

func newJSONDocument(value json.RawMessage) (jsonDocument, error) {
	if !json.Valid(value) {
		return nil, fmt.Errorf("invalid JSON document")
	}
	return jsonDocument(append([]byte(nil), value...)), nil
}

func (d jsonDocument) RawMessage() json.RawMessage {
	return append(json.RawMessage(nil), d...)
}

func (d jsonDocument) Value() (driver.Value, error) {
	if !json.Valid(d) {
		return nil, fmt.Errorf("invalid JSON document")
	}
	return string(d), nil
}

func (d *jsonDocument) Scan(value any) error {
	var raw []byte
	switch value := value.(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		return fmt.Errorf("scan JSON document")
	}
	parsed, err := newJSONDocument(raw)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

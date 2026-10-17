package xsql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type JSONField json.RawMessage

func NewJSONField(value json.RawMessage) (JSONField, error) {
	if !json.Valid(value) {
		return nil, fmt.Errorf("invalid JSON document")
	}
	return JSONField(append([]byte(nil), value...)), nil
}

func (d JSONField) RawMessage() json.RawMessage {
	return append(json.RawMessage(nil), d...)
}

func (d JSONField) Value() (driver.Value, error) {
	if !json.Valid(d) {
		return nil, fmt.Errorf("invalid JSON document")
	}
	return string(d), nil
}

func (d *JSONField) Scan(value any) error {
	var raw []byte
	switch value := value.(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		return fmt.Errorf("scan JSON document")
	}
	parsed, err := NewJSONField(raw)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

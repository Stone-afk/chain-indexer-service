package xsql

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
)

func TestJSONFieldRejectsInvalidJSON(t *testing.T) {
	if _, err := NewJSONField(json.RawMessage(`{"valid":`)); err == nil {
		t.Fatal("NewJSONField() accepted invalid JSON")
	}
	document, err := NewJSONField(json.RawMessage(`{"valid":true}`))
	if err != nil {
		t.Fatalf("NewJSONField() error = %v", err)
	}
	if string(document.RawMessage()) != `{"valid":true}` {
		t.Fatalf("RawMessage() = %q", document.RawMessage())
	}
}

func TestJSONFieldValueAndScan(t *testing.T) {
	document, err := NewJSONField(json.RawMessage(`{"event":"transfer"}`))
	if err != nil {
		t.Fatal(err)
	}
	value, err := document.Value()
	if err != nil {
		t.Fatal(err)
	}
	if value != driver.Value(`{"event":"transfer"}`) {
		t.Fatalf("Value() = %#v", value)
	}

	for _, value := range []any{[]byte(`{"event":"transfer"}`), `{"event":"transfer"}`} {
		var scanned JSONField
		if err := scanned.Scan(value); err != nil {
			t.Fatalf("Scan(%T) error = %v", value, err)
		}
		if string(scanned) != `{"event":"transfer"}` {
			t.Fatalf("Scan(%T) = %q", value, scanned)
		}
	}
	var scanned JSONField
	if err := scanned.Scan(nil); err == nil {
		t.Fatal("Scan(nil) succeeded")
	}
}

package postgre

import (
	"encoding/json"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

func TestNumericUint256RoundTrip(t *testing.T) {
	const maximum = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	value, err := domain.ParseUint256Decimal(maximum)
	if err != nil {
		t.Fatal(err)
	}
	stored := numericFromUint256(value)
	if stored != maximum {
		t.Fatalf("numericFromUint256() = %q", stored)
	}
	roundTrip, err := uint256FromNumeric(stored)
	if err != nil {
		t.Fatalf("uint256FromNumeric() error = %v", err)
	}
	if roundTrip.String() != maximum {
		t.Fatalf("round trip = %q", roundTrip.String())
	}
}

func TestJSONDocumentRejectsInvalidJSON(t *testing.T) {
	if _, err := newJSONDocument(json.RawMessage(`{"valid":`)); err == nil {
		t.Fatal("newJSONDocument() accepted invalid JSON")
	}
	document, err := newJSONDocument(json.RawMessage(`{"valid":true}`))
	if err != nil {
		t.Fatalf("newJSONDocument() error = %v", err)
	}
	if string(document.RawMessage()) != `{"valid":true}` {
		t.Fatalf("RawMessage() = %q", document.RawMessage())
	}
}

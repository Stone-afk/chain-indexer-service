package domain

import (
	"reflect"
	"testing"
)

const maxUint256Decimal = "115792089237316195423570985008687907853269984665640564039457584007913129639935"

func TestParseUint256DecimalAcceptsBounds(t *testing.T) {
	for _, input := range []string{"0", maxUint256Decimal} {
		value, err := ParseUint256Decimal(input)
		if err != nil {
			t.Fatalf("ParseUint256Decimal(%q) error = %v", input, err)
		}
		if got := value.String(); got != input {
			t.Fatalf("ParseUint256Decimal(%q).String() = %q", input, got)
		}
	}
}

func TestParseUint256DecimalRejectsInvalidValues(t *testing.T) {
	tooLarge := "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	for _, input := range []string{"", "-1", "1.5", "not-a-number", tooLarge} {
		t.Run(input, func(t *testing.T) {
			if _, err := ParseUint256Decimal(input); err == nil {
				t.Fatalf("ParseUint256Decimal(%q) succeeded", input)
			}
		})
	}
}

func TestUint256DoesNotExposeMutableState(t *testing.T) {
	typeOfValue := reflect.TypeOf(Uint256{})
	for i := 0; i < typeOfValue.NumField(); i++ {
		if typeOfValue.Field(i).IsExported() {
			t.Fatalf("Uint256 field %q is exported", typeOfValue.Field(i).Name)
		}
	}
}

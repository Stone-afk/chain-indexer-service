package domain

import (
	"fmt"
	"math/big"
)

// Uint256 stores an unsigned 256-bit integer without exposing mutable state.
type Uint256 struct {
	value big.Int
}

// ParseUint256Decimal parses a base-10 unsigned 256-bit integer.
func ParseUint256Decimal(input string) (Uint256, error) {
	if input == "" {
		return Uint256{}, fmt.Errorf("uint256 decimal value is required")
	}
	for _, digit := range input {
		if digit < '0' || digit > '9' {
			return Uint256{}, fmt.Errorf("invalid uint256 decimal value")
		}
	}

	parsed, ok := new(big.Int).SetString(input, 10)
	if !ok || parsed.BitLen() > 256 {
		return Uint256{}, fmt.Errorf("uint256 decimal value is out of range")
	}
	return Uint256{value: *parsed}, nil
}

func (v Uint256) String() string {
	return v.value.String()
}

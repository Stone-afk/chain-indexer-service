package abi

import "testing"

func TestParseEventDeclaration(t *testing.T) {
	tests := []struct {
		name, declaration, topic string
		params                   []string
		indexed                  []bool
	}{
		{"ERC20 Transfer", "event Transfer(address indexed from, address indexed to, uint256 value)", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", []string{"from", "to", "value"}, []bool{true, true, false}},
		{"ERC721 Transfer", "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", []string{"from", "to", "tokenId"}, []bool{true, true, true}},
		{"Approval", "event Approval(address indexed owner, address indexed spender, uint256 value)", "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925", []string{"owner", "spender", "value"}, []bool{true, true, false}},
		{"ApprovalForAll", "event ApprovalForAll(address indexed owner, address indexed operator, bool approved)", "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31", []string{"owner", "operator", "approved"}, []bool{true, true, false}},
		{"zero params", "event Paused()", "0x9e87fac88ff661f02d44f95383c817fece4bce600a3dab7a54406878b965e752", []string{}, []bool{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseEventDeclaration(tt.declaration)
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != eventName(tt.declaration) {
				t.Errorf("Name = %q", got.Name)
			}
			if got.Topic0 != tt.topic {
				t.Errorf("Topic0 = %q", got.Topic0)
			}
			if len(got.ParamNames) != len(tt.params) || len(got.IndexedFlags) != len(tt.indexed) {
				t.Fatalf("metadata lengths = %d, %d", len(got.ParamNames), len(got.IndexedFlags))
			}
			for i := range tt.params {
				if got.ParamNames[i] != tt.params[i] || got.IndexedFlags[i] != tt.indexed[i] {
					t.Errorf("parameter %d = %q/%v", i, got.ParamNames[i], got.IndexedFlags[i])
				}
			}
		})
	}
	for _, declaration := range []string{"", "Transfer(address indexed from)", "event Transfer(address indexed)", "event Transfer(address indexed from);", "event Transfer(address memory from)", "event Transfer(address indexed from,)", "event Transfer(uint999 value)"} {
		t.Run("reject "+declaration, func(t *testing.T) {
			if _, err := ParseEventDeclaration(declaration); err == nil {
				t.Errorf("accepted %q", declaration)
			}
		})
	}
	for _, declaration := range []string{"event Transfer!(address from)", "event Transfer(address from!)", "event Transfer(address indexed from) anonymous"} {
		t.Run("reject identifier/anonymous "+declaration, func(t *testing.T) {
			if _, err := ParseEventDeclaration(declaration); err == nil {
				t.Errorf("accepted %q", declaration)
			}
		})
	}
	if _, err := ParseEventDeclaration("  event Ping(address indexed sender)  "); err != nil {
		t.Fatalf("outer whitespace rejected: %v", err)
	}
}

func eventName(s string) string {
	start := len("event ")
	for i := start; i < len(s); i++ {
		if s[i] == '(' {
			return s[start:i]
		}
	}
	return ""
}

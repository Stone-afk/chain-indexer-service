package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestStartBlockExplicitZeroIsValid(t *testing.T) {
	path := writeConfig(t, validYAML(""))

	if _, err := LoadForService(path, testLookupEnv); err != nil {
		t.Fatalf("LoadForService() error = %v, want explicit startBlock: 0 to be valid", err)
	}
}

func TestMissingStartBlockIsInvalid(t *testing.T) {
	path := writeConfig(t, strings.Replace(validYAML(""), "    startBlock: 0\n", "", 1))

	if _, err := LoadForService(path, testLookupEnv); err == nil {
		t.Fatal("LoadForService() succeeded without startBlock")
	}
}

func TestServiceRejectsNegativeStartBlock(t *testing.T) {
	path := writeConfig(t, strings.Replace(validYAML(""), "    startBlock: 0\n", "    startBlock: -1\n", 1))

	if _, err := LoadForService(path, testLookupEnv); err == nil {
		t.Fatal("LoadForService() succeeded with a negative startBlock")
	}
}

func TestDuplicateChainIDIsInvalid(t *testing.T) {
	path := writeConfig(t, strings.Replace(validYAML(""), "    chainId: 56\n", "    chainId: 1\n", 1))

	if _, err := LoadForService(path, testLookupEnv); err == nil {
		t.Fatal("LoadForService() succeeded with duplicate enabled chain IDs")
	}
}

func TestServiceRequiresAtLeastOneEnabledChain(t *testing.T) {
	path := writeConfig(t, stringsReplace(validYAML(""), "enabled: true", "enabled: false"))

	if _, err := LoadForService(path, testLookupEnv); err == nil {
		t.Fatal("LoadForService() succeeded with no enabled chains")
	}
}

func TestServiceAcceptsHTTPAndWSURLsWithoutInfuraKey(t *testing.T) {
	path := writeConfig(t, selfHostedYAML())

	config, err := LoadForService(path, func(name string) (string, bool) {
		if name == "POSTGRES_DSN" {
			return testDSN, true
		}
		return "", false
	})
	if err != nil {
		t.Fatalf("LoadForService() error = %v", err)
	}
	if config.Chains["ethereum"].HTTPURL != "http://127.0.0.1:8545" || config.Chains["ethereum"].WSURL != "ws://127.0.0.1:8546" {
		t.Fatalf("ethereum URLs = (%q, %q)", config.Chains["ethereum"].HTTPURL, config.Chains["ethereum"].WSURL)
	}
}

func TestServiceRejectsInvalidHTTPScheme(t *testing.T) {
	path := writeConfig(t, strings.Replace(selfHostedYAML(), "http://127.0.0.1:8545", "ftp://127.0.0.1:8545", 1))

	if _, err := LoadForService(path, testLookupEnv); err == nil {
		t.Fatal("LoadForService() succeeded with an FTP HTTP URL")
	}
}

func TestServiceRejectsInvalidWSScheme(t *testing.T) {
	path := writeConfig(t, strings.Replace(selfHostedYAML(), "ws://127.0.0.1:8546", "https://127.0.0.1:8546", 1))

	if _, err := LoadForService(path, testLookupEnv); err == nil {
		t.Fatal("LoadForService() succeeded with an HTTPS WebSocket URL")
	}
}

func TestBuiltinChainBaseline(t *testing.T) {
	config, err := LoadForService(filepath.Join("..", "..", "config.example.yaml"), testLookupEnv)
	if err != nil {
		t.Fatalf("LoadForService(config.example.yaml) error = %v", err)
	}

	want := map[string]struct {
		chainID       int64
		confirmBlocks int64
		httpURL       string
		wsURL         string
		maxReorgDepth int64
	}{
		"ethereum":  {chainID: 1, confirmBlocks: 12, maxReorgDepth: 64, httpURL: "https://mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://mainnet.infura.io/ws/v3/" + testAPIKey},
		"bsc":       {chainID: 56, confirmBlocks: 15, maxReorgDepth: 32, httpURL: "https://bsc-mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://bsc-mainnet.infura.io/ws/v3/" + testAPIKey},
		"polygon":   {chainID: 137, confirmBlocks: 100, maxReorgDepth: 512, httpURL: "https://polygon-mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://polygon-mainnet.infura.io/ws/v3/" + testAPIKey},
		"arbitrum":  {chainID: 42161, confirmBlocks: 20, maxReorgDepth: 128, httpURL: "https://arbitrum-mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://arbitrum-mainnet.infura.io/ws/v3/" + testAPIKey},
		"optimism":  {chainID: 10, confirmBlocks: 20, maxReorgDepth: 450, httpURL: "https://optimism-mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://optimism-mainnet.infura.io/ws/v3/" + testAPIKey},
		"base":      {chainID: 8453, confirmBlocks: 20, maxReorgDepth: 600, httpURL: "https://base-mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://base-mainnet.infura.io/ws/v3/" + testAPIKey},
		"avalanche": {chainID: 43114, confirmBlocks: 15, maxReorgDepth: 128, httpURL: "https://avalanche-mainnet.infura.io/v3/" + testAPIKey, wsURL: "wss://avalanche-mainnet.infura.io/ws/v3/" + testAPIKey},
	}
	if len(config.Chains) != len(want) {
		t.Fatalf("configured chains = %d, want %d", len(config.Chains), len(want))
	}
	for name, expected := range want {
		chain, ok := config.Chains[name]
		if !ok {
			t.Errorf("missing built-in chain %q", name)
			continue
		}
		if chain.ChainID != expected.chainID || chain.ConfirmBlocks != expected.confirmBlocks || chain.MaxReorgDepth != expected.maxReorgDepth || chain.HTTPURL != expected.httpURL || chain.WSURL != expected.wsURL {
			t.Errorf("%s = (chainID=%d, confirmBlocks=%d, httpURL=%q, wsURL=%q), want (%d, %d, %q, %q)", name, chain.ChainID, chain.ConfirmBlocks, chain.HTTPURL, chain.WSURL, expected.chainID, expected.confirmBlocks, expected.httpURL, expected.wsURL)
		}
	}
}

func TestMaxReorgDepthValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		want int64
		ok   bool
	}{
		{"omitted", "", 0, true}, {"zero", "    maxReorgDepth: 0\n", 0, true},
		{"negative", "    maxReorgDepth: -1\n", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, validYAML(tc.yaml))
			cfg, err := LoadForService(path, testLookupEnv)
			if tc.ok != (err == nil) {
				t.Fatalf("error=%v, want success=%v", err, tc.ok)
			}
			if tc.ok && cfg.Chains["ethereum"].MaxReorgDepth != tc.want {
				t.Fatalf("depth=%d", cfg.Chains["ethereum"].MaxReorgDepth)
			}
		})
	}
}

func TestDisabledChainRejectsNegativeMaxReorgDepth(t *testing.T) {
	contents := strings.Replace(validYAML(""), "enabled: true", "enabled: false", 1)
	contents = strings.Replace(contents, "    confirmBlocks: 12\n", "    confirmBlocks: 12\n    maxReorgDepth: -1\n", 1)
	if _, err := LoadForService(writeConfig(t, contents), testLookupEnv); err == nil {
		t.Fatal("accepted negative maxReorgDepth on disabled chain")
	}
}

func TestServiceRejectsConfigOutsideBuiltinBaseline(t *testing.T) {
	bscConfig := `  bsc:
    enabled: true
    chainId: 56
    startBlock: 0
    httpUrl: https://bsc-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://bsc-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 15
`
	tests := []struct {
		name     string
		contents string
	}{
		{
			name:     "missing built-in chain",
			contents: strings.Replace(validYAML(""), bscConfig, "", 1),
		},
		{
			name: "unexpected chain",
			contents: validYAML(`  custom:
    enabled: true
    chainId: 999
    startBlock: 0
    httpUrl: https://custom.example.test
    wsUrl: wss://custom.example.test
    confirmBlocks: 1
`),
		},
		{
			name:     "wrong chain ID",
			contents: strings.Replace(validYAML(""), "    chainId: 1\n", "    chainId: 2\n", 1),
		},
		{
			name:     "wrong confirmation blocks",
			contents: strings.Replace(validYAML(""), "    confirmBlocks: 12\n", "    confirmBlocks: 13\n", 1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeConfig(t, test.contents)
			if _, err := LoadForService(path, testLookupEnv); err == nil {
				t.Fatal("LoadForService() succeeded outside the built-in baseline")
			}
		})
	}
}

func selfHostedYAML() string {
	lines := strings.Split(validYAML(""), "\n")
	for index, line := range lines {
		switch {
		case strings.Contains(line, "httpUrl:"):
			lines[index] = "    httpUrl: http://127.0.0.1:8545"
		case strings.Contains(line, "wsUrl:"):
			lines[index] = "    wsUrl: ws://127.0.0.1:8546"
		}
	}
	return strings.Join(lines, "\n")
}

func stringsReplace(s, old, new string) string {
	return strings.NewReplacer(old, new).Replace(s)
}

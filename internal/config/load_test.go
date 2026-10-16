package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testDSN = "postgres://tester:dsn-secret@db.example.test:5432/indexer"
const testAPIKey = "infura-api-key-secret"

func TestLoadForServiceExpandsEnvironment(t *testing.T) {
	path := writeConfig(t, validYAML(""))

	config, err := LoadForService(path, testLookupEnv)
	if err != nil {
		t.Fatalf("LoadForService() error = %v", err)
	}
	if config.Database.DSN != testDSN {
		t.Errorf("DSN = %q, want expanded value", config.Database.DSN)
	}
	chain := config.Chains["ethereum"]
	if !strings.Contains(chain.HTTPURL, testAPIKey) || !strings.Contains(chain.WSURL, testAPIKey) {
		t.Errorf("chain URLs = (%q, %q), want expanded API key", chain.HTTPURL, chain.WSURL)
	}
}

func TestLoadForServiceRejectsUnknownYAMLField(t *testing.T) {
	path := writeConfig(t, validYAML("    confirmBlockz: 12\n"))

	_, err := LoadForService(path, testLookupEnv)
	if err == nil {
		t.Fatal("LoadForService() succeeded with an unknown YAML field")
	}
}

func TestLoadersRejectMultipleYAMLDocuments(t *testing.T) {
	servicePath := writeConfig(t, validYAML("")+"---\ndatabase:\n  dsn: ignored\n")
	if _, err := LoadForService(servicePath, testLookupEnv); err == nil {
		t.Fatal("LoadForService() accepted multiple YAML documents")
	}

	migrationPath := writeConfig(t, "database:\n  dsn: ${POSTGRES_DSN}\n---\ndatabase:\n  dsn: ignored\n")
	if _, err := LoadForMigration(migrationPath, testLookupEnv); err == nil {
		t.Fatal("LoadForMigration() accepted multiple YAML documents")
	}
}

func TestLoadForServiceRejectsMissingEnvironmentVariable(t *testing.T) {
	path := writeConfig(t, validYAML(""))

	_, err := LoadForService(path, func(name string) (string, bool) {
		if name == "POSTGRES_DSN" {
			return testDSN, true
		}
		return "", false
	})
	if err == nil {
		t.Fatal("LoadForService() succeeded without INFURA_API_KEY")
	}
	if !strings.Contains(err.Error(), "INFURA_API_KEY") {
		t.Errorf("error = %q, want missing variable name", err)
	}
	if strings.Contains(err.Error(), testDSN) || strings.Contains(err.Error(), testAPIKey) {
		t.Errorf("error = %q leaks a secret", err)
	}
}

func TestLoadersRejectResidualEnvironmentPlaceholders(t *testing.T) {
	tests := []struct {
		name   string
		dsn    string
		lookup func(string) (string, bool)
	}{
		{
			name: "invalid variable name",
			dsn:  "${POSTGRES-DSN}",
			lookup: func(name string) (string, bool) {
				return testLookupEnv(name)
			},
		},
		{
			name: "empty variable name",
			dsn:  "${}",
			lookup: func(name string) (string, bool) {
				return testLookupEnv(name)
			},
		},
		{
			name: "nested replacement",
			dsn:  "${POSTGRES_DSN}",
			lookup: func(name string) (string, bool) {
				if name == "POSTGRES_DSN" {
					return "${NESTED}", true
				}
				return testLookupEnv(name)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			servicePath := writeConfig(t, strings.Replace(validYAML(""), "${POSTGRES_DSN}", test.dsn, 1))
			if _, err := LoadForService(servicePath, test.lookup); err == nil {
				t.Fatal("LoadForService() accepted a residual placeholder")
			} else if strings.Contains(err.Error(), testDSN) || strings.Contains(err.Error(), testAPIKey) {
				t.Fatalf("LoadForService() error leaked a secret: %v", err)
			}

			migrationPath := writeConfig(t, "database:\n  dsn: \""+test.dsn+"\"\n")
			if _, err := LoadForMigration(migrationPath, test.lookup); err == nil {
				t.Fatal("LoadForMigration() accepted a residual placeholder")
			} else if strings.Contains(err.Error(), testDSN) || strings.Contains(err.Error(), testAPIKey) {
				t.Fatalf("LoadForMigration() error leaked a secret: %v", err)
			}
		})
	}
}

func TestLoadForMigrationDoesNotRequireInfuraKey(t *testing.T) {
	path := writeConfig(t, validYAML(""))

	database, err := LoadForMigration(path, func(name string) (string, bool) {
		if name == "POSTGRES_DSN" {
			return testDSN, true
		}
		return "", false
	})
	if err != nil {
		t.Fatalf("LoadForMigration() error = %v", err)
	}
	if database.DSN != testDSN {
		t.Errorf("DSN = %q, want expanded value", database.DSN)
	}
}

func TestLoadForMigrationDoesNotValidateChains(t *testing.T) {
	path := writeConfig(t, strings.Replace(validYAML(""), "confirmBlocks: 12", "confirmBlocks: invalid", 1))

	database, err := LoadForMigration(path, func(name string) (string, bool) {
		if name == "POSTGRES_DSN" {
			return testDSN, true
		}
		return "", false
	})
	if err != nil {
		t.Fatalf("LoadForMigration() error = %v, want chain settings ignored", err)
	}
	if database.DSN != testDSN {
		t.Errorf("DSN = %q, want expanded value", database.DSN)
	}
}

func TestLoadErrorRedactsSecrets(t *testing.T) {
	path := writeConfig(t, validYAML("    confirmBlocks: invalid\n"))

	_, err := LoadForService(path, testLookupEnv)
	if err == nil {
		t.Fatal("LoadForService() succeeded with invalid YAML value")
	}
	for _, secret := range []string{
		testDSN,
		testAPIKey,
		"https://mainnet.infura.io/v3/" + testAPIKey,
	} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error = %q leaks %q", err, secret)
		}
	}
}

func validYAML(extra string) string {
	return "database:\n  dsn: ${POSTGRES_DSN}\nchains:\n" + builtinChainsYAML + extra
}

const builtinChainsYAML = `  ethereum:
    enabled: true
    chainId: 1
    startBlock: 0
    httpUrl: https://mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 12
  bsc:
    enabled: true
    chainId: 56
    startBlock: 0
    httpUrl: https://bsc-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://bsc-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 15
  polygon:
    enabled: true
    chainId: 137
    startBlock: 0
    httpUrl: https://polygon-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://polygon-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 100
  arbitrum:
    enabled: true
    chainId: 42161
    startBlock: 0
    httpUrl: https://arbitrum-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://arbitrum-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 20
  optimism:
    enabled: true
    chainId: 10
    startBlock: 0
    httpUrl: https://optimism-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://optimism-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 20
  base:
    enabled: true
    chainId: 8453
    startBlock: 0
    httpUrl: https://base-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://base-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 20
  avalanche:
    enabled: true
    chainId: 43114
    startBlock: 0
    httpUrl: https://avalanche-mainnet.infura.io/v3/${INFURA_API_KEY}
    wsUrl: wss://avalanche-mainnet.infura.io/ws/v3/${INFURA_API_KEY}
    confirmBlocks: 15
`

func testLookupEnv(name string) (string, bool) {
	switch name {
	case "POSTGRES_DSN":
		return testDSN, true
	case "INFURA_API_KEY":
		return testAPIKey, true
	default:
		return "", false
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

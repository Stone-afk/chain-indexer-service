package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

var environmentVariable = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
var residualEnvironmentVariable = regexp.MustCompile(`\$\{[^}]*\}`)

// LoadForService loads the complete configuration required by the service.
func LoadForService(path string, lookupEnv func(string) (string, bool)) (*Config, error) {
	raw, err := loadRaw(path)
	if err != nil {
		return nil, err
	}

	dsn, err := expandEnvironment(raw.Database.DSN, lookupEnv)
	if err != nil {
		return nil, err
	}
	raw.Database.DSN = dsn

	for name, chain := range raw.Chains {
		chain.HTTPURL, err = expandEnvironment(chain.HTTPURL, lookupEnv)
		if err != nil {
			return nil, err
		}
		chain.WSURL, err = expandEnvironment(chain.WSURL, lookupEnv)
		if err != nil {
			return nil, err
		}
		raw.Chains[name] = chain
	}

	if err := validateRawService(raw); err != nil {
		return nil, err
	}

	return normalize(raw), nil
}

// LoadForMigration loads only the database configuration required by migration.
func LoadForMigration(path string, lookupEnv func(string) (string, bool)) (*DatabaseConfig, error) {
	raw, err := loadRawMigration(path)
	if err != nil {
		return nil, err
	}

	dsn, err := expandEnvironment(raw.Database.DSN, lookupEnv)
	if err != nil {
		return nil, err
	}
	database := DatabaseConfig{DSN: dsn}
	if err := validateDatabase(database); err != nil {
		return nil, err
	}
	return &database, nil
}

func loadRawMigration(path string) (rawMigrationConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return rawMigrationConfig{}, fmt.Errorf("read configuration file: %w", err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	var raw rawMigrationConfig
	if err := decoder.Decode(&raw); err != nil {
		return rawMigrationConfig{}, fmt.Errorf("invalid YAML configuration")
	}
	if err := rejectTrailingYAMLDocument(decoder); err != nil {
		return rawMigrationConfig{}, err
	}
	return raw, nil
}

func loadRaw(path string) (rawConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return rawConfig{}, fmt.Errorf("read configuration file: %w", err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	var raw rawConfig
	if err := decoder.Decode(&raw); err != nil {
		return rawConfig{}, fmt.Errorf("invalid YAML configuration")
	}
	if err := rejectTrailingYAMLDocument(decoder); err != nil {
		return rawConfig{}, err
	}
	return raw, nil
}

func rejectTrailingYAMLDocument(decoder *yaml.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("invalid YAML configuration")
	}
	return nil
}

func expandEnvironment(value string, lookupEnv func(string) (string, bool)) (string, error) {
	var missing string
	expanded := environmentVariable.ReplaceAllStringFunc(value, func(match string) string {
		name := environmentVariable.FindStringSubmatch(match)[1]
		replacement, ok := lookupEnv(name)
		if !ok {
			missing = name
			return match
		}
		return replacement
	})
	if missing != "" {
		return "", fmt.Errorf("required environment variable %q is not set", missing)
	}
	if residualEnvironmentVariable.MatchString(expanded) {
		return "", fmt.Errorf("configuration contains an unresolved environment placeholder")
	}
	return expanded, nil
}

func normalize(raw rawConfig) *Config {
	config := &Config{
		Database: DatabaseConfig{DSN: raw.Database.DSN},
		Chains:   make(map[string]ChainConfig, len(raw.Chains)),
	}
	for name, chain := range raw.Chains {
		startBlock := int64(0)
		if chain.StartBlock != nil {
			startBlock = *chain.StartBlock
		}
		config.Chains[name] = ChainConfig{
			Enabled:       chain.Enabled,
			ChainID:       chain.ChainID,
			StartBlock:    startBlock,
			HTTPURL:       chain.HTTPURL,
			WSURL:         chain.WSURL,
			ConfirmBlocks: chain.ConfirmBlocks,
			MaxReorgDepth: chain.MaxReorgDepth,
		}
	}
	return config
}

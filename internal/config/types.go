// Package config loads and validates chain-indexer configuration.
package config

// Config is the normalized configuration needed by the service.
type Config struct {
	Database DatabaseConfig
	Chains   map[string]ChainConfig
}

// DatabaseConfig holds the database connection configuration.
type DatabaseConfig struct {
	DSN string
}

// ChainConfig holds the normalized configuration for one chain.
type ChainConfig struct {
	Enabled       bool
	ChainID       int64
	StartBlock    int64
	HTTPURL       string
	WSURL         string
	ConfirmBlocks int64
	MaxReorgDepth int64
}

type rawConfig struct {
	Database rawDatabaseConfig         `yaml:"database"`
	Chains   map[string]rawChainConfig `yaml:"chains"`
}

type rawMigrationConfig struct {
	Database rawDatabaseConfig `yaml:"database"`
	Chains   any               `yaml:"chains"`
}

type rawDatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

type rawChainConfig struct {
	Enabled       bool   `yaml:"enabled"`
	ChainID       int64  `yaml:"chainId"`
	StartBlock    *int64 `yaml:"startBlock"`
	HTTPURL       string `yaml:"httpUrl"`
	WSURL         string `yaml:"wsUrl"`
	ConfirmBlocks int64  `yaml:"confirmBlocks"`
	MaxReorgDepth int64  `yaml:"maxReorgDepth"`
}

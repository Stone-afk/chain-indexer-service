package config

import (
	"fmt"
	"net/url"
	"strings"
)

type builtinChain struct {
	chainID       int64
	confirmBlocks int64
}

var builtinChains = map[string]builtinChain{
	"ethereum":  {chainID: 1, confirmBlocks: 12},
	"bsc":       {chainID: 56, confirmBlocks: 15},
	"polygon":   {chainID: 137, confirmBlocks: 100},
	"arbitrum":  {chainID: 42161, confirmBlocks: 20},
	"optimism":  {chainID: 10, confirmBlocks: 20},
	"base":      {chainID: 8453, confirmBlocks: 20},
	"avalanche": {chainID: 43114, confirmBlocks: 15},
}

func validateDatabase(database DatabaseConfig) error {
	if strings.TrimSpace(database.DSN) == "" {
		return fmt.Errorf("database DSN is required")
	}
	return nil
}

func validateRawService(raw rawConfig) error {
	if err := validateDatabase(DatabaseConfig{DSN: raw.Database.DSN}); err != nil {
		return err
	}
	if err := validateBuiltinChains(raw.Chains); err != nil {
		return err
	}

	enabledCount := 0
	for name, chain := range raw.Chains {
		if chain.ConfirmBlocks <= 0 {
			return fmt.Errorf("chain %q must have positive confirmation blocks", name)
		}
		if err := validateURL(name, "HTTP", chain.HTTPURL, "http", "https"); err != nil {
			return err
		}
		if err := validateURL(name, "WebSocket", chain.WSURL, "ws", "wss"); err != nil {
			return err
		}
		if chain.Enabled {
			enabledCount++
			if chain.StartBlock == nil || *chain.StartBlock < 0 {
				return fmt.Errorf("enabled chain %q must explicitly set a non-negative startBlock", name)
			}
		}
	}
	if enabledCount == 0 {
		return fmt.Errorf("service requires at least one enabled chain")
	}
	return nil
}

func validateBuiltinChains(chains map[string]rawChainConfig) error {
	if len(chains) != len(builtinChains) {
		return fmt.Errorf("service configuration must contain exactly the built-in chains")
	}
	for name, expected := range builtinChains {
		chain, ok := chains[name]
		if !ok {
			return fmt.Errorf("service configuration is missing built-in chain %q", name)
		}
		if chain.ChainID != expected.chainID {
			return fmt.Errorf("chain %q does not match the built-in chain ID", name)
		}
		if chain.ConfirmBlocks != expected.confirmBlocks {
			return fmt.Errorf("chain %q does not match the built-in confirmation blocks", name)
		}
	}
	return nil
}

func validateURL(chainName, label, value string, allowedSchemes ...string) error {
	parsed, err := url.ParseRequestURI(value)
	if err == nil && parsed.Host != "" {
		for _, scheme := range allowedSchemes {
			if parsed.Scheme == scheme {
				return nil
			}
		}
	}
	return fmt.Errorf("chain %q %s URL must use %s", chainName, label, strings.Join(allowedSchemes, " or "))
}

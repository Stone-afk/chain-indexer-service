//go:build e2e

package testioc

import (
	"fmt"
	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"os"
	"path/filepath"
	"testing"
)

// WriteServiceConfig writes an isolated service configuration for a fixture.
func WriteServiceConfig(t testing.TB, db config.DatabaseConfig, chains map[string]config.ChainConfig) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	s := fmt.Sprintf("database:\n  dsn: %q\nchains:\n", db.DSN)
	for name, c := range chains {
		s += fmt.Sprintf("  %s:\n    enabled: %t\n    chainId: %d\n    startBlock: %d\n    httpUrl: %q\n    wsUrl: %q\n    confirmBlocks: %d\n    maxReorgDepth: %d\n", name, c.Enabled, c.ChainID, c.StartBlock, c.HTTPURL, c.WSURL, c.ConfirmBlocks, c.MaxReorgDepth)
	}
	if err := os.WriteFile(path, []byte(s), 0600); err != nil {
		t.Fatalf("write service config: %v", err)
	}
	return path
}

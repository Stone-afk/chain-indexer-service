//go:build e2e

package testioc

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
)

func InitConfig(t testing.TB) *config.Config {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test IoC source")
	}
	path := filepath.Join(filepath.Dir(filename), "..", "..", "..", "config", "config.local.yaml")
	cfg, err := config.LoadForService(path, os.LookupEnv)
	if err != nil {
		t.Fatalf("load local e2e configuration: %v", err)
	}
	return cfg
}

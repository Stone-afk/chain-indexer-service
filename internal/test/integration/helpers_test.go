//go:build e2e

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gorm.io/gorm"
)

var businessTables = []string{
	"chain_checkpoints",
	"chain_blocks",
	"chain_raw_events",
	"chain_transactions",
	"chain_transfers",
	"chain_reorgs",
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", ".."))
}

func localConfigPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "config", "config.local.yaml")
}

func migrationDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "script", "postgre")
}

func buildBinary(t *testing.T, packagePath string) string {
	t.Helper()
	name := filepath.Base(packagePath)
	path := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-o", path, packagePath)
	command.Dir = repositoryRoot(t)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, output)
	}
	return path
}

func commandEnvironment(extra ...string) []string {
	values := make(map[string]string, len(extra))
	for _, entry := range extra {
		name, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		values[name] = value
	}
	environment := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, replaced := values[name]; !replaced {
			environment = append(environment, entry)
		}
	}
	for name, value := range values {
		environment = append(environment, name+"="+value)
	}
	return environment
}

func writeConfigWithDSN(t *testing.T, currentDSN, dsn string) string {
	t.Helper()
	contents, err := os.ReadFile(localConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	updated := strings.Replace(string(contents), currentDSN, dsn, 1)
	if updated == string(contents) {
		t.Fatal("local PostgreSQL DSN not found in config.local.yaml")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeMigrationConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "migration.yaml")
	if err := os.WriteFile(path, []byte("database:\n  dsn: ${POSTGRES_DSN}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeMigrationFile(t *testing.T, dir, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
		t.Fatalf("write migration %q: %v", name, err)
	}
}

func assertTableExists(t *testing.T, db *gorm.DB, table string, want bool) {
	t.Helper()
	var exists bool
	if err := db.Raw("SELECT to_regclass(?) IS NOT NULL", fmt.Sprintf("public.%s", table)).Scan(&exists).Error; err != nil {
		t.Fatalf("query table %q existence: %v", table, err)
	}
	if exists != want {
		t.Errorf("table %q exists = %t, want %t", table, exists, want)
	}
}

func hashOf(character byte) string    { return "0x" + strings.Repeat(string(character), 64) }
func addressOf(character byte) string { return "0x" + strings.Repeat(string(character), 40) }

//go:build e2e

package integration

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"gorm.io/gorm"
)

func TestMigrateCommandUpStatusDownUp(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	binary := buildBinary(t, "./cmd/migrate")

	firstUp := runMigrateCommand(t, binary, "up")
	assertLogLineContains(t, firstUp, "migration applied", "command=migrate", "direction=up", "migration_version=1", "migration_name=initial_schema")
	assertLogLineContains(t, firstUp, "migration applied", "command=migrate", "direction=up", "migration_version=2", "migration_name=event_biz_cursors")

	noOpUp := runMigrateCommand(t, binary, "up")
	if strings.Contains(noOpUp, "migration applied") || strings.Contains(noOpUp, "migration_version") {
		t.Fatalf("no-op up reported an applied migration: %q", noOpUp)
	}

	status := runMigrateCommand(t, binary, "status")
	assertLogLineContains(t, status, "000001", "initial_schema", "applied")
	assertLogLineContains(t, status, "000002", "event_biz_cursors", "applied")

	down := runMigrateCommand(t, binary, "down")
	assertLogLineContains(t, down, "migration rolled back", "command=migrate", "direction=down", "migration_version=2", "migration_name=event_biz_cursors")

	lastUp := runMigrateCommand(t, binary, "up")
	assertLogLineContains(t, lastUp, "migration applied", "command=migrate", "direction=up", "migration_version=2", "migration_name=event_biz_cursors")

	for _, output := range []string{firstUp, noOpUp, status, down, lastUp} {
		assertOutputOmitsSecrets(t, output, cfg.Database.DSN)
	}

	var versionCount, tableCount int64
	if err := db.Raw("SELECT COUNT(*) FROM schema_migrations WHERE version IN (1, 2)").Scan(&versionCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE 'chain_%'").Scan(&tableCount).Error; err != nil {
		t.Fatal(err)
	}
	if versionCount != 2 || tableCount != 7 {
		t.Fatalf("version count = %d, business tables = %d", versionCount, tableCount)
	}
}

func TestMigrateDatabaseFailureRedactsDSN(t *testing.T) {
	const secretDSN = "postgres://secret-user:secret-password@127.0.0.1:1/secret-db?sslmode=disable"
	binary := buildBinary(t, "./cmd/migrate")
	command := exec.Command(binary, "status", "--config", writeMigrationConfig(t), "--migrations-dir", migrationDir(t))
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment("POSTGRES_DSN=" + secretDSN)
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 1 {
		t.Fatal("migrate status succeeded with unreachable database")
	}
	for _, secret := range []string{secretDSN, "secret-password"} {
		if bytes.Contains(output, []byte(secret)) {
			t.Fatalf("output leaked %q: %q", secret, output)
		}
	}
}

func TestMigrateRejectsInvalidArguments(t *testing.T) {
	binary := buildBinary(t, "./cmd/migrate")
	command := exec.Command(binary, "unknown")
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 2 || !bytes.Contains(output, []byte(`unknown migration direction "unknown"`)) {
		t.Fatalf("migrate err = %v, output = %q", err, output)
	}
}

func TestMigrateRejectsInvalidConfig(t *testing.T) {
	binary := buildBinary(t, "./cmd/migrate")
	command := exec.Command(binary, "status", "--config", filepath.Join(t.TempDir(), "missing.yaml"))
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 1 || !bytes.Contains(output, []byte("load migration configuration:")) {
		t.Fatalf("migrate err = %v, output = %q", err, output)
	}
}

func TestChainIndexerRequiresConfig(t *testing.T) {
	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary)
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 2 || !strings.Contains(string(output), "usage: chain-indexer --config <path>") {
		t.Fatalf("chain-indexer err = %v, output = %q", err, output)
	}
}

func TestChainIndexerRejectsUnknownFlag(t *testing.T) {
	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary, "--unknown")
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 2 || !strings.Contains(string(output), "invalid service arguments") {
		t.Fatalf("chain-indexer err = %v, output = %q", err, output)
	}
}

func TestChainIndexerRejectsServeAndMigratePositionalArguments(t *testing.T) {
	binary := buildBinary(t, "./cmd/chain-indexer")
	for _, argument := range []string{"serve", "migrate"} {
		t.Run(argument, func(t *testing.T) {
			command := exec.Command(binary, argument, "--config", "ignored")
			command.Dir = repositoryRoot(t)
			command.Env = commandEnvironment()
			output, err := command.CombinedOutput()
			exitError, ok := err.(*exec.ExitError)
			if !ok || exitError.ExitCode() != 2 || !strings.Contains(string(output), "usage: chain-indexer --config <path>") {
				t.Fatalf("chain-indexer %s err = %v, output = %q", argument, err, output)
			}
		})
	}
}

func TestChainIndexerHelpListsConfigFlag(t *testing.T) {
	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary, "--help")
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "--config") {
		t.Fatalf("chain-indexer err = %v, output = %q", err, output)
	}
}

func TestChainIndexerRejectsInvalidConfigBeforeDatabase(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid-config.yaml")
	contents := []byte("database:\n  dsn: host=127.0.0.1 port=1 user=invalid password=invalid dbname=invalid sslmode=disable\nchains: {}\n")
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary, "--config", configPath)
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 1 {
		t.Fatalf("chain-indexer succeeded with invalid configuration: %q", output)
	}
	result := string(output)
	if !strings.Contains(result, "load service configuration:") || !strings.Contains(result, "service configuration must contain exactly the built-in chains") {
		t.Fatalf("chain-indexer output = %q", output)
	}
	if strings.Contains(result, "start service:") {
		t.Fatalf("chain-indexer attempted to start before rejecting its configuration: %q", output)
	}
}

func TestChainIndexerRejectsMissingSchema(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	const applicationName = "chain-indexer-e2e-missing-schema"
	configPath := writeConfigWithDSN(t, cfg.Database.DSN, cfg.Database.DSN+" application_name="+applicationName)
	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary, "--config", configPath)
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 1 || !bytes.Contains(output, []byte("schema migration table is missing")) {
		t.Fatalf("chain-indexer err = %v, output = %q", err, output)
	}
	if bytes.Contains(output, []byte("service ready")) {
		t.Fatalf("chain-indexer reported ready: %q", output)
	}
	if countApplicationConnections(t, db, applicationName) != 0 {
		t.Fatal("service database connection remains after schema gate failure")
	}
}

func TestChainIndexerRejectsBogusHigherSchemaVersion(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	createSchemaMigrationHistory(t, db, 2, "bogus")
	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary, "--config", localConfigPath(t))
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		exitError, ok := err.(*exec.ExitError)
		if !ok || exitError.ExitCode() != 1 {
			t.Fatalf("chain-indexer succeeded; stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
	case <-time.After(2 * time.Second):
		_ = command.Process.Kill()
		<-done
		t.Fatalf("chain-indexer did not reject bogus migration history; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "service ready") || strings.Contains(stderr.String(), "service ready") {
		t.Fatalf("chain-indexer reported ready; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestChainIndexerDatabaseFailureRedactsSecrets(t *testing.T) {
	const secretDSN = "postgres://secret-user:secret-password@127.0.0.1:1/secret-db?sslmode=disable"
	cfg := testioc.InitConfig(t)
	binary := buildBinary(t, "./cmd/chain-indexer")
	configPath := writeConfigWithDSN(t, cfg.Database.DSN, secretDSN)
	command := exec.Command(binary, "--config", configPath)
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment("INFURA_API_KEY=secret-api-key")
	output, err := command.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 1 {
		t.Fatal("chain-indexer succeeded with unreachable database")
	}
	for _, secret := range []string{secretDSN, "secret-password", "secret-api-key"} {
		if bytes.Contains(output, []byte(secret)) {
			t.Fatalf("output leaked %q: %q", secret, output)
		}
	}
}

func TestChainIndexerLifecycle(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}

	const applicationName = "chain-indexer-e2e-lifecycle"
	configPath := writeConfigWithDSN(t, cfg.Database.DSN, cfg.Database.DSN+" application_name="+applicationName)
	binary := buildBinary(t, "./cmd/chain-indexer")
	command := exec.Command(binary, "--config", configPath)
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{})
	doneReading := make(chan struct{})
	var output strings.Builder
	go func() {
		defer close(doneReading)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			output.WriteString(line)
			output.WriteByte('\n')
			if strings.Contains(line, "service ready") {
				select {
				case <-ready:
				default:
					close(ready)
				}
			}
		}
	}()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("service did not become ready; stdout=%q stderr=%q", output.String(), stderr.String())
	}
	if countApplicationConnections(t, db, applicationName) == 0 {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal("no live service database connection")
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	select {
	case err := <-wait:
		if err != nil {
			t.Fatalf("service exit: %v, stderr=%q", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("service did not stop after SIGINT")
	}
	<-doneReading
	if countApplicationConnections(t, db, applicationName) != 0 {
		t.Fatal("service database connection remains after shutdown")
	}
	combined := output.String() + stderr.String()
	assertLogLineContains(t, combined, "service ready", "command=chain-indexer")
	assertLogLineContains(t, combined, "service stopping", "command=chain-indexer")
	assertLogLineContains(t, combined, "service stopped", "command=chain-indexer")
	for _, forbidden := range []string{"indexing_active", "listener", "worker"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("service output contains forbidden value %q", forbidden)
		}
	}
	assertOutputOmitsSecrets(t, combined, cfg.Database.DSN)
}

func runMigrateCommand(t *testing.T, binary, direction string) string {
	t.Helper()
	args := []string{direction, "--config", localConfigPath(t), "--migrations-dir", migrationDir(t)}
	command := exec.Command(binary, args...)
	command.Dir = repositoryRoot(t)
	command.Env = commandEnvironment()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("migrate %s: %v\n%s", direction, err, output)
	}
	return string(output)
}

func assertLogLineContains(t *testing.T, output, message string, fields ...string) {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, message) {
			continue
		}
		matches := true
		for _, field := range fields {
			if !strings.Contains(line, field) {
				matches = false
				break
			}
		}
		if matches {
			return
		}
	}
	t.Fatalf("output has no log line containing %q and fields %q: %q", message, fields, output)
}

func assertOutputOmitsSecrets(t *testing.T, output, dsn string) {
	t.Helper()
	for _, forbidden := range []string{"secret", dsn, "http://127.0.0.1:8545", "ws://127.0.0.1:8546"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output contains forbidden value %q", forbidden)
		}
	}
}

func countApplicationConnections(t *testing.T, db *gorm.DB, name string) int64 {
	t.Helper()
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = ?", name).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

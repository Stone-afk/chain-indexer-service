package postgre

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

var migrationFilenamePattern = regexp.MustCompile(`^([0-9]{6})_([A-Za-z0-9][A-Za-z0-9_-]*)\.(up|down)\.sql$`)

const migrationAdvisoryLockKey int64 = 0x4348494e44584d47

const bootstrapSchemaMigrationsSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    applied_at BIGINT NOT NULL
)`

type Migration struct {
	Version  int64
	Name     string
	UpPath   string
	DownPath string
}

type MigrationStatus struct {
	Version int64
	Name    string
	Applied bool
}

type MigrationRunner struct {
	db         *sql.DB
	migrations []Migration
}

type migrationPair struct {
	migration Migration
	hasUp     bool
	hasDown   bool
}

func DiscoverMigrations(dir string) ([]Migration, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}

	pairs := make(map[int64]*migrationPair)
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}

		matches := migrationFilenamePattern.FindStringSubmatch(entry.Name())
		if matches == nil {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid migration version in %q", entry.Name())
		}

		contents, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if strings.TrimSpace(string(contents)) == "" {
			return nil, fmt.Errorf("migration %q is empty", entry.Name())
		}

		pair, ok := pairs[version]
		if !ok {
			pair = &migrationPair{migration: Migration{Version: version, Name: matches[2]}}
			pairs[version] = pair
		} else if pair.migration.Name != matches[2] {
			return nil, fmt.Errorf("duplicate migration version %06d", version)
		}

		path := filepath.Join(dir, entry.Name())
		switch matches[3] {
		case "up":
			if pair.hasUp {
				return nil, fmt.Errorf("duplicate up migration for version %06d", version)
			}
			pair.migration.UpPath = path
			pair.hasUp = true
		case "down":
			if pair.hasDown {
				return nil, fmt.Errorf("duplicate down migration for version %06d", version)
			}
			pair.migration.DownPath = path
			pair.hasDown = true
		}
	}

	versions := make([]int64, 0, len(pairs))
	for version, pair := range pairs {
		if !pair.hasUp || !pair.hasDown {
			return nil, fmt.Errorf("migration version %06d is missing its pair", version)
		}
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	migrations := make([]Migration, 0, len(versions))
	for _, version := range versions {
		migrations = append(migrations, pairs[version].migration)
	}
	return migrations, nil
}

func NewMigrationRunner(db *gorm.DB, dir string) (*MigrationRunner, error) {
	migrations, err := DiscoverMigrations(dir)
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get migration database handle: %w", err)
	}
	return &MigrationRunner{db: sqlDB, migrations: migrations}, nil
}

func (r *MigrationRunner) Up(ctx context.Context) ([]Migration, error) {
	var executed []Migration
	err := r.withMigrationLock(ctx, func(conn *sql.Conn) error {
		if err := bootstrapSchemaMigrations(ctx, conn); err != nil {
			return err
		}
		applied, err := loadAppliedMigrations(ctx, conn)
		if err != nil {
			return err
		}
		if err := validateAppliedMigrations(r.migrations, applied); err != nil {
			return err
		}

		for _, migration := range r.migrations {
			if _, ok := applied[migration.Version]; ok {
				continue
			}
			migrationSQL, err := os.ReadFile(migration.UpPath)
			if err != nil {
				return fmt.Errorf("read up migration %06d: %w", migration.Version, err)
			}
			if err := applyUpMigration(ctx, conn, migration, string(migrationSQL)); err != nil {
				return err
			}
			executed = append(executed, migration)
		}
		return nil
	})
	return executed, err
}

func (r *MigrationRunner) Down(ctx context.Context) (*Migration, error) {
	var executed *Migration
	err := r.withMigrationLock(ctx, func(conn *sql.Conn) error {
		if err := bootstrapSchemaMigrations(ctx, conn); err != nil {
			return err
		}
		applied, err := loadAppliedMigrations(ctx, conn)
		if err != nil {
			return err
		}
		if err := validateAppliedMigrations(r.migrations, applied); err != nil {
			return err
		}

		for i := len(r.migrations) - 1; i >= 0; i-- {
			migration := r.migrations[i]
			if _, ok := applied[migration.Version]; !ok {
				continue
			}
			migrationSQL, err := os.ReadFile(migration.DownPath)
			if err != nil {
				return fmt.Errorf("read down migration %06d: %w", migration.Version, err)
			}
			if err := applyDownMigration(ctx, conn, migration, string(migrationSQL)); err != nil {
				return err
			}
			executed = &migration
			return nil
		}
		return nil
	})
	return executed, err
}

func (r *MigrationRunner) Status(ctx context.Context) ([]MigrationStatus, error) {
	var tableExists bool
	if err := r.db.QueryRowContext(ctx, "SELECT to_regclass('schema_migrations') IS NOT NULL").Scan(&tableExists); err != nil {
		return nil, fmt.Errorf("check schema_migrations: %w", err)
	}

	applied := make(map[int64]string)
	if tableExists {
		var err error
		applied, err = loadAppliedMigrations(ctx, r.db)
		if err != nil {
			return nil, err
		}
		if err := validateAppliedMigrations(r.migrations, applied); err != nil {
			return nil, err
		}
	}

	statuses := make([]MigrationStatus, 0, len(r.migrations))
	for _, migration := range r.migrations {
		_, isApplied := applied[migration.Version]
		statuses = append(statuses, MigrationStatus{Version: migration.Version, Name: migration.Name, Applied: isApplied})
	}
	return statuses, nil
}

func (r *MigrationRunner) withMigrationLock(ctx context.Context, operation func(*sql.Conn) error) (err error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get dedicated migration connection: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close migration connection: %w", closeErr)
		}
	}()

	if _, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockKey); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		if _, unlockErr := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockKey); err == nil && unlockErr != nil {
			err = fmt.Errorf("release migration advisory lock: %w", unlockErr)
		}
	}()

	return operation(conn)
}

func bootstrapSchemaMigrations(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, bootstrapSchemaMigrationsSQL); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}
	return nil
}

type migrationQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadAppliedMigrations(ctx context.Context, queryer migrationQuerier) (map[int64]string, error) {
	rows, err := queryer.QueryContext(ctx, "SELECT version, name FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]string)
	for rows.Next() {
		var version int64
		var name string
		if err := rows.Scan(&version, &name); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}

func validateAppliedMigrations(migrations []Migration, applied map[int64]string) error {
	local := make(map[int64]Migration, len(migrations))
	for _, migration := range migrations {
		local[migration.Version] = migration
	}
	for version, name := range applied {
		migration, ok := local[version]
		if !ok {
			return fmt.Errorf("database contains unknown migration version %06d", version)
		}
		if name != migration.Name {
			return fmt.Errorf("migration version %06d name mismatch", version)
		}
	}

	seenPending := false
	for _, migration := range migrations {
		_, isApplied := applied[migration.Version]
		if !isApplied {
			seenPending = true
			continue
		}
		if seenPending {
			return fmt.Errorf("applied migration version %06d follows an unapplied local version", migration.Version)
		}
	}
	return nil
}

func applyUpMigration(ctx context.Context, conn *sql.Conn, migration Migration, migrationSQL string) (err error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin up migration %06d: %w", migration.Version, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, migrationSQL); err != nil {
		return fmt.Errorf("execute up migration %06d: %w", migration.Version, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT)", migration.Version, migration.Name); err != nil {
		return fmt.Errorf("record up migration %06d: %w", migration.Version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit up migration %06d: %w", migration.Version, err)
	}
	return nil
}

func applyDownMigration(ctx context.Context, conn *sql.Conn, migration Migration, migrationSQL string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin down migration %06d: %w", migration.Version, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, migrationSQL); err != nil {
		return fmt.Errorf("execute down migration %06d: %w", migration.Version, err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version = $1", migration.Version); err != nil {
		return fmt.Errorf("remove migration version %06d: %w", migration.Version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit down migration %06d: %w", migration.Version, err)
	}
	return nil
}

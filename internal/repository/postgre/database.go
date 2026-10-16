package postgre

import (
	"context"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Database struct {
	gorm *gorm.DB
}

type schemaMigrationIdentity struct {
	Version int64
	Name    string
}

var requiredSchemaMigrations = []schemaMigrationIdentity{
	{Version: 1, Name: "initial_schema"},
}

func Open(ctx context.Context, dsn string) (*Database, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Discard,
	})
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database")
	}
	return &Database{gorm: db.WithContext(ctx)}, nil
}

func (d *Database) GORM() *gorm.DB { return d.gorm }

func (d *Database) Ping(ctx context.Context) error {
	sqlDB, err := d.gorm.DB()
	if err != nil {
		return fmt.Errorf("get PostgreSQL connection pool")
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL database")
	}
	return nil
}

func (d *Database) Close() error {
	sqlDB, err := d.gorm.DB()
	if err != nil {
		return fmt.Errorf("get PostgreSQL connection pool")
	}
	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("close PostgreSQL database")
	}
	return nil
}

func CheckSchemaVersion(ctx context.Context, db *gorm.DB, required int64) error {
	if required != int64(len(requiredSchemaMigrations)) {
		return fmt.Errorf("unsupported required schema version %d", required)
	}
	var tableExists bool
	if err := db.WithContext(ctx).Raw("SELECT to_regclass('schema_migrations') IS NOT NULL").Scan(&tableExists).Error; err != nil {
		return fmt.Errorf("check schema migration table")
	}
	if !tableExists {
		return fmt.Errorf("schema migration table is missing")
	}
	var applied []schemaMigrationIdentity
	if err := db.WithContext(ctx).Raw("SELECT version, name FROM schema_migrations WHERE version <= ? ORDER BY version", required).Scan(&applied).Error; err != nil {
		return fmt.Errorf("read schema migrations through version %d", required)
	}
	if len(applied) != len(requiredSchemaMigrations) {
		return fmt.Errorf("required schema migration history through version %d is incomplete", required)
	}
	for index, expected := range requiredSchemaMigrations {
		if applied[index] != expected {
			return fmt.Errorf("schema migration version %d identity mismatch", expected.Version)
		}
	}
	return nil
}

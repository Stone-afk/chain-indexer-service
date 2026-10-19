package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/pkg/postgre"
)

// migrationDirection 表示迁移命令支持的操作。
type migrationDirection string

func (d migrationDirection) ToString() string { return string(d) }

const (
	// migrationDirectionUp 执行待应用迁移。
	migrationDirectionUp migrationDirection = "up"
	// migrationDirectionDown 回滚最近一次迁移。
	migrationDirectionDown migrationDirection = "down"
	// migrationDirectionStatus 查看迁移状态。
	migrationDirectionStatus migrationDirection = "status"
)

func run(ctx context.Context, args []string, stdout, stderr io.Writer, lookupEnv func(string) (string, bool)) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: migrate <up|down|status> --config <path> [--migrations-dir <path>]")
		return 2
	}
	direction := migrationDirection(args[0])
	if direction != migrationDirectionUp && direction != migrationDirectionDown && direction != migrationDirectionStatus {
		fmt.Fprintf(stderr, "unknown migration direction %q\n", direction.ToString())
		return 2
	}

	flags := flag.NewFlagSet("migrate "+direction.ToString(), flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "configuration file")
	migrationsDir := flags.String("migrations-dir", "script/postgre", "migration directory")
	if err := flags.Parse(args[1:]); err != nil {
		fmt.Fprintln(stderr, "invalid migration arguments")
		return 2
	}
	if *configPath == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "--config is required and positional arguments are not allowed")
		return 2
	}

	databaseConfig, err := config.LoadForMigration(*configPath, lookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "load migration configuration: %v\n", err)
		return 1
	}
	database, err := postgre.Open(databaseConfig.DSN)
	if err != nil {
		fmt.Fprintf(stderr, "open migration database: %v\n", err)
		return 1
	}

	runner, operationErr := postgre.NewMigrationRunner(database, *migrationsDir)
	if operationErr == nil {
		logger := slog.New(slog.NewTextHandler(stdout, nil)).With("command", "migrate", "direction", direction.ToString())
		switch direction {
		case migrationDirectionUp:
			var executed []postgre.Migration
			executed, operationErr = runner.Up(ctx)
			for _, migration := range executed {
				logger.Info("migration applied", "migration_version", migration.Version, "migration_name", migration.Name)
			}
		case migrationDirectionDown:
			var executed *postgre.Migration
			executed, operationErr = runner.Down(ctx)
			if operationErr == nil && executed != nil {
				logger.Info("migration rolled back", "migration_version", executed.Version, "migration_name", executed.Name)
			}
		case migrationDirectionStatus:
			var statuses []postgre.MigrationStatus
			statuses, operationErr = runner.Status(ctx)
			if operationErr == nil {
				for _, status := range statuses {
					state := "pending"
					if status.Applied {
						state = "applied"
					}
					fmt.Fprintf(stdout, "%06d %s %s\n", status.Version, status.Name, state)
				}
			}
		}
	}
	closeErr := postgre.Close(database)
	if operationErr != nil {
		fmt.Fprintf(stderr, "migrate %s: %v\n", direction.ToString(), operationErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "close migration database: %v\n", closeErr)
		return 1
	}
	return 0
}

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Stone-afk/chain-indexer-service/cmd/chain-indexer/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/config"
)

func main() {
	flags := flag.NewFlagSet("chain-indexer", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "configuration file")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Fprintln(os.Stdout, "usage: chain-indexer --config <path>")
			return
		}
		fmt.Fprintln(os.Stderr, "invalid service arguments")
		os.Exit(2)
	}
	if *configPath == "" || flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: chain-indexer --config <path>")
		os.Exit(2)
	}

	serviceConfig, err := config.LoadForService(*configPath, os.LookupEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load service configuration: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	_, cleanupDB, err := ioc.InitDB(ctx, serviceConfig.Database)
	if err != nil {
		stop()
		fmt.Fprintf(os.Stderr, "start service: %v\n", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("command", "chain-indexer")
	logger.Info("service ready", "schema_version", 1)
	<-ctx.Done()
	logger.Info("service stopping")
	stop()
	if err := cleanupDB(); err != nil {
		fmt.Fprintf(os.Stderr, "stop service: %v\n", err)
		os.Exit(1)
	}
	logger.Info("service stopped")
}

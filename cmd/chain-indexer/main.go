package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"github.com/Stone-afk/chain-indexer-service/cmd/chain-indexer/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/listener"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
	"github.com/Stone-afk/chain-indexer-service/internal/scanner"
	"github.com/Stone-afk/chain-indexer-service/internal/worker/transaction"
	"gorm.io/gorm"
)

type initDBFunc func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error)
type initEventFunc func(*gorm.DB) repository.EventRepository
type initChainFunc func(*gorm.DB) repository.ChainRepository
type initRPCFunc func(context.Context, config.ChainConfig) (provider.Reader, provider.HeadSubscriber, func() error, error)
type initScannerFunc func(provider.Reader, repository.ChainRepository, config.ChainConfig) *scanner.Scanner
type initListenerFunc func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener
type initWorkerFunc func(repository.EventRepository) (*transaction.Worker, error)
type runListenerFunc func(context.Context, *listener.Listener, func() <-chan struct{}) error
type runWorkerFunc func(context.Context, *transaction.Worker, func() <-chan struct{}) error

type hiddenError struct {
	cause error
	what  string
}

type publicError interface {
	PublicError() string
}

func (e hiddenError) Error() string { return e.what }
func (e hiddenError) Unwrap() error { return e.cause }
func hide(what string, err error) error {
	if err == nil {
		return nil
	}
	return hiddenError{cause: err, what: what}
}

func exposePublic(what string, err error) error {
	var public publicError
	if errors.As(err, &public) {
		return hiddenError{cause: err, what: what + ": " + public.PublicError()}
	}
	return hide(what, err)
}

func enabledChainNames(chains map[string]config.ChainConfig) []string {
	out := make([]string, 0)
	for n, c := range chains {
		if c.Enabled {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func runService(ctx context.Context, cfg *config.Config, logger *slog.Logger, db initDBFunc, event initEventFunc, chain initChainFunc, rpc initRPCFunc, makeScanner initScannerFunc, makeListener initListenerFunc, makeWorker initWorkerFunc, runL runListenerFunc, runW runWorkerFunc) error {
	database, dbCleanup, err := db(ctx, cfg.Database)
	if err != nil {
		return exposePublic("initialize database", err)
	}
	cleanups := []func() error{dbCleanup}
	cleanup := func(cause error) error {
		var all []error
		if cause != nil {
			all = append(all, cause)
		}
		for i := len(cleanups) - 1; i >= 0; i-- {
			if e := cleanups[i](); e != nil {
				all = append(all, e)
			}
		}
		return errors.Join(all...)
	}
	stopError := func(first error) error {
		if first == nil {
			return hide("service stopped with error", cleanup(nil))
		}
		return hide(first.Error(), cleanup(first))
	}
	events := event(database)
	chains := chain(database)
	type pair struct{ l *listener.Listener }
	pairs := map[string]pair{}
	for _, name := range enabledChainNames(cfg.Chains) {
		r, s, cl, e := rpc(ctx, cfg.Chains[name])
		if e != nil {
			return hide("initialize RPC for chain "+name, cleanup(e))
		}
		cleanups = append(cleanups, cl)
		sc := makeScanner(r, chains, cfg.Chains[name])
		ls := makeListener(s, sc)
		pairs[name] = pair{l: ls}
	}
	w, e := makeWorker(events)
	if e != nil {
		return hide("initialize worker", cleanup(e))
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	entered := make(chan struct{}, len(pairs)+1)
	permit := make(chan struct{})
	var release sync.Once
	closePermit := func() { release.Do(func() { close(permit) }) }
	start := func(label string, run func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := run(); e != nil {
				mu.Lock()
				if first == nil && ctx.Err() == nil {
					first = hide("component "+label, e)
					cancel()
				}
				mu.Unlock()
			} else {
				mu.Lock()
				if first == nil && ctx.Err() == nil {
					first = errors.New("component " + label + " returned unexpectedly")
					cancel()
				}
				mu.Unlock()
			}
		}()
	}
	for _, name := range enabledChainNames(cfg.Chains) {
		p := pairs[name]
		var once sync.Once
		start("chain "+name+" listener", func() error {
			return runL(child, p.l, func() <-chan struct{} { once.Do(func() { entered <- struct{}{} }); return permit })
		})
	}
	var wonce sync.Once
	start("transaction worker", func() error {
		return runW(child, w, func() <-chan struct{} { wonce.Do(func() { entered <- struct{}{} }); return permit })
	})
	for i := 0; i < len(pairs)+1; i++ {
		select {
		case <-entered:
		case <-child.Done():
			closePermit()
			cancel()
			wg.Wait()
			return stopError(first)
		}
	}
	if child.Err() != nil {
		closePermit()
		wg.Wait()
		return stopError(first)
	}
	logger.Info("service ready", "schema_version", 3, "enabled_chains", len(pairs))
	closePermit()
	<-child.Done()
	if ctx.Err() != nil {
		logger.Info("service stopping")
	}
	wg.Wait()
	mu.Lock()
	firstErr := first
	mu.Unlock()
	if ctx.Err() != nil && firstErr == nil {
		return hide("cleanup service", cleanup(nil))
	}
	return stopError(firstErr)
}

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
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("command", "chain-indexer")
	runL := func(c context.Context, l *listener.Listener, entered func() <-chan struct{}) error {
		<-entered()
		return l.Run(c)
	}
	runW := func(c context.Context, w *transaction.Worker, entered func() <-chan struct{}) error {
		<-entered()
		return w.Run(c)
	}
	if err := runService(ctx, serviceConfig, logger, ioc.InitDB, ioc.InitEvent, ioc.InitChain, ioc.InitRPC, ioc.InitScanner, ioc.InitListener, ioc.InitWorker, runL, runW); err != nil {
		stop()
		fmt.Fprintf(os.Stderr, "stop service: %v\n", err)
		os.Exit(1)
	}
	stop()
	logger.Info("service stopped")
}

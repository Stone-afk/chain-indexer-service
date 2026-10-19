package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeioc "github.com/Stone-afk/chain-indexer-service/cmd/chain-indexer/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/listener"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
	"github.com/Stone-afk/chain-indexer-service/internal/scanner"
	"github.com/Stone-afk/chain-indexer-service/internal/worker/transaction"
	"gorm.io/gorm"
)

const runtimeSecret = "runtime-secret-dsn-api-key"

type lifecycleRecorder struct {
	mu          sync.Mutex
	init, clean []string
}

func (r *lifecycleRecorder) initMark(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init = append(r.init, s)
}
func (r *lifecycleRecorder) cleanFn(s string, e error) func() error {
	return func() error { r.mu.Lock(); r.clean = append(r.clean, s); r.mu.Unlock(); return e }
}

type recordingHandler struct {
	state  *handlerState
	signal chan struct{}
	attrs  []slog.Attr
}
type handlerState struct {
	mu      sync.Mutex
	records []slog.Record
	ready   bool
}

func newHandler(signal chan struct{}) *recordingHandler {
	return &recordingHandler{state: &handlerState{}, signal: signal}
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.state.mu.Lock()
	for _, a := range h.attrs {
		r.AddAttrs(a)
	}
	h.state.records = append(h.state.records, r.Clone())
	ready := r.Message == "service ready"
	if ready {
		h.state.ready = true
	}
	h.state.mu.Unlock()
	if ready && h.signal != nil {
		select {
		case h.signal <- struct{}{}:
		default:
		}
	}
	return nil
}
func (h *recordingHandler) WithAttrs(a []slog.Attr) slog.Handler {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	cp := *h
	cp.attrs = append(append([]slog.Attr{}, h.attrs...), a...)
	return &cp
}
func (h *recordingHandler) WithGroup(string) slog.Handler {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	cp := *h
	return &cp
}
func (h *recordingHandler) count(m string) int {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	n := 0
	for _, r := range h.state.records {
		if r.Message == m {
			n++
		}
	}
	return n
}
func (h *recordingHandler) messages() []string {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	messages := make([]string, 0, len(h.state.records))
	for _, record := range h.state.records {
		messages = append(messages, record.Message)
	}
	return messages
}
func (h *recordingHandler) text() string {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	var b bytes.Buffer
	for _, r := range h.state.records {
		b.WriteString(r.Message)
		r.Attrs(func(a slog.Attr) bool { b.WriteString(a.String()); return true })
		b.WriteByte('\n')
	}
	return b.String()
}

type runPlan struct {
	mu                              sync.Mutex
	ls                              map[*listener.Listener]string
	ws                              map[*transaction.Worker]string
	enterGates                      map[string]chan struct{}
	resultGates                     map[string]chan struct{}
	errs                            map[string]error
	duplicateEntered                map[string]bool
	permitObserver                  map[string]func() error
	inv, arrival, ent, cancel, done chan string
	release                         <-chan struct{}
}

func newRunPlan() *runPlan {
	return &runPlan{ls: map[*listener.Listener]string{}, ws: map[*transaction.Worker]string{}, enterGates: map[string]chan struct{}{}, resultGates: map[string]chan struct{}{}, errs: map[string]error{}, duplicateEntered: map[string]bool{}, permitObserver: map[string]func() error{}, inv: make(chan string, 32), arrival: make(chan string, 32), ent: make(chan string, 32), cancel: make(chan string, 32), done: make(chan string, 32)}
}
func (p *runPlan) lr(c context.Context, l *listener.Listener, e func() <-chan struct{}) error {
	p.mu.Lock()
	id, g := p.ls[l], p.enterGates[p.ls[l]]
	p.mu.Unlock()
	return p.run(c, id, g, e)
}
func (p *runPlan) wr(c context.Context, w *transaction.Worker, e func() <-chan struct{}) error {
	p.mu.Lock()
	id := p.ws[w]
	g := p.enterGates[id]
	p.mu.Unlock()
	return p.run(c, id, g, e)
}
func (p *runPlan) run(c context.Context, id string, g <-chan struct{}, e func() <-chan struct{}) error {
	waitRelease := func() {
		if p.release != nil {
			<-p.release
		}
	}
	p.inv <- id
	p.mu.Lock()
	resultGate := p.resultGates[id]
	p.mu.Unlock()
	select {
	case <-resultGate:
		p.mu.Lock()
		x := p.errs[id]
		p.mu.Unlock()
		if x != nil {
			return x
		}
		return nil
	default:
	}
	select {
	case <-c.Done():
		p.cancel <- id
		waitRelease()
		p.done <- id
		return c.Err()
	case <-resultGate:
		p.mu.Lock()
		x := p.errs[id]
		p.mu.Unlock()
		if x != nil {
			return x
		}
		return nil
	case <-g:
		permit := e()
		p.arrival <- id
		<-permit
		if observer := p.permitObserver[id]; observer != nil {
			if err := observer(); err != nil {
				return err
			}
		}
		p.mu.Lock()
		dup := p.duplicateEntered[id]
		p.mu.Unlock()
		if dup {
			e()
		}
		p.ent <- id
	}
	p.mu.Lock()
	result := p.resultGates[id]
	p.mu.Unlock()
	select {
	case <-c.Done():
		p.cancel <- id
		waitRelease()
		p.done <- id
		return c.Err()
	case <-result:
		p.mu.Lock()
		x := p.errs[id]
		p.mu.Unlock()
		if x != nil {
			return x
		}
		p.done <- id
		return nil
	}
}
func await(t *testing.T, c context.Context, ch <-chan string, w string) {
	t.Helper()
	select {
	case x := <-ch:
		if x != w {
			t.Fatalf("got %q want %q", x, w)
		}
	case <-c.Done():
		t.Fatal(c.Err())
	}
}
func awaitN(t *testing.T, c context.Context, ch <-chan string, n int) []string {
	t.Helper()
	o := make([]string, 0, n)
	for len(o) < n {
		select {
		case x := <-ch:
			o = append(o, x)
		case <-c.Done():
			t.Fatal(c.Err())
		}
	}
	return o
}
func awaitErr(t *testing.T, c context.Context, ch <-chan error) error {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-c.Done():
		t.Fatal(c.Err())
		return nil
	}
}
func cfg() *config.Config {
	return &config.Config{Chains: map[string]config.ChainConfig{"zeta": {Enabled: true, ChainID: 2}, "alpha": {Enabled: true, ChainID: 1}, "off": {Enabled: false, ChainID: 3}}}
}

type testDeps struct {
	db       initDBFunc
	event    initEventFunc
	chain    initChainFunc
	rpc      initRPCFunc
	scanner  initScannerFunc
	listener initListenerFunc
	worker   initWorkerFunc
}

func initializers(r *lifecycleRecorder) testDeps {
	return testDeps{db: func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error) {
		r.initMark("db")
		return &gorm.DB{}, r.cleanFn("db", nil), nil
	}, event: func(*gorm.DB) repository.EventRepository { r.initMark("event"); return nil }, chain: func(*gorm.DB) repository.ChainRepository { r.initMark("chain"); return nil }, rpc: func(context.Context, config.ChainConfig) (provider.Reader, provider.HeadSubscriber, func() error, error) {
		r.initMark("rpc")
		return nil, nil, r.cleanFn("rpc", nil), nil
	}, scanner: func(provider.Reader, repository.ChainRepository, config.ChainConfig) *scanner.Scanner {
		r.initMark("scanner")
		return nil
	}, listener: func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
		r.initMark("listener")
		return &listener.Listener{}
	}, worker: func(repository.EventRepository) (*transaction.Worker, error) {
		r.initMark("worker")
		return &transaction.Worker{}, nil
	}}
}
func start(t *testing.T, d testDeps, c context.Context, l *slog.Logger, p *runPlan) <-chan error {
	t.Helper()
	o := make(chan error, 1)
	go func() {
		o <- runService(c, cfg(), l, d.db, d.event, d.chain, d.rpc, d.scanner, d.listener, d.worker, p.lr, p.wr)
	}()
	return o
}

func TestRuntimeIoCSignatures(t *testing.T) {
	d := testDeps{db: runtimeioc.InitDB, event: runtimeioc.InitEvent, chain: runtimeioc.InitChain, rpc: runtimeioc.InitRPC, scanner: runtimeioc.InitScanner, listener: runtimeioc.InitListener, worker: runtimeioc.InitWorker}
	if d.db == nil || d.event == nil || d.chain == nil || d.rpc == nil || d.scanner == nil || d.listener == nil || d.worker == nil {
		t.Fatal("initializer missing")
	}
	var _ runListenerFunc = func(context.Context, *listener.Listener, func() <-chan struct{}) error { return nil }
	var _ runWorkerFunc = func(context.Context, *transaction.Worker, func() <-chan struct{}) error { return nil }
}
func TestEnabledChainNamesAreSortedAndDisabledChainsAreExcluded(t *testing.T) {
	g := enabledChainNames(map[string]config.ChainConfig{"z": {Enabled: true}, "a": {Enabled: true}, "x": {}})
	if fmt.Sprint(g) != "[a z]" || !sort.StringsAreSorted(g) {
		t.Fatal(g)
	}
}
func TestRunServiceInitializesOneListenerPerEnabledChainAndOneWorker(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	p := newRunPlan()
	n := 0
	workerCalls := 0
	d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
		n++
		l := &listener.Listener{}
		id := []string{"listener:alpha", "listener:zeta"}[n-1]
		p.ls[l] = id
		p.enterGates[id] = make(chan struct{})
		p.resultGates[id] = make(chan struct{})
		return l
	}
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
		workerCalls++
		w := &transaction.Worker{}
		p.ws[w] = "worker"
		p.enterGates["worker"] = make(chan struct{})
		p.resultGates["worker"] = make(chan struct{})
		return w, nil
	}
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := start(t, d, c, slog.Default(), p)
	q, s := context.WithTimeout(context.Background(), time.Second)
	defer s()
	inv := awaitN(t, q, p.inv, 3)
	sort.Strings(inv)
	if fmt.Sprint(inv) != "[listener:alpha listener:zeta worker]" || n != 2 || workerCalls != 1 {
		t.Fatalf("listeners=%d", n)
	}
	cancel()
	awaitErr(t, q, done)
	select {
	case extra := <-p.inv:
		t.Fatalf("unexpected invocation after shutdown %q", extra)
	default:
	}
}
func TestRunServiceComponentFailureCancelsPeers(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
		mode string
	}{
		{name: "ordinary-error-before-enter", want: errors.New("component"), mode: "error-before"},
		{name: "nil-before-enter", mode: "nil-before"},
		{name: "context-canceled-before-enter", want: context.Canceled, mode: "canceled-before"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &lifecycleRecorder{}
			d := initializers(r)
			ids := map[*listener.Listener]string{}
			workerIDs := map[*transaction.Worker]string{}
			invoked := make(chan string, 3)
			canceled := make(chan string, 2)
			donePeers := make(chan string, 2)
			releasePeers := make(chan struct{})
			ready := newHandler(nil)
			var listeners []*listener.Listener
			d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
				id := []string{"listener:alpha", "listener:zeta"}[len(listeners)]
				l := &listener.Listener{}
				listeners = append(listeners, l)
				ids[l] = id
				return l
			}
			d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
				w := &transaction.Worker{}
				workerIDs[w] = "worker"
				return w, nil
			}
			c, cancel := context.WithCancel(context.Background())
			defer cancel()
			run := func(c context.Context, id string, enter func() <-chan struct{}) error {
				invoked <- id
				if id == "listener:alpha" {
					switch tc.mode {
					case "nil-before":
						return nil
					case "canceled-before":
						return context.Canceled
					default:
						return tc.want
					}
				}
				permit := enter()
				<-permit
				<-c.Done()
				canceled <- id
				<-releasePeers
				donePeers <- id
				return c.Err()
			}
			result := make(chan error, 1)
			go func() {
				result <- runService(c, cfg(), slog.New(ready), d.db, d.event, d.chain, d.rpc, d.scanner, d.listener, d.worker,
					func(ctx context.Context, l *listener.Listener, enter func() <-chan struct{}) error {
						return run(ctx, ids[l], enter)
					},
					func(ctx context.Context, w *transaction.Worker, enter func() <-chan struct{}) error {
						return run(ctx, workerIDs[w], enter)
					})
			}()
			q, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			got := awaitN(t, q, invoked, 3)
			sort.Strings(got)
			if fmt.Sprint(got) != "[listener:alpha listener:zeta worker]" {
				t.Fatalf("invoked=%v", got)
			}
			select {
			case <-result:
				t.Fatal("service returned before peers were released")
			default:
			}
			awaitN(t, q, canceled, 2)
			if ready.count("service ready") != 0 {
				t.Fatal("service became ready after component failure")
			}
			select {
			case <-result:
				t.Fatal("service returned before peers were released")
			default:
			}
			close(releasePeers)
			awaitN(t, q, donePeers, 2)
			e := awaitErr(t, q, result)
			if e == nil || (tc.want != nil && !errors.Is(e, tc.want)) {
				t.Fatalf("error=%v want=%v", e, tc.want)
			}
		})
	}
}
func TestRunServiceSignalCancellationIsGraceful(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	p := newRunPlan()
	d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
		l := &listener.Listener{}
		p.ls[l] = "listener"
		p.enterGates["listener"] = make(chan struct{})
		p.resultGates["listener"] = make(chan struct{})
		return l
	}
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
		w := &transaction.Worker{}
		p.ws[w] = "worker"
		p.enterGates["worker"] = make(chan struct{})
		p.resultGates["worker"] = make(chan struct{})
		return w, nil
	}
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := start(t, d, c, slog.Default(), p)
	q, s := context.WithTimeout(context.Background(), time.Second)
	defer s()
	awaitN(t, q, p.inv, 3)
	cancel()
	if e := awaitErr(t, q, done); e != nil {
		t.Fatal(e)
	}
}
func TestRunServiceExternalCancellationLogsStoppingAfterReady(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	p := newRunPlan()
	d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
		l := &listener.Listener{}
		id := fmt.Sprintf("listener:%d", len(p.ls))
		p.ls[l] = id
		p.enterGates[id] = make(chan struct{})
		p.resultGates[id] = make(chan struct{})
		return l
	}
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
		w := &transaction.Worker{}
		p.ws[w] = "worker"
		p.enterGates["worker"] = make(chan struct{})
		p.resultGates["worker"] = make(chan struct{})
		return w, nil
	}
	ready := make(chan struct{}, 1)
	h := newHandler(ready)
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	done := start(t, d, serviceCtx, slog.New(h), p)
	waitCtx, stopWaiting := context.WithTimeout(context.Background(), time.Second)
	defer stopWaiting()
	awaitN(t, waitCtx, p.inv, 3)
	for _, gate := range p.enterGates {
		close(gate)
	}
	awaitN(t, waitCtx, p.arrival, 3)
	select {
	case <-ready:
	case <-waitCtx.Done():
		t.Fatal(waitCtx.Err())
	}
	awaitN(t, waitCtx, p.ent, 3)
	stopService()
	if err := awaitErr(t, waitCtx, done); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(h.messages()); got != "[service ready service stopping]" {
		t.Fatalf("lifecycle messages=%s", got)
	}
}
func TestRunServicePartialInitializationFailureCleansUpInReverseWithoutReady(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	workerCalls := 0
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
		workerCalls++
		return &transaction.Worker{}, nil
	}
	cause := errors.New("rpc")
	d.rpc = func(_ context.Context, c config.ChainConfig) (provider.Reader, provider.HeadSubscriber, func() error, error) {
		r.initMark("rpc")
		if c.ChainID == 1 {
			return nil, nil, r.cleanFn("rpc:alpha", nil), nil
		}
		return nil, nil, r.cleanFn("rpc:zeta", nil), cause
	}
	h := newHandler(nil)
	p := newRunPlan()
	c, s := context.WithTimeout(context.Background(), time.Second)
	defer s()
	if e := awaitErr(t, c, start(t, d, c, slog.New(h), p)); !errors.Is(e, cause) {
		t.Fatal(e)
	}
	if h.count("service ready") != 0 || fmt.Sprint(r.clean) != "[rpc:alpha db]" {
		t.Fatalf("cleanup=%v ready=%d", r.clean, h.count("service ready"))
	}
	if workerCalls != 0 {
		t.Fatalf("worker initializer calls=%d", workerCalls)
	}
	select {
	case got := <-p.inv:
		t.Fatalf("unexpected invocation %q", got)
	default:
	}
}
func TestRunServiceAttemptsAllCleanupsAndJoinsErrors(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	componentErr, dbErr, rpcAlphaErr, rpcZetaErr := errors.New("component"), errors.New("db"), errors.New("rpc alpha"), errors.New("rpc zeta")
	d.db = func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error) {
		return &gorm.DB{}, r.cleanFn("db", dbErr), nil
	}
	d.rpc = func(_ context.Context, cfg config.ChainConfig) (provider.Reader, provider.HeadSubscriber, func() error, error) {
		if cfg.ChainID == 1 {
			return nil, nil, r.cleanFn("rpc:alpha", rpcAlphaErr), nil
		}
		return nil, nil, r.cleanFn("rpc:zeta", rpcZetaErr), nil
	}
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) { return &transaction.Worker{}, nil }
	x, s := context.WithTimeout(context.Background(), time.Second)
	defer s()
	p := newRunPlan()
	n := 0
	d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
		n++
		l := &listener.Listener{}
		id := []string{"listener:alpha", "listener:zeta"}[n-1]
		p.ls[l] = id
		p.enterGates[id] = make(chan struct{})
		p.resultGates[id] = make(chan struct{})
		if id == "listener:alpha" {
			p.errs[id] = componentErr
		}
		return l
	}
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
		w := &transaction.Worker{}
		p.ws[w] = "worker"
		p.enterGates["worker"] = make(chan struct{})
		p.resultGates["worker"] = make(chan struct{})
		return w, nil
	}
	h := newHandler(make(chan struct{}, 1))
	eCh := start(t, d, x, slog.New(h), p)
	q, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	awaitN(t, q, p.inv, 3)
	for _, id := range []string{"listener:alpha", "listener:zeta", "worker"} {
		close(p.enterGates[id])
	}
	arrivals := awaitN(t, q, p.arrival, 3)
	ents := awaitN(t, q, p.ent, 3)
	sort.Strings(arrivals)
	sort.Strings(ents)
	if fmt.Sprint(arrivals) != "[listener:alpha listener:zeta worker]" || fmt.Sprint(ents) != "[listener:alpha listener:zeta worker]" {
		t.Fatalf("arrivals=%v entered=%v", arrivals, ents)
	}
	if h.count("service ready") != 1 {
		t.Fatalf("ready count=%d", h.count("service ready"))
	}
	close(p.resultGates["listener:alpha"])
	e := awaitErr(t, q, eCh)
	for _, v := range []error{componentErr, dbErr, rpcAlphaErr, rpcZetaErr} {
		if !errors.Is(e, v) {
			t.Errorf("missing %v", v)
		}
	}
	if fmt.Sprint(r.clean) != "[rpc:zeta rpc:alpha db]" {
		t.Fatal(r.clean)
	}
}
func TestRunServiceReadyAfterAllComponentsEnterRun(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	p := newRunPlan()
	sentinel := errors.New("last entrant")
	d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
		l := &listener.Listener{}
		id := fmt.Sprintf("listener:%d", len(p.ls))
		p.ls[l] = id
		p.enterGates[id] = make(chan struct{})
		p.resultGates[id] = make(chan struct{})
		return l
	}
	d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
		w := &transaction.Worker{}
		p.ws[w] = "worker"
		p.enterGates["worker"] = make(chan struct{})
		p.resultGates["worker"] = make(chan struct{})
		p.errs["worker"] = sentinel
		return w, nil
	}
	h := newHandler(make(chan struct{}, 1))
	p.permitObserver["worker"] = func() error {
		if h.count("service ready") != 1 {
			return errors.New("permit-before-ready")
		}
		return nil
	}
	x, cancel := context.WithCancel(context.Background())
	defer cancel()
	peerRelease := make(chan struct{})
	p.release = peerRelease
	done := start(t, d, x, slog.New(h).With("command", "chain-indexer"), p)
	q, s := context.WithTimeout(context.Background(), time.Second)
	defer s()
	awaitN(t, q, p.inv, 3)
	if h.count("service ready") != 0 {
		t.Fatal("premature ready")
	}
	for l, id := range p.ls {
		if id == "listener:0" {
			p.duplicateEntered[id] = true
		}
		close(p.enterGates[id])
		await(t, q, p.arrival, id)
		_ = l
	}
	if h.count("service ready") != 0 {
		t.Fatal("premature ready")
	}
	select {
	case <-h.signal:
		t.Fatal("premature ready signal")
	default:
	}
	close(p.enterGates["worker"])
	await(t, q, p.arrival, "worker")
	close(p.resultGates["worker"])
	select {
	case <-h.signal:
	case <-q.Done():
		t.Fatal(q.Err())
	}
	if h.count("service ready") != 1 {
		t.Fatal("ready count")
	}
	h.state.mu.Lock()
	commandCount := 0
	var command string
	var schema, enabled int64
	h.state.records[len(h.state.records)-1].Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "command":
			commandCount++
			command = a.Value.String()
		case "schema_version":
			schema = a.Value.Int64()
		case "enabled_chains":
			enabled = a.Value.Int64()
		}
		return true
	})
	h.state.mu.Unlock()
	if command != "chain-indexer" || commandCount != 1 || schema != 3 || enabled != 2 {
		t.Fatalf("ready attrs command=%q commandCount=%d schema=%d enabled=%d", command, commandCount, schema, enabled)
	}
	awaitN(t, q, p.cancel, 2)
	select {
	case <-done:
		t.Fatal("service returned before peers completed")
	default:
	}
	close(peerRelease)
	awaitN(t, q, p.done, 2)
	select {
	case extra := <-p.arrival:
		t.Fatalf("unexpected extra arrival %q", extra)
	default:
	}
	if e := awaitErr(t, q, done); !errors.Is(e, sentinel) {
		t.Fatalf("error=%v", e)
	}
	if h.count("service stopping") != 0 {
		t.Fatal("component failure was logged as an ordinary stop")
	}
	cancel()
}
func TestRunServiceErrorsAndLogsRedactSecrets(t *testing.T) {
	var hasHiddenWhat func(error, string) bool
	hasHiddenWhat = func(err error, want string) bool {
		if err == nil {
			return false
		}
		var h hiddenError
		if errors.As(err, &h) && h.what == want {
			return true
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				if hasHiddenWhat(child, want) {
					return true
				}
			}
		}
		if unwrapped := errors.Unwrap(err); unwrapped != nil {
			return hasHiddenWhat(unwrapped, want)
		}
		return false
	}
	for _, name := range []string{"init-db", "init-rpc-chain", "component-listener", "component-worker", "cleanup"} {
		t.Run(name, func(t *testing.T) {
			r := &lifecycleRecorder{}
			d := initializers(r)
			cause := errors.New(runtimeSecret)
			if name == "init-db" {
				d.db = func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error) {
					return nil, nil, cause
				}
			}
			if name == "init-rpc-chain" {
				d.rpc = func(context.Context, config.ChainConfig) (provider.Reader, provider.HeadSubscriber, func() error, error) {
					return nil, nil, nil, cause
				}
			}
			if name == "cleanup" {
				d.db = func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error) {
					return &gorm.DB{}, func() error { return cause }, nil
				}
			}
			readySignal := make(chan struct{}, 1)
			h := newHandler(readySignal)
			var serviceCtx context.Context
			var serviceCancel context.CancelFunc
			if name == "cleanup" {
				serviceCtx, serviceCancel = context.WithCancel(context.Background())
			} else {
				serviceCtx, serviceCancel = context.WithTimeout(context.Background(), time.Second)
			}
			defer serviceCancel()
			p := newRunPlan()
			if name == "component-listener" || name == "component-worker" || name == "cleanup" {
				d.listener = func(provider.HeadSubscriber, listener.HeadHandler) *listener.Listener {
					l := &listener.Listener{}
					id := "listener:alpha"
					if len(p.ls) > 0 {
						id = "listener:zeta"
					}
					p.ls[l] = id
					p.enterGates[id] = make(chan struct{})
					p.resultGates[id] = make(chan struct{})
					if name == "component-listener" && id == "listener:alpha" {
						p.errs[id] = cause
					}
					return l
				}
				d.worker = func(repository.EventRepository) (*transaction.Worker, error) {
					w := &transaction.Worker{}
					p.ws[w] = "worker"
					p.enterGates["worker"] = make(chan struct{})
					p.resultGates["worker"] = make(chan struct{})
					if name == "component-worker" {
						p.errs["worker"] = cause
					}
					return w, nil
				}
			}
			var eCh <-chan error
			if name == "cleanup" {
				eCh = start(t, d, serviceCtx, slog.New(h), p)
				startedCtx, startedCancel := context.WithTimeout(context.Background(), time.Second)
				awaitN(t, startedCtx, p.inv, 3)
				startedCancel()
				for _, gate := range p.enterGates {
					close(gate)
				}
				arrivalCtx, arrivalCancel := context.WithTimeout(context.Background(), time.Second)
				awaitN(t, arrivalCtx, p.arrival, 3)
				arrivalCancel()
				readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
				select {
				case <-readySignal:
				case <-readyCtx.Done():
					t.Fatal(readyCtx.Err())
				}
				readyCancel()
				serviceCancel()
			} else {
				eCh = start(t, d, serviceCtx, slog.New(h), p)
			}
			if name == "component-listener" || name == "component-worker" {
				q, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				awaitN(t, q, p.inv, 3)
				if name == "component-listener" {
					close(p.resultGates["listener:alpha"])
				} else {
					close(p.resultGates["worker"])
				}
			}
			resultCtx, resultCancel := context.WithTimeout(context.Background(), 2*time.Second)
			e := awaitErr(t, resultCtx, eCh)
			resultCancel()
			if e == nil || !errors.Is(e, cause) {
				t.Fatal("cause missing")
			}
			if name == "cleanup" && e.Error() != "cleanup service" {
				t.Fatalf("top-level error=%q", e.Error())
			}
			if name == "init-db" && e.Error() != "initialize database" {
				t.Fatalf("top-level error=%q", e.Error())
			}
			if name == "cleanup" && h.count("service ready") != 1 {
				t.Fatalf("cleanup ready count=%d", h.count("service ready"))
			}
			if name != "cleanup" && h.count("service ready") != 0 {
				t.Fatalf("%s ready count=%d", name, h.count("service ready"))
			}
			if name == "init-rpc-chain" && e.Error() != "initialize RPC for chain alpha" {
				t.Fatalf("top-level error=%q", e.Error())
			}
			if name == "component-listener" && (e.Error() != "component chain alpha listener" || !hasHiddenWhat(e, "component chain alpha listener")) {
				t.Fatalf("listener wrapper=%q", e.Error())
			}
			if name == "component-worker" && (e.Error() != "component transaction worker" || !hasHiddenWhat(e, "component transaction worker")) {
				t.Fatalf("worker wrapper=%q", e.Error())
			}
			if strings.Contains(e.Error(), runtimeSecret) || strings.Contains(h.text(), runtimeSecret) {
				t.Fatal("secret leaked")
			}
		})
	}
}

type operatorVisibleTestError struct {
	cause   error
	message string
}

func (e operatorVisibleTestError) Error() string       { return e.cause.Error() }
func (e operatorVisibleTestError) Unwrap() error       { return e.cause }
func (e operatorVisibleTestError) PublicError() string { return e.message }

func TestRunServiceExposesOperatorSafeDatabaseInitializationError(t *testing.T) {
	r := &lifecycleRecorder{}
	d := initializers(r)
	cause := errors.New("database initialization cause")
	d.db = func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error) {
		return nil, nil, operatorVisibleTestError{cause: cause, message: "initialize PostgreSQL: schema migration table is missing"}
	}
	err := runService(context.Background(), cfg(), slog.Default(), d.db, d.event, d.chain, d.rpc, d.scanner, d.listener, d.worker, newRunPlan().lr, newRunPlan().wr)
	if !errors.Is(err, cause) {
		t.Fatal("database initialization cause missing")
	}
	if got, want := err.Error(), "initialize database: initialize PostgreSQL: schema migration table is missing"; got != want {
		t.Fatalf("error=%q want=%q", got, want)
	}
}

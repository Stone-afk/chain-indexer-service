//go:build e2e

package integration

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
)

// Keep the sanitizer fixture real at runtime without storing secret-shaped content in source.
var secretLikeHexFixture = strings.Repeat("0123456789abcdef", 2)

func TestLiveHarnessLifecycleIsolated(t *testing.T) {
	for _, tc := range []struct {
		name, target, env string
		fail, up          bool
	}{
		{"start-fail", "e2e-live-up", "DOCKER_START_FAIL=1", true, true}, {"cleanup-fail", "e2e-live-run", "DOCKER_FAIL_DOWN_AT=1", true, false}, {"preflight-fail", "e2e-live-run", "GO_PREFLIGHT_FAIL=1", true, false}, {"catalog-fail", "e2e-live-run", "GO_CATALOG_FAIL=1", true, false}, {"empty-catalog", "e2e-live-run", "GO_EMPTY_CATALOG=1", true, false}, {"suite-fail", "e2e-live-run", "GO_SUITE_FAIL=7", true, true}, {"suite-skip", "e2e-live-run", "GO_SUITE_SKIP=1", true, true}, {"not-run", "e2e-live-run", "GO_SUITE_NOT_RUN=1", true, true}, {"success", "e2e-live-run", "GO_STATUS=PASS", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := isolatedHarness(t, tc.env)
			r := runMake(t, d, tc.target)
			if (r.err != nil) != tc.fail {
				t.Fatalf("err=%v out=%s", r.err, r.out)
			}
			c := read(t, filepath.Join(d, "calls.log"))
			if strings.Contains(c, " up ") != tc.up {
				t.Fatalf("up=%v calls=%s", tc.up, c)
			}
			if b, e := os.ReadFile(filepath.Join(d, "status")); e == nil && bytes.Contains(b, []byte("{")) {
				t.Fatal("raw json")
			}
		})
	}
}

func TestLiveHarnessSummaryMutations(t *testing.T) {
	d := isolatedHarness(t, "")
	for _, x := range []struct{ n, s, v string }{{"duplicate", "LE-01 result=PASS duration_ms=1 request_credit_estimate=1 stable_error_class=none\nLE-01 result=PASS duration_ms=1 request_credit_estimate=1 stable_error_class=none\n", "LIVE_STATUS="}, {"extra", "LE-01 result=PASS duration_ms=1 request_credit_estimate=1 stable_error_class=none extra\n", "LIVE_STATUS="}, {"wrong-phase", "LE-01 result=PASS duration_ms=1 request_credit_estimate=1 stable_error_class=none\n", "LIVE_PHASE_A_STATUS="}, {"invalid", "LE-01 result=PASS duration_ms=x request_credit_estimate=1 stable_error_class=none\n", "LIVE_STATUS="}, {"multiline", "LE-01 result=PASS duration_ms=1 request_credit_estimate=1 stable_error_class=none\nsecret\n", "LIVE_STATUS="}} {
		p := filepath.Join(d, x.n)
		if err := os.WriteFile(p, []byte(x.s), 0600); err != nil {
			t.Fatal(err)
		}
		if r := runMake(t, d, "e2e-live-summary", x.v+p); r.err == nil {
			t.Errorf("accepted %s", x.n)
		}
	}
	p := filepath.Join(d, "valid")
	if err := os.WriteFile(p, []byte("LE-01 result=PASS duration_ms=1 request_credit_estimate=1 stable_error_class=none\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r := runMake(t, d, "e2e-live-summary", "LIVE_STATUS="+p)
	if r.err != nil || strings.Count(r.out, "LE-36 ") != 1 {
		t.Fatalf("%v %s", r.err, r.out)
	}
}

func TestLiveHarnessLedgerOwnsTerminalActions(t *testing.T) {
	for _, tc := range []struct {
		name, env, first string
		fail             bool
	}{
		{"pass", "GO_STATUS=PASS", "PASS", false},
		{"skip", "GO_SUITE_SKIP=1", "FAIL", true},
		{"fail", "GO_SUITE_FAIL=7", "FAIL", true},
		{"package", "GO_SUITE_NOT_RUN=1", "NOT_RUN", true},
		{"subtest", "GO_SUBTEST_ONLY=1", "NOT_RUN", true},
		{"duplicate", "GO_SUITE_DUPLICATE=1", "NOT_RUN", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := isolatedHarness(t, tc.env)
			r := runMake(t, d, "e2e-live-run")
			if (r.err != nil) != tc.fail {
				t.Fatalf("result=%v output=%s", r.err, r.out)
			}
			status := read(t, filepath.Join(d, "status"))
			if !strings.HasPrefix(status, "LE-01 result="+tc.first+" ") {
				t.Fatalf("terminal result=%s", status)
			}
			if strings.Count(status, "LE-") != 13 || strings.Contains(status, "LE-14 ") {
				t.Fatalf("phase B ownership=%s", status)
			}
			calls := read(t, filepath.Join(d, "calls.log"))
			if strings.Count(calls, "-json") != 1 || !strings.Contains(calls, "-run ^TestInfura") {
				t.Fatalf("not one Infura JSON run: %s", calls)
			}
		})
	}
}

func TestLiveHarnessPhaseALedgerOwnership(t *testing.T) {
	d := isolatedHarness(t, "GO_STATUS=PASS")
	r := runMake(t, d, "e2e", "CHAIN_INDEXER_LIVE_E2E=0")
	if r.err != nil {
		t.Fatalf("local ledger: %v %s", r.err, r.out)
	}
	status := read(t, filepath.Join(d, "phase-a"))
	if strings.Count(status, "LE-") != 22 || !strings.HasPrefix(status, "LE-14 result=PASS ") || !strings.Contains(status, "LE-35 result=PASS ") || strings.Contains(status, "LE-13 ") {
		t.Fatalf("phase A ownership=%s", status)
	}
	calls := read(t, filepath.Join(d, "calls.log"))
	if strings.Count(calls, "-json") != 1 || strings.Contains(calls, " -run ") || strings.Contains(calls, "GOFLAGS=-run") || strings.Contains(calls, "-failfast") {
		t.Fatalf("local suite was filtered: %s", calls)
	}
	for _, test := range []string{"TestMigrationUpStatusDownUp", "TestInitialSchemaCatalogMatchesPDF", "TestCanonicalAndWorkerFailureRollback", "TestLiveLE14StartBlockBoundedScan", "TestLocalForkConfirmationBoundary", "TestWorkerPersistenceTransactionAndNative"} {
		if !strings.Contains(calls, "json-test "+test) {
			t.Fatalf("full local suite omitted %s: %s", test, calls)
		}
	}
}

func TestLiveHarnessFailureDiagnosticsSanitized(t *testing.T) {
	for _, tc := range []struct {
		name, target, status, parent string
		args                         []string
	}{
		{"phase-a", "e2e", "phase-a", "TestLiveLE14StartBlockBoundedScan", []string{"CHAIN_INDEXER_LIVE_E2E=0"}},
		{"phase-b", "e2e-live-run", "status", "TestInfuraLE02EndpointMatrix", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := isolatedHarness(t, "GO_DIAGNOSTIC_FAIL=1")
			tmp := filepath.Join(d, "tmp")
			if err := os.Mkdir(tmp, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("TMPDIR", tmp)
			stdout, stderr, err := runMakeSplit(t, d, tc.target, tc.args...)
			if err == nil || !strings.Contains(stderr, "Error 7") {
				t.Fatalf("original go exit lost: err=%v stderr=%s", err, stderr)
			}
			for _, name := range []string{tc.parent, tc.parent + "/assertion", tc.parent + "/secret-unquoted", tc.parent + "/secret-single", tc.parent + "/secret-double"} {
				if !strings.Contains(stderr, "diagnostic test="+name) {
					t.Fatalf("failed test identity missing: %s", stderr)
				}
			}
			if !strings.Contains(stderr, "safe assertion marker") {
				t.Fatalf("safe assertion missing: %s", stderr)
			}
			for _, safe := range []string{"safe unquoted", "safe single", "safe double"} {
				if !strings.Contains(stderr, safe) {
					t.Fatalf("safe secret assertion prefix %q missing: %s", safe, stderr)
				}
			}
			for _, assertion := range []string{"later output assertion survived", "single quoted suffix assertion survived", "double quoted suffix assertion survived"} {
				if !strings.Contains(stderr, assertion) {
					t.Fatalf("safe assertion after secret %q missing: %s", assertion, stderr)
				}
			}
			for _, secret := range []string{"postgres://", "two word secret", "private_service", "private_app", "http://", "https://", "ws://", "wss://", secretLikeHexFixture, "unquoted value,tail;suffix", "single value,tail;suffix", "double value,tail;suffix"} {
				if strings.Contains(stderr, secret) {
					t.Fatalf("diagnostic leaked %q: %s", secret, stderr)
				}
			}
			for _, r := range stderr {
				if (r < 32 && r != '\n') || r == 127 {
					t.Fatalf("diagnostic retained control %#U", r)
				}
			}
			if len(stderr) > 5000 {
				t.Fatalf("diagnostic is unbounded: %d bytes", len(stderr))
			}
			if strings.Contains(stdout, "diagnostic test=") {
				t.Fatalf("diagnostic was not written to stderr: %s", stdout)
			}
			for _, line := range strings.Split(strings.TrimSpace(read(t, filepath.Join(d, tc.status))), "\n") {
				if len(strings.Fields(line)) != 5 {
					t.Fatalf("ledger format changed: %q", line)
				}
			}
			entries, readErr := os.ReadDir(tmp)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("raw JSON temporary retained: %+v", entries)
			}
		})
	}

	for _, tc := range []struct {
		name, target string
		args         []string
	}{
		{"phase-a", "e2e", []string{"CHAIN_INDEXER_LIVE_E2E=0"}},
		{"phase-b", "e2e-live-run", nil},
	} {
		t.Run(tc.name+"-success", func(t *testing.T) {
			d := isolatedHarness(t, "GO_STATUS=PASS")
			stdout, stderr, err := runMakeSplit(t, d, tc.target, tc.args...)
			if err != nil {
				t.Fatalf("success failed: %v stdout=%s stderr=%s", err, stdout, stderr)
			}
			if strings.Contains(stdout+stderr, "diagnostic test=") {
				t.Fatalf("success emitted failure diagnostic: stdout=%s stderr=%s", stdout, stderr)
			}
		})
	}
}

func TestLiveHarnessFailureDiagnosticsSkipOwnership(t *testing.T) {
	t.Run("phase-a-ignores-non-owned-skip", func(t *testing.T) {
		d := isolatedHarness(t, "GO_DIAGNOSTIC_SKIP=1")
		_, stderr, err := runMakeSplit(t, d, "e2e", "CHAIN_INDEXER_LIVE_E2E=0")
		if err == nil {
			t.Fatalf("legacy failure unexpectedly passed: %s", stderr)
		}
		if !strings.Contains(stderr, "diagnostic test=TestLegacyIntegration output=legacy assertion failed") {
			t.Fatalf("legacy failure diagnostic missing: %s", stderr)
		}
		if strings.Contains(stderr, "TestInfuraLE02EndpointMatrix") || strings.Contains(stderr, "expected offline Infura skip") {
			t.Fatalf("non-owned Infura skip polluted Phase A diagnostics: %s", stderr)
		}
	})

	t.Run("phase-b-reports-owned-skip", func(t *testing.T) {
		d := isolatedHarness(t, "GO_DIAGNOSTIC_SKIP=1")
		_, stderr, err := runMakeSplit(t, d, "e2e-live-run")
		if err == nil {
			t.Fatalf("owned skip unexpectedly passed: %s", stderr)
		}
		if !strings.Contains(stderr, "diagnostic test=TestInfuraLE02EndpointMatrix output=owned Infura skip reason") {
			t.Fatalf("owned skip diagnostic missing: %s", stderr)
		}
		if !strings.Contains(read(t, filepath.Join(d, "status")), "LE-02 result=FAIL ") {
			t.Fatal("owned skip no longer maps to ledger FAIL")
		}
	})
}

func TestLiveHarnessWrapperAlwaysDown(t *testing.T) {
	for _, e := range []string{"GO_PREFLIGHT_FAIL=1", "DOCKER_FINAL_CLEANUP_FAIL=1", "SUMMARY_INVALID=1", "GO_STATUS=PASS"} {
		d := isolatedHarness(t, e)
		r := runMake(t, d, "e2e-live")
		if !strings.Contains(read(t, filepath.Join(d, "calls.log")), "down") {
			t.Fatal("down missing")
		}
		if !strings.Contains(r.out, "LE-36 ") {
			t.Fatal("summary missing")
		}
	}
}

func TestLiveHarnessTemplateAndOfflineGate(t *testing.T) {
	d := t.TempDir()
	k := strings.Repeat("a", 32)
	p := filepath.Join(d, "config.yaml")
	s := strings.ReplaceAll(read(t, filepath.Join(repoRoot(t), "internal/test/integration/testdata/infura_config.template.yaml")), "${INFURA_API_KEY}", k)
	if err := os.WriteFile(p, []byte(s), 0600); err != nil {
		t.Fatal(err)
	}
	c, e := config.LoadForService(p, func(n string) (string, bool) {
		if n == "POSTGRES_DSN" {
			return "postgres://dummy:dummy@127.0.0.1:5432/dummy", true
		}
		if n == "INFURA_API_KEY" {
			return k, true
		}
		return "", false
	})
	if e != nil || len(c.Chains) != 7 {
		t.Fatal(e)
	}
	re := regexp.MustCompile(`/v3/([0-9a-f]{32})$`)
	for n, x := range c.Chains {
		if !x.Enabled || x.ChainID == 0 || x.StartBlock == 0 || x.ConfirmBlocks == 0 || !strings.HasPrefix(x.HTTPURL, "https://") || !strings.HasPrefix(x.WSURL, "wss://") || len(re.FindStringSubmatch(x.HTTPURL)) != 2 {
			t.Fatalf("bad %s", n)
		}
	}
	if r := runMake(t, isolatedHarness(t, ""), "e2e-live-run"); r.err == nil {
		t.Fatal("offline gate missing")
	}
}

func TestInfuraPreflight(t *testing.T) {
	if os.Getenv("CHAIN_INDEXER_LIVE_E2E") != "1" {
		t.Skip("live opt-in required")
	}
	root := repoRoot(t)
	configPath := os.Getenv("CHAIN_INDEXER_LIVE_CONFIG")
	if configPath == "" {
		configPath = filepath.Join(root, "config/config.local.yaml")
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatal(err)
	}
	c, err := config.LoadForService(configPath, func(name string) (string, bool) {
		if name == "POSTGRES_DSN" {
			return "postgres://dummy:dummy@127.0.0.1:5432/dummy", true
		}
		return os.LookupEnv(name)
	})
	if err != nil {
		t.Fatalf("config invalid: %v", err)
	}
	want := map[string]struct {
		id      int64
		confirm int64
		host    string
	}{"ethereum": {1, 12, "mainnet.infura.io"}, "bsc": {56, 15, "bsc-mainnet.infura.io"}, "polygon": {137, 100, "polygon-mainnet.infura.io"}, "arbitrum": {42161, 20, "arbitrum-mainnet.infura.io"}, "optimism": {10, 20, "optimism-mainnet.infura.io"}, "base": {8453, 20, "base-mainnet.infura.io"}, "avalanche": {43114, 15, "avalanche-mainnet.infura.io"}}
	var firstKey string
	for name, w := range want {
		ch, ok := c.Chains[name]
		if !ok {
			t.Fatalf("chain %s missing", name)
		}
		if !ch.Enabled || ch.ChainID != w.id || ch.ConfirmBlocks != w.confirm || ch.StartBlock == 0 {
			t.Fatalf("chain %s invalid metadata", name)
		}
		hu, he := url.Parse(ch.HTTPURL)
		wu, we := url.Parse(ch.WSURL)
		mh := regexp.MustCompile(`^/v3/([0-9a-fA-F]{32})$`).FindStringSubmatch(hu.Path)
		mw := regexp.MustCompile(`^/ws/v3/([0-9a-fA-F]{32})$`).FindStringSubmatch(wu.Path)
		if he != nil || we != nil || hu.Scheme != "https" || wu.Scheme != "wss" || hu.Host != w.host || wu.Host != w.host || len(mh) != 2 || len(mw) != 2 || mh[1] != mw[1] || (firstKey != "" && firstKey != mh[1]) {
			t.Fatalf("chain %s invalid endpoint", name)
		}
		firstKey = mh[1]
	}
}

func TestLiveHarnessOfflinePreflightSkip(t *testing.T) {
	c := exec.Command(os.Args[0], "-test.run=^TestInfuraPreflight$", "-test.v")
	c.Env = append(os.Environ(), "CHAIN_INDEXER_LIVE_E2E=0")
	c.Dir = t.TempDir()
	var b bytes.Buffer
	c.Stdout, c.Stderr = &b, &b
	if err := c.Run(); err != nil {
		t.Fatalf("offline preflight failed: %v\n%s", err, b.String())
	}
	if !strings.Contains(b.String(), "SKIP") || strings.Contains(b.String(), "config/config.local.yaml") {
		t.Fatalf("unexpected preflight output: %s", b.String())
	}
}

func TestLiveHarnessPreflightRejectsInvalidLocalConfig(t *testing.T) {
	root := repoRoot(t)
	template := read(t, filepath.Join(root, "internal/test/integration/testdata/infura_config.template.yaml"))
	key := strings.Repeat("a", 32)
	valid := strings.ReplaceAll(strings.ReplaceAll(template, "${INFURA_API_KEY}", key), "${POSTGRES_DSN}", "postgres://dummy:dummy@127.0.0.1:5432/dummy")
	for _, tc := range []struct{ name, body string }{{"valid", valid}, {"missing", strings.Replace(valid, "  bsc:", "", 1)}, {"bad-key", strings.ReplaceAll(valid, key, strings.Repeat("b", 31))}} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			c := exec.Command(os.Args[0], "-test.run=^TestInfuraPreflight$", "-test.v")
			c.Env = append(os.Environ(), "CHAIN_INDEXER_LIVE_E2E=1", "CHAIN_INDEXER_LIVE_CONFIG="+p)
			var b bytes.Buffer
			c.Stdout, c.Stderr = &b, &b
			err := c.Run()
			if tc.name == "valid" && err != nil {
				t.Fatal(err)
			}
			if tc.name != "valid" && err == nil {
				t.Fatal("invalid config accepted")
			}
			if strings.Contains(b.String(), key) {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestLiveHarnessPrimaryAndCleanupFailure(t *testing.T) {
	d := isolatedHarness(t, "GO_SUITE_FAIL=7 DOCKER_FAIL_DOWN_AT=2")
	r := runMake(t, d, "e2e-live-run")
	if r.err == nil {
		t.Fatal("suite failure unexpectedly succeeded")
	}
	if ee, ok := r.err.(*exec.ExitError); !ok || ee.ExitCode() != 2 || !strings.Contains(r.out, "Error 7") {
		t.Fatalf("exit=%v out=%s", r.err, r.out)
	}
	assertStatusClass(t, d, "cleanup")
	if got := downCount(t, d); got != 2 {
		t.Fatalf("down count=%d", got)
	}
}

func TestLiveHarnessGitGateFailures(t *testing.T) {
	for _, env := range []string{"GIT_IGNORE_FAIL=1", "GIT_TRACKED=1"} {
		t.Run(env, func(t *testing.T) {
			d := isolatedHarness(t, env)
			r := runMake(t, d, "e2e-live-run")
			if r.err == nil {
				t.Fatal("config gate unexpectedly succeeded")
			}
			assertStatusClass(t, d, "config")
			if strings.Contains(read(t, filepath.Join(d, "calls.log")), "docker up") {
				t.Fatal("docker up called")
			}
		})
	}
}

func TestLiveHarnessWrapperSummaryAndFinalCleanupFailures(t *testing.T) {
	for _, env := range []string{"SUMMARY_INVALID=1 GO_STATUS=PASS", "DOCKER_FAIL_DOWN_AT=3 GO_STATUS=PASS"} {
		t.Run(env, func(t *testing.T) {
			d := isolatedHarness(t, env)
			if strings.Contains(env, "SUMMARY_INVALID") {
				p := filepath.Join(d, "phase-a")
				if err := os.WriteFile(p, []byte("bad\n"), 0600); err != nil {
					t.Fatal(err)
				}
				env += " LIVE_PHASE_A_STATUS=" + p
			}
			r := runMake(t, d, "e2e-live", strings.Fields(env)...)
			if r.err == nil {
				t.Fatal("wrapper unexpectedly succeeded")
			}
			if !strings.Contains(read(t, filepath.Join(d, "calls.log")), "down") {
				t.Fatal("down was not called")
			}
			if strings.Contains(env, "SUMMARY_INVALID") {
				assertStatusClassAt(t, filepath.Join(d, "wrapper"), "summary")
			} else {
				assertStatusClassAt(t, filepath.Join(d, "wrapper"), "cleanup")
			}
		})
	}
}

func TestLiveHarnessSignals(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		d := isolatedHarness(t, "GO_BLOCK_READY=1")
		c := exec.Command("make", "e2e-live")
		c.Dir = d
		c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		c.Args = append(c.Args, "LIVE_STATUS="+filepath.Join(d, "status"), "LIVE_WRAPPER_STATUS="+filepath.Join(d, "wrapper"), "LIVE_MANIFEST="+filepath.Join(d, "internal/test/integration/testdata/infura_samples.json"), "LIVE_PHASE_A_STATUS="+filepath.Join(d, "phase-a"))
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		waitFile(t, filepath.Join(d, "ready"))
		if err := syscall.Kill(-c.Process.Pid, sig.(syscall.Signal)); err != nil {
			t.Fatal(err)
		}
		if err := waitTimeout(c, 3*time.Second); err == nil {
			t.Fatal("wrapper unexpectedly succeeded")
		}
		if !strings.Contains(read(t, filepath.Join(d, "calls.log")), "down") {
			t.Fatal("wrapper did not clean up")
		}
		assertStatusClassAt(t, filepath.Join(d, "wrapper"), "signal")
	}
}

func TestLiveHarnessRepositorySecretGate(t *testing.T) {
	root := repoRoot(t)
	local := filepath.Join(root, "config/config.local.yaml")
	if _, err := os.Stat(local); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "check-ignore", "-q", "config/config.local.yaml")
	cmd.Dir = root
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("git", "ls-files", "--error-unmatch", "config/config.local.yaml")
	cmd.Dir = root
	if err := cmd.Run(); err == nil {
		t.Fatal("local config is tracked")
	}
	paths := []string{".gitignore", "Makefile", "config.example.yaml", "internal/test/integration/testdata/infura_config.template.yaml", "internal/test/integration/live_harness_test.go", "config/config.local.yaml"}
	secret := regexp.MustCompile(`[0-9a-fA-F]{32}`)
	for _, f := range paths[:5] {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		if secret.Match(b) {
			t.Fatal("secret-like content detected")
		}
	}
	for _, args := range [][]string{{"diff", "--"}, {"diff", "--cached", "--"}} {
		args = append(args, paths...)
		b, err := exec.Command("git", args...).Output()
		if err != nil {
			t.Fatal(err)
		}
		if secret.Match(b) {
			t.Fatal("secret-like content detected")
		}
	}
}

type result struct {
	out string
	err error
}

func runMake(t *testing.T, d, target string, v ...string) result {
	a := append([]string{}, v...)
	a = append(a, target)
	c := exec.Command("make", a...)
	c.Dir = d
	var b bytes.Buffer
	c.Stdout, c.Stderr = &b, &b
	e := c.Run()
	return result{b.String(), e}
}

func runMakeSplit(t *testing.T, d, target string, v ...string) (string, string, error) {
	t.Helper()
	a := append([]string{}, v...)
	a = append(a, target)
	c := exec.Command("make", a...)
	c.Dir = d
	var stdout, stderr bytes.Buffer
	c.Stdout, c.Stderr = &stdout, &stderr
	err := c.Run()
	return stdout.String(), stderr.String(), err
}

func assertStatusClass(t *testing.T, d, class string) {
	t.Helper()
	b := read(t, filepath.Join(d, "status"))
	if !strings.Contains(b, "stable_error_class="+class) {
		t.Fatalf("status class %s: %s", class, b)
	}
}
func assertStatusClassAt(t *testing.T, p, class string) {
	t.Helper()
	b := read(t, p)
	if !strings.Contains(b, "stable_error_class="+class) {
		t.Fatalf("status class %s: %s", class, b)
	}
}
func downCount(t *testing.T, d string) int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(d, "down.count"))
	if err != nil {
		t.Fatal(err)
	}
	n := strings.TrimSpace(string(b))
	var v int
	if _, err := fmt.Sscan(n, &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func waitFile(t *testing.T, p string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(p); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("ready timeout")
}
func waitTimeout(c *exec.Cmd, d time.Duration) error {
	ch := make(chan error, 1)
	go func() { ch <- c.Wait() }()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		_ = c.Process.Kill()
		return <-ch
	}
}
func isolatedHarness(t *testing.T, extra string) string {
	d := t.TempDir()
	r := repoRoot(t)
	b, e := os.ReadFile(filepath.Join(r, "Makefile"))
	if e != nil {
		t.Fatal(e)
	}
	if err := os.WriteFile(filepath.Join(d, "Makefile"), b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(d, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(d, "internal/test/integration/testdata"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "config/config.local.yaml"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "internal/test/integration/testdata/infura_samples.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(d, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct{ n, b string }{
		{"docker", "printf 'docker %s\\n' \"$*\" >> \"$CALLS\"; case \"$*\" in *' up '* ) test -z \"$DOCKER_START_FAIL\";; *' down '* ) n=$(cat \"$DOWN_COUNT_FILE\" 2>/dev/null || echo 0); n=$((n+1)); printf '%s\\n' \"$n\" >\"$DOWN_COUNT_FILE\"; test \"$n\" != \"$DOCKER_FAIL_DOWN_AT\";; esac"},
		{"git", "printf 'git %s\\n' \"$*\" >> \"$CALLS\"; case \"$1\" in check-ignore) test -z \"$GIT_IGNORE_FAIL\";; ls-files) test -n \"$GIT_TRACKED\" && printf '%s\\n' config/config.local.yaml && exit 0 || exit 1;; *) exit 1;; esac"},
		{"go", fakeGoJSON},
	} {
		if err := os.WriteFile(filepath.Join(bin, x.n), []byte("#!/bin/sh\n"+x.b+"\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(d, "calls.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CALLS", filepath.Join(d, "calls.log"))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	t.Setenv("CHAIN_INDEXER_LIVE_E2E", "1")
	t.Setenv("LIVE_STATUS", filepath.Join(d, "status"))
	t.Setenv("LIVE_PHASE_A_STATUS", filepath.Join(d, "phase-a"))
	t.Setenv("LIVE_WRAPPER_STATUS", filepath.Join(d, "wrapper"))
	t.Setenv("LIVE_MANIFEST", filepath.Join(d, "internal/test/integration/testdata/infura_samples.json"))
	t.Setenv("DOWN_COUNT_FILE", filepath.Join(d, "down.count"))
	t.Setenv("READY_FILE", filepath.Join(d, "ready"))
	t.Setenv("SECRET_LIKE_HEX_FIXTURE", secretLikeHexFixture)
	for _, n := range []string{"DOCKER_START_FAIL", "DOCKER_FAIL_DOWN_AT", "GO_PREFLIGHT_FAIL", "GO_CATALOG_FAIL", "GO_EMPTY_CATALOG", "GO_SUITE_FAIL", "GO_SUITE_SKIP", "GO_SUITE_NOT_RUN", "GO_STATUS", "GO_BLOCK_READY", "SUMMARY_INVALID", "GIT_IGNORE_FAIL", "GIT_TRACKED", "GO_SUBTEST_ONLY", "GO_SUITE_DUPLICATE", "GO_DIAGNOSTIC_FAIL", "GO_DIAGNOSTIC_SKIP", "GOFLAGS"} {
		t.Setenv(n, "")
	}
	for _, x := range strings.Fields(extra) {
		q := strings.SplitN(x, "=", 2)
		if len(q) == 2 {
			t.Setenv(q[0], q[1])
		}
	}
	return d
}

// fakeGoJSON emits only go-test events and never reads or writes a status
// ledger. Make owns extraction and atomic publication of every result.
const fakeGoJSON = `
printf 'go GOFLAGS=%s args=%s\n' "${GOFLAGS-}" "$*" >> "$CALLS"
emit_secret_failures() {
 printf '{"Action":"output","Test":"%s/secret-unquoted","Output":"safe unquoted api_key=unquoted value,tail;suffix"}\n' "$failed"
 printf '{"Action":"output","Test":"%s/secret-unquoted","Output":"later output assertion survived"}\n' "$failed"
 printf '{"Action":"fail","Test":"%s/secret-unquoted"}\n' "$failed"
 printf '{"Action":"output","Test":"%s/secret-single","Output":"safe single token: \\u0027single value,tail;suffix\\u0027 single quoted suffix assertion survived"}\n' "$failed"
 printf '{"Action":"fail","Test":"%s/secret-single"}\n' "$failed"
 printf '{"Action":"output","Test":"%s/secret-double","Output":"safe double secret = \\u0022double value,tail;suffix\\u0022 double quoted suffix assertion survived"}\n' "$failed"
 printf '{"Action":"fail","Test":"%s/secret-double"}\n' "$failed"
}
case "$*" in
 *'-list'*)
  test -z "$GO_CATALOG_FAIL" || exit 1
  printf 'TestInfuraPreflight\n'
  test "$GO_EMPTY_CATALOG" = 1 || printf 'TestInfuraLE02EndpointMatrix\n'
 ;;
 *'-json'*)
  if test "$GO_BLOCK_READY" = 1; then
   : >"$READY_FILE"
   trap 'exit 130' INT
   trap 'exit 143' TERM
   while :; do sleep 1; done
  fi
	  if test "$GO_DIAGNOSTIC_FAIL" = 1; then
	   padding=$(awk 'BEGIN { for (i=0;i<6000;i++) printf "x" }')
	   case "$*" in
	    *'-race'*)
	     failed=TestLiveLE14StartBlockBoundedScan
	     for test in TestMigrationUpStatusDownUp TestInitialSchemaCatalogMatchesPDF TestCanonicalAndWorkerFailureRollback; do
	      printf '{"Action":"pass","Test":"%s"}\n' "$test"
	      printf 'json-test %s\n' "$test" >> "$CALLS"
	     done
	     for test in TestLiveLE14StartBlockBoundedScan TestLiveLE15CheckpointResume TestLiveLE16CursorPaginationRestartIdempotency TestLiveLE17MultiChainIsolation TestLiveLE18DatabaseUniquenessSingleRowFact TestLiveLE19HTTP429TimeoutErrorPropagation TestLiveLE20WSDisconnectCancelCleanup TestLiveLE21ReorgEvidenceBoundary TestLocalForkConfirmationBoundary TestLocalForkSameHeightRollbackReplay TestLocalForkDeepAncestorBeforeCheckpoint TestLocalForkWorkerInterleavings TestLocalForkRestartRecovery TestLocalForkNoCommonAncestorFailClosed TestLocalForkReplayIdempotent TestLocalForkDuplicateOutOfOrderHeads TestWorkerPersistenceTransactionAndNative TestWorkerPersistenceERC20AndERC721 TestWorkerPersistenceERC1155 TestWorkerPersistenceNumericAddressAndMalformed TestWorkerPersistenceMixedLogsAndJSONLinkage TestWorkerPersistenceStateReplayAndPageAtomicity; do
	      printf 'json-test %s\n' "$test" >> "$CALLS"
	      if test "$test" = "$failed"; then
	       printf '{"Action":"output","Test":"%s/assertion","Output":"safe assertion marker postgres://user:pass@db:5432/name host=db password=\\u0027two word secret\\u0027 service=private_service application_name=private_app hash=%s http://example.invalid/path https://example.invalid/v3/id ws://example.invalid/socket wss://example.invalid/ws \\t\\u0001 %s"}\n' "$failed" "$SECRET_LIKE_HEX_FIXTURE" "$padding"
	       printf '{"Action":"fail","Test":"%s/assertion"}\n' "$failed"
	       emit_secret_failures
	       printf '{"Action":"output","Test":"%s","Output":"parent failed after assertion"}\n' "$failed"
	       printf '{"Action":"fail","Test":"%s"}\n' "$failed"
	      else
	       printf '{"Action":"pass","Test":"%s"}\n' "$test"
	      fi
	     done
	    ;;
	    *)
	     failed=TestInfuraLE02EndpointMatrix
	     printf '{"Action":"pass","Test":"TestInfuraPreflight"}\n'
	     printf '{"Action":"output","Test":"%s/assertion","Output":"safe assertion marker postgres://user:pass@db:5432/name host=db password=\\u0027two word secret\\u0027 service=private_service application_name=private_app hash=%s http://example.invalid/path https://example.invalid/v3/id ws://example.invalid/socket wss://example.invalid/ws \\t\\u0001 %s"}\n' "$failed" "$SECRET_LIKE_HEX_FIXTURE" "$padding"
	     printf '{"Action":"fail","Test":"%s/assertion"}\n' "$failed"
	     emit_secret_failures
	     printf '{"Action":"output","Test":"%s","Output":"parent failed after assertion"}\n' "$failed"
	     printf '{"Action":"fail","Test":"%s"}\n' "$failed"
	     i=3
	     while test "$i" -le 13; do
	      printf '{"Action":"pass","Test":"TestInfuraLE%02d"}\n' "$i"
	      i=$((i+1))
	     done
	    ;;
	   esac
	   exit 7
	  fi
	  if test "$GO_DIAGNOSTIC_SKIP" = 1; then
	   case "$*" in
	    *'-race'*)
	     printf '{"Action":"output","Test":"TestLegacyIntegration","Output":"legacy assertion failed"}\n'
	     printf '{"Action":"fail","Test":"TestLegacyIntegration"}\n'
	     printf '{"Action":"output","Test":"TestInfuraLE02EndpointMatrix","Output":"expected offline Infura skip"}\n'
	     printf '{"Action":"skip","Test":"TestInfuraLE02EndpointMatrix"}\n'
	     for test in TestLiveLE14StartBlockBoundedScan TestLiveLE15CheckpointResume TestLiveLE16CursorPaginationRestartIdempotency TestLiveLE17MultiChainIsolation TestLiveLE18DatabaseUniquenessSingleRowFact TestLiveLE19HTTP429TimeoutErrorPropagation TestLiveLE20WSDisconnectCancelCleanup TestLiveLE21ReorgEvidenceBoundary TestLocalForkConfirmationBoundary TestLocalForkSameHeightRollbackReplay TestLocalForkDeepAncestorBeforeCheckpoint TestLocalForkWorkerInterleavings TestLocalForkRestartRecovery TestLocalForkNoCommonAncestorFailClosed TestLocalForkReplayIdempotent TestLocalForkDuplicateOutOfOrderHeads TestWorkerPersistenceTransactionAndNative TestWorkerPersistenceERC20AndERC721 TestWorkerPersistenceERC1155 TestWorkerPersistenceNumericAddressAndMalformed TestWorkerPersistenceMixedLogsAndJSONLinkage TestWorkerPersistenceStateReplayAndPageAtomicity; do
	      printf '{"Action":"pass","Test":"%s"}\n' "$test"
	     done
	     exit 7
	    ;;
	    *)
	     printf '{"Action":"pass","Test":"TestInfuraPreflight"}\n'
	     printf '{"Action":"output","Test":"TestInfuraLE02EndpointMatrix","Output":"owned Infura skip reason"}\n'
	     printf '{"Action":"skip","Test":"TestInfuraLE02EndpointMatrix"}\n'
	     i=3
	     while test "$i" -le 13; do
	      printf '{"Action":"pass","Test":"TestInfuraLE%02d"}\n' "$i"
	      i=$((i+1))
	     done
	    ;;
	   esac
	   exit 0
	  fi
  if test "$GO_SUITE_NOT_RUN" = 1; then
   printf '{"Action":"pass","Package":"fixture"}\n'
   exit 0
  fi
  if test "$GO_SUBTEST_ONLY" = 1; then
   printf '{"Test":"TestInfuraPreflight/child","Action":"pass"}\n'
   exit 0
  fi
  if test "$GO_SUITE_DUPLICATE" = 1; then
   printf '{"Test":"TestInfuraPreflight","Action":"pass"}\n{"Test":"TestInfuraPreflight","Action":"pass"}\n'
   exit 0
  fi
  action=pass
  if test -n "$GO_SUITE_FAIL"; then action=fail
  elif test "$GO_SUITE_SKIP" = 1; then action=skip
  elif test "$GO_STATUS" != PASS; then exit 0
  fi
  case "$*" in *'-race'*)
	   for test in TestMigrationUpStatusDownUp TestInitialSchemaCatalogMatchesPDF TestCanonicalAndWorkerFailureRollback; do
	    printf '{"Test":"%s","Action":"pass"}\n' "$test"
	    printf 'json-test %s\n' "$test" >> "$CALLS"
	   done
   for test in TestLiveLE14StartBlockBoundedScan TestLiveLE15CheckpointResume TestLiveLE16CursorPaginationRestartIdempotency TestLiveLE17MultiChainIsolation TestLiveLE18DatabaseUniquenessSingleRowFact TestLiveLE19HTTP429TimeoutErrorPropagation TestLiveLE20WSDisconnectCancelCleanup TestLiveLE21ReorgEvidenceBoundary TestLocalForkConfirmationBoundary TestLocalForkSameHeightRollbackReplay TestLocalForkDeepAncestorBeforeCheckpoint TestLocalForkWorkerInterleavings TestLocalForkRestartRecovery TestLocalForkNoCommonAncestorFailClosed TestLocalForkReplayIdempotent TestLocalForkDuplicateOutOfOrderHeads TestWorkerPersistenceTransactionAndNative TestWorkerPersistenceERC20AndERC721 TestWorkerPersistenceERC1155 TestWorkerPersistenceNumericAddressAndMalformed TestWorkerPersistenceMixedLogsAndJSONLinkage TestWorkerPersistenceStateReplayAndPageAtomicity; do
	    printf 'json-test %s\n' "$test" >> "$CALLS"
    printf '{"Test":"%s","Action":"%s"}\n' "$test" "$action"
   done
   exit 0
  ;; esac
  printf '{"Test":"TestInfuraPreflight","Action":"%s"}\n' "$action"
  i=2
  while test "$i" -le 13; do
   printf '{"Test":"TestInfuraLE%02d","Action":"pass"}\n' "$i"
   i=$((i+1))
  done
  test -z "$GO_SUITE_FAIL" || exit "$GO_SUITE_FAIL"
 ;;
 *'TestInfuraPreflight'*) test -z "$GO_PREFLIGHT_FAIL" ;;
esac
`

func repoRoot(t *testing.T) string {
	w, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	return filepath.Clean(filepath.Join(w, "../../.."))
}
func read(t *testing.T, p string) string {
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

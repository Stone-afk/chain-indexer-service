E2E_COMPOSE := docker compose -p chain-indexer-service-e2e -f scripts/test_docker_compose.yml
CHAIN_INDEXER_LIVE_E2E ?= 0
LIVE_E2E_COMPOSE := docker compose -p chain-indexer-service-live-e2e -f scripts/test_docker_compose.yml
LIVE_STATUS ?= /tmp/chain-indexer-service-live-e2e.status
LIVE_PHASE_A_STATUS ?= /tmp/chain-indexer-service-e2e.status
LIVE_WRAPPER_STATUS ?= /tmp/chain-indexer-service-live-wrapper.status
LIVE_MANIFEST ?= internal/test/integration/testdata/infura_samples.json
LIVE_IDS := LE-01 LE-02 LE-03 LE-04 LE-05 LE-06 LE-07 LE-08 LE-09 LE-10 LE-11 LE-12 LE-13 LE-14 LE-15 LE-16 LE-17 LE-18 LE-19 LE-20 LE-21 LE-22 LE-23 LE-24 LE-25 LE-26 LE-27 LE-28 LE-29 LE-30 LE-31 LE-32 LE-33 LE-34 LE-35 LE-36

# Only terminal actions of exact top-level test names from the same JSON run
# may own a scenario result. Package/subtest actions and catalog output cannot.
define LIVE_LEDGER_SCRIPT
import json, os, re, sys, tempfile, unicodedata
phase, destination = sys.argv[1:3]
names = {
"TestLiveLE14StartBlockBoundedScan": 14,
"TestLiveLE15CheckpointResume": 15,
"TestLiveLE16CursorPaginationRestartIdempotency": 16,
"TestLiveLE17MultiChainIsolation": 17,
"TestLiveLE18DatabaseUniquenessSingleRowFact": 18,
"TestLiveLE19HTTP429TimeoutErrorPropagation": 19,
"TestLiveLE20WSDisconnectCancelCleanup": 20,
"TestLiveLE21ReorgEvidenceBoundary": 21,
"TestLocalForkConfirmationBoundary": 22,
"TestLocalForkSameHeightRollbackReplay": 23,
"TestLocalForkDeepAncestorBeforeCheckpoint": 24,
"TestLocalForkWorkerInterleavings": 25,
"TestLocalForkRestartRecovery": 26,
"TestLocalForkNoCommonAncestorFailClosed": 27,
"TestLocalForkReplayIdempotent": 28,
"TestLocalForkDuplicateOutOfOrderHeads": 29,
"TestWorkerPersistenceTransactionAndNative": 30,
"TestWorkerPersistenceERC20AndERC721": 31,
"TestWorkerPersistenceERC1155": 32,
"TestWorkerPersistenceNumericAddressAndMalformed": 33,
"TestWorkerPersistenceMixedLogsAndJSONLinkage": 34,
"TestWorkerPersistenceStateReplayAndPageAtomicity": 35}
owned = range(14,36) if phase == "a" else range(1,14)
def scenario_for(test):
    root = test.split("/",1)[0]
    if phase == "a":
        return names.get(root)
    match = re.fullmatch(r"TestInfuraLE(0[2-9]|1[0-3])(?:[A-Z][A-Za-z0-9_]*)?",root)
    return 1 if root == "TestInfuraPreflight" else int(match[1]) if match else None
results = {}
test_output = {}
failed_actions = {}
failed_order = []
if len(sys.argv) >= 4:
    with open(sys.argv[3], encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            test, action = event.get("Test"), event.get("Action")
            scenario = scenario_for(test) if isinstance(test,str) else None
            if isinstance(test,str) and action == "output" and isinstance(event.get("Output"),str):
                test_output.setdefault(test,[]).append(event["Output"])
            if isinstance(test,str) and (action == "fail" or action == "skip" and scenario in owned) and test not in failed_actions:
                failed_actions[test] = action
                failed_order.append(test)
            if not isinstance(test,str) or "/" in test or action not in ("pass","fail","skip"):
                continue
            if scenario not in owned:
                continue
            if scenario in results:
                raise ValueError("duplicate terminal scenario action")
            results[scenario] = "PASS" if action == "pass" else "FAIL"
directory = os.path.dirname(os.path.abspath(destination))
fd, temporary = tempfile.mkstemp(prefix=".live-ledger-", dir=directory)
try:
    with os.fdopen(fd,"w",encoding="utf-8") as output:
        for scenario in owned:
            result = results.get(scenario,"NOT_RUN")
            error = "test" if result == "FAIL" else "none"
            output.write(f"LE-{scenario:02d} result={result} duration_ms=unknown request_credit_estimate=unknown stable_error_class={error}\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary,destination)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
def sanitize(value):
    value = re.sub(r"(?i)\bpostgres(?:ql)?://[^\s\"'<>]+", "[REDACTED_DSN]", value)
    conninfo_value = r'''(?:'(?:\\.|[^'])*'|"(?:\\.|[^"])*"|(?:\\.|[^\s,;])+)'''
    conninfo = rf"(?i)\b(?:host|hostaddr|port|dbname|user|password|service|sslmode|application_name)\s*=\s*{conninfo_value}(?:\s+[a-z_][a-z0-9_]*\s*=\s*{conninfo_value})*"
    value = re.sub(conninfo, "[REDACTED_DSN]", value)
    value = re.sub(r"(?i)\b(?:https?|wss?)://[^\s\"'<>]+", "[REDACTED_URL]", value)
    secret = r"(?i)\b(?:api[_-]?key|token|secret)\s*[:=]\s*"
    value = re.sub(secret + r"'(?:\\.|[^'\\])*'", "[REDACTED_SECRET]", value)
    value = re.sub(secret + r'"(?:\\.|[^"\\])*"', "[REDACTED_SECRET]", value)
    value = re.sub(secret + r"[^\r\n]*", "[REDACTED_SECRET]", value)
    value = re.sub(r"(?i)(?<![0-9a-f])(?:0x)?[0-9a-f]{24,}(?![0-9a-f])", "[REDACTED_HEX]", value)
    value = "".join(" " if unicodedata.category(ch).startswith("C") else ch for ch in value)
    return re.sub(r"\s+", " ", value).strip()
if len(sys.argv) == 5 and (sys.argv[4] != "0" or any(results.get(scenario) != "PASS" for scenario in owned)):
    lines = []
    for test in failed_order:
        name = sanitize(test)[:256]
        detail = " ".join(filter(None,(sanitize(fragment) for fragment in test_output.get(test,[])))) or "test action " + failed_actions[test]
        lines.append(f"diagnostic test={name} output={detail[:1024]}")
    if not lines:
        lines.append("diagnostic test=go-test-json output=no failing test output was emitted")
    diagnostic = "\n".join(lines)
    if len(diagnostic) > 4096:
        diagnostic = diagnostic[:4075] + " [TRUNCATED]"
    print(diagnostic, file=sys.stderr)
if len(sys.argv) >= 4 and any(results.get(scenario) != "PASS" for scenario in owned):
    sys.exit(1)
endef
export LIVE_LEDGER_SCRIPT

.PHONY: e2e e2e-live-up e2e-live-run e2e-live-summary e2e-live-down e2e-live

e2e:
	@test "$(CHAIN_INDEXER_LIVE_E2E)" = 0
	@set -eu; status='$(LIVE_PHASE_A_STATUS)'; json=''; \
	python3 -c "$$LIVE_LEDGER_SCRIPT" a "$$status"; \
	cleanup() { $(E2E_COMPOSE) down -v --remove-orphans; }; \
	on_exit() { s=$$?; c=0; test -z "$$json" || rm -f "$$json"; cleanup || c=$$?; test "$$s" -ne 0 && exit "$$s"; exit "$$c"; }; \
	trap on_exit EXIT; trap 'exit 130' INT; trap 'exit 143' TERM; \
	cleanup; $(E2E_COMPOSE) up -d --wait; json=$$(mktemp); \
	set +e; CHAIN_INDEXER_LIVE_E2E=0 GOFLAGS= go test -json -race -shuffle=on -tags=e2e ./internal/test/integration/... -count=1 >"$$json"; rc=$$?; set -e; \
	ledger_rc=0; python3 -c "$$LIVE_LEDGER_SCRIPT" a "$$status" "$$json" "$$rc" || ledger_rc=$$?; \
	if test "$$rc" -ne 0; then exit "$$rc"; fi; test "$$ledger_rc" -eq 0

e2e-live-up:
	@set -eu; test "$(CHAIN_INDEXER_LIVE_E2E)" = 1; test -f config/config.local.yaml; test -f "$(LIVE_MANIFEST)"; $(LIVE_E2E_COMPOSE) up -d --wait

e2e-live-run:
	@set -eu; test "$(CHAIN_INDEXER_LIVE_E2E)" = 1; status='$(LIVE_STATUS)'; tmp=$$(mktemp "$$status.tmp.XXXXXX"); tmp_json=''; fail_class=test; \
	init() { for id in $(LIVE_IDS); do case "$$id" in LE-01|LE-02|LE-03|LE-04|LE-05|LE-06|LE-07|LE-08|LE-09|LE-10|LE-11|LE-12|LE-13) printf '%s result=NOT_RUN duration_ms=unknown request_credit_estimate=unknown stable_error_class=none\n' "$$id";; esac; done >"$$tmp" && mv "$$tmp" "$$status"; }; \
	mark_not_run_fail() { cls="$$1"; test -f "$$status" || return 0; awk -v cls="$$cls" 'NF==5 && $$1 ~ /^LE-[0-9][0-9]$$/ && $$2 != "result=PASS" { $$5="stable_error_class=" cls } { print }' "$$status" >"$$tmp" && mv "$$tmp" "$$status"; }; \
	cleanup() { $(LIVE_E2E_COMPOSE) down -v --remove-orphans >/dev/null 2>&1; }; \
	on_exit() { s=$$?; c=0; test -n "$$tmp_json" && rm -f "$$tmp_json"; cleanup || c=$$?; if test "$$s" -ne 0; then test "$$c" -ne 0 && fail_class=cleanup; mark_not_run_fail "$$fail_class"; exit "$$s"; fi; if test "$$c" -ne 0; then fail_class=cleanup; mark_not_run_fail cleanup; exit "$$c"; fi; if test ! -f "$$status" || grep -q 'result=NOT_RUN' "$$status"; then fail_class=catalog; mark_not_run_fail catalog; exit 1; fi; exit 0; }; \
	trap on_exit EXIT; trap 'fail_class=signal; exit 130' INT; trap 'fail_class=signal; exit 143' TERM; \
	init; if ! cleanup; then fail_class=cleanup; exit 1; fi; \
	if ! test -f config/config.local.yaml || ! git check-ignore -q config/config.local.yaml || test -n "$$(git ls-files --error-unmatch config/config.local.yaml 2>/dev/null || true)"; then fail_class=config; exit 1; fi; \
	if ! test -f "$(LIVE_MANIFEST)"; then fail_class=manifest; exit 1; fi; \
	if ! CHAIN_INDEXER_LIVE_E2E=1 LIVE_STATUS="$$status" go test -tags=e2e ./internal/test/integration/... -run '^TestInfuraPreflight$$' -count=1; then fail_class=test; exit 1; fi; \
	catalog_tmp=$$(mktemp); if ! go test -tags=e2e ./internal/test/integration/... -run '^$$' -list '^TestInfura' >"$$catalog_tmp"; then rm -f "$$catalog_tmp"; fail_class=catalog; exit 1; fi; if ! grep -v '^TestInfuraPreflight$$' "$$catalog_tmp" | grep -q '^TestInfura'; then rm -f "$$catalog_tmp"; fail_class=catalog; exit 1; fi; rm -f "$$catalog_tmp"; \
	if ! CHAIN_INDEXER_LIVE_E2E=1 $(MAKE) e2e-live-up; then fail_class=cleanup; exit 1; fi; \
	tmp_json=$$(mktemp); set +e; LIVE_STATUS="$$status" go test -json -tags=e2e ./internal/test/integration/... -run '^TestInfura' -count=1 >"$$tmp_json"; go_rc=$$?; set -e; \
	ledger_rc=0; python3 -c "$$LIVE_LEDGER_SCRIPT" b "$$status" "$$tmp_json" "$$go_rc" || ledger_rc=$$?; if test "$$go_rc" -ne 0; then fail_class=test; exit "$$go_rc"; fi; if test "$$ledger_rc" -ne 0; then fail_class=test; exit "$$ledger_rc"; fi

e2e-live-summary:
	@set -eu; awk -v b="$(LIVE_STATUS)" -v a="$(LIVE_PHASE_A_STATUS)" -v w="$(LIVE_WRAPPER_STATUS)" '\
	BEGIN { for(i=1;i<=13;i++) allow[sprintf("LE-%02d",i)]=1; for(i=14;i<=35;i++) allow[sprintf("LE-%02d",i)]=2; allow["LE-36"]=3; parse(b,1); parse(a,2); parse(w,3); for(i=1;i<=36;i++){ id=sprintf("LE-%02d",i); if(!(id in seen)){ result="NOT_RUN"; dur="unknown"; cred="unknown"; cls="none" } else { result=val[id,"result"]; dur=val[id,"duration_ms"]; cred=val[id,"request_credit_estimate"]; cls=val[id,"stable_error_class"] } printf "%s result=%s duration_ms=%s request_credit_estimate=%s stable_error_class=%s\n",id,result,dur,cred,cls } if(bad) exit 2 }\
	function parse(file,phase, line,n,f,id,k,q){ if(file=="") return; while((getline line < file)>0){ n=split(line,f," "); valid=(n==5 && f[1] ~ /^LE-[0-9][0-9]$$/ && (f[1] in allow) && allow[f[1]]==phase && f[2] ~ /^result=(PASS|FAIL|NOT_RUN)$$/ && f[3] ~ /^duration_ms=(unknown|[0-9]+)$$/ && f[4] ~ /^request_credit_estimate=(unknown|[0-9]+)$$/ && f[5] ~ /^stable_error_class=(none|config|manifest|catalog|test|cleanup|signal|summary)$$/); if(!valid){ bad=1 } else { id=f[1]; if(id in seen){ bad=1 } else { seen[id]=1; for(k=2;k<=5;k++){ split(f[k],q,"="); val[id,q[1]]=q[2] } } } } close(file) }'

e2e-live-down:
	@test "$(CHAIN_INDEXER_LIVE_E2E)" = 1; $(LIVE_E2E_COMPOSE) down -v --remove-orphans

e2e-live:
	@set -eu; primary=0; summary_validation=0; down=0; wrapper_tmp=$$(mktemp '$(LIVE_WRAPPER_STATUS).tmp.XXXXXX'); \
	write_wrapper() { printf 'LE-36 result=%s duration_ms=unknown request_credit_estimate=unknown stable_error_class=%s\n' "$$1" "$$2" >"$$wrapper_tmp" && mv "$$wrapper_tmp" '$(LIVE_WRAPPER_STATUS)'; }; \
	final_summary() { $(MAKE) e2e-live-summary; }; \
	finish_signal() { code="$$1"; trap - INT TERM EXIT; CHAIN_INDEXER_LIVE_E2E=1 $(MAKE) e2e-live-down >/dev/null 2>&1 || down=$$?; write_wrapper FAIL signal; final_summary || summary_validation=$$?; test "$$code" -eq 130 && exit 130; exit 143; }; \
	trap 'finish_signal 130' INT; trap 'finish_signal 143' TERM; \
	CHAIN_INDEXER_LIVE_E2E=1 $(MAKE) e2e-live-run || primary=$$?; \
	$(MAKE) e2e-live-summary >/dev/null 2>&1 || summary_validation=$$?; \
	CHAIN_INDEXER_LIVE_E2E=1 $(MAKE) e2e-live-down >/dev/null 2>&1 || down=$$?; \
	if test "$$primary" -ne 0; then write_wrapper FAIL test; elif test "$$summary_validation" -ne 0; then write_wrapper FAIL summary; elif test "$$down" -ne 0; then write_wrapper FAIL cleanup; else write_wrapper PASS none; fi; \
	final_summary; final=$$?; if test "$$primary" -ne 0; then exit "$$primary"; elif test "$$summary_validation" -ne 0; then exit "$$summary_validation"; elif test "$$down" -ne 0; then exit "$$down"; else exit "$$final"; fi

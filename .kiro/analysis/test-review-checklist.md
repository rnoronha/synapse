# Test Review Checklist

**Origin:** The read-after-write test hung on every attempt due to 3 compounding issues:
hardcoded 3600s `sustained` condition ignoring `--duration`, missing `--duration` passthrough
in `deploy-and-test.sh`, and SSM timeout too short for the actual runtime. Devil's advocate
reviews then found CRITICAL gaps in all 8 test scripts. This checklist prevents recurrence.

**Rule:** Every test script MUST pass this checklist before being run on EC2 infrastructure.
Automated checks (1–3) are enforced by `scripts/validate-test.sh`. Manual checks (4–5) are
performed after the first run.

---

## 1. Pre-Run Validation

- [ ] **Compiles:** `python3 -c "import py_compile; py_compile.compile('scripts/test_X.py', doraise=True)"` succeeds
- [ ] **--help works:** `python3 scripts/test_X.py --help` exits 0 and prints usage
- [ ] **Quick exit:** `python3 scripts/test_X.py <url> --duration 10` exits within 120s (not hang forever)
- [ ] **No hardcoded durations that override --duration:** grep for `override_dur`, hardcoded sleep values > 60s, or conditions that ignore the duration flag
- [ ] **Signal handling:** script handles SIGTERM/SIGINT gracefully (prints partial results, exits non-zero)

## 2. Assertion Validation

- [ ] **Has pass/fail criteria:** script can exit non-zero on failure (grep for `sys.exit(1)` or equivalent)
- [ ] **No vacuous passes:** zero results → FAIL, not silent PASS. Check: `if st['total'] == 0: ... FAIL`
- [ ] **Thresholds are explicit:** pass/fail thresholds defined as constants at module level, not buried in logic
- [ ] **Errors cause failure:** connection errors, storm errors, and exceptions during test conditions cause non-zero exit
- [ ] **Broad exception catches are deliberate:** any `except Exception` is at a service boundary with explicit error counting, not inside utility functions silently swallowing failures

## 3. Infrastructure Validation (deploy-and-test.sh)

- [ ] **Test is in the case statement:** `deploy-and-test.sh` has a case for this test name
- [ ] **All required flags passed:** `--duration`, `--output`, `--reader`, `--concurrency` etc. are forwarded from deploy-and-test.sh args to the test command
- [ ] **SSM timeout is sufficient:** `SSM_TIMEOUT` accounts for total test runtime. Formula:
  - Single-condition tests: `DURATION + 300`
  - Multi-condition tests: `NUM_CONDITIONS × DURATION + 300`
  - Tests with startup overhead: add startup time
- [ ] **Reader URL passed when readers > 0:** if the test needs a reader URL, the case block checks `$READERS` and passes `--reader` / `--reader-ports`
- [ ] **Results fetched from file:** test writes to `--output /tmp/test-results.json`, deploy-and-test.sh fetches it (immune to 24KB SSM stdout cap)

## 4. Results Validation (post-run, manual)

- [ ] **Non-zero iteration count:** every condition ran at least some iterations
- [ ] **Numbers are physically plausible:**
  - Latencies > 0 (not all exactly 0.000)
  - Throughput within 10x of target rate
  - Error rate < 1% for healthy tests
  - p99 < 10× p50 (no extreme outlier domination)
- [ ] **No suspicious patterns:**
  - All conditions identical → test may not be varying load
  - 100% consistency with 0 staleness → reads may be hitting writer, not readers
  - Throughput exactly matches rate limit → test is bottlenecked on sleep, not measuring real perf
- [ ] **JSON output is valid:** `python3 -m json.tool results/X-results.json` succeeds
- [ ] **Verdict line present:** output contains `VERDICT: PASS` or `VERDICT: FAIL`

## 5. Cross-Run Validation (after ≥2 runs)

- [ ] **Results are reproducible:** key metrics within 20% across runs on same instance type
- [ ] **No monotonic degradation:** metrics don't get worse on every successive run (would indicate resource leak or data accumulation)
- [ ] **Branch comparison is apples-to-apples:** same instance type, same data snapshot, same `--duration`, same `--readers`
- [ ] **Outlier runs investigated:** if one run differs by >2× from others, check cortex log for errors, OOM, or throttling

---

## Quick Reference: The Three Bugs That Caused the RAW Hang

| Bug | Layer | How checklist catches it |
|-----|-------|------------------------|
| `sustained` condition hardcoded to 3600s, ignoring `--duration` | Test script | §1: "No hardcoded durations that override --duration" |
| `deploy-and-test.sh` didn't pass `--duration` to RAW test | Infrastructure | §3: "All required flags passed" |
| SSM timeout = `DURATION + 300` but RAW runs 4× DURATION | Infrastructure | §3: "SSM timeout is sufficient" (multi-condition formula) |

## When to Run This Checklist

1. **New test script:** full checklist before first EC2 run
2. **Modified test script:** sections 1–2 + section 3 if args changed
3. **Modified deploy-and-test.sh:** section 3 for affected tests
4. **After every EC2 run:** sections 4–5

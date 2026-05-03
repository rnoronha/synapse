# Devil's Advocate Review: `scripts/test_read_after_write.py`

**Date:** 2026-05-03
**Reviewer:** Kiro (adversarial test review)
**Verdict:** The script is a characterization/observability tool, not a correctness test. It cannot fail. This is the central problem.

---

## 1. DOES IT TEST WHAT IT CLAIMS?

### Claim (docstring): "Read-after-write consistency characterization test"

**[CRITICAL] It characterizes but never asserts.** There is not a single `assert`, threshold check, or exit-code-based pass/fail in the entire script. It prints statistics and writes JSON. A 50% miss rate produces the same exit code (0) as a 100% consistency rate. This is a measurement tool masquerading as a test.

**[HIGH] `READ_TIMEOUT` is defined but never used.** Line 25 defines `READ_TIMEOUT = READONLY_REFRESH_PERIOD + 2.0` (7.0s), suggesting the author intended a hard timeout on the retry loop. But `_write_and_read` uses a fixed `BACKOFF_DELAYS` list that sums to 10.85s — exceeding the intended timeout. The timeout concept was abandoned mid-implementation.

**[MEDIUM] Docstring claims "measures staleness distribution across load conditions" but the `sustained` condition is hardcoded to 3600s.** The `--duration` flag is ignored for `sustained` (line 30: `override_dur=3600`). This is documented nowhere in `--help`. A user running `--duration 60` for a quick check will be surprised by a 1-hour final condition.

### Claim-to-code mapping:

| Claim | Code | Backed by assertion? |
|-------|------|---------------------|
| Measures staleness distribution | `_compute_stats` computes p50/p99/max | No — printed only |
| Across load conditions | `CONDITIONS` list with 4 entries | No — no comparison between conditions |
| Writes routed to writer are immediately visible | Not tested | **Not tested at all** |
| Reads routed to reader may be stale up to 5s | Retry loop implicitly measures this | No assertion that staleness ≤ 5s |

---

## 2. EDGE CASES MISSED

**[CRITICAL] No control over routing.** The test uses a single Telepath URL. Both `callStorm` (write) and `storm` (read) go through the same proxy. Whether the read hits a reader process or the writer process depends entirely on the `QueryRouter` behind that proxy. The test has **zero visibility into which process served the read**. A read that hits the writer will always return immediately, inflating the consistency rate and masking reader staleness.

**[HIGH] No test of the boundary condition.** The LMDB readonly refresh is every 5.0s. The interesting case is a write that lands just *after* a refresh — maximum staleness. The test uses random timing with rate-limited iteration, making it statistically unlikely to consistently hit the worst case. A targeted test would write, wait 4.9s, then read from a known reader.

**[HIGH] Single connection.** `_run_condition` opens one Telepath connection and serializes all writes and reads through it. Under the `high` condition (50/s target), the serial write→retry-read cycle means actual throughput will be far below 50/s. The test doesn't measure or report actual achieved rate, so the "high load" condition is a fiction.

**[MEDIUM] No concurrent writers.** Real multi-process deployments have concurrent writes from multiple clients. This test is strictly serial — write one node, read it back, repeat. It cannot detect bugs triggered by concurrent write interleaving (e.g., a reader refresh that picks up write A but misses write B from the same batch).

**[MEDIUM] FQDN collision across runs.** Node names use `int(time.time())` as a timestamp (line 68). Two runs started in the same second will generate identical FQDNs. Since `inet:fqdn` is a primary key, the second run's "write" is a no-op (node already exists), and the read will always succeed immediately — a guaranteed false pass.

**[LOW] No unicode/special character testing.** All FQDNs are ASCII. Doesn't exercise normalization paths.

---

## 3. FALSE PASS SCENARIOS

**[CRITICAL] The test cannot report FAIL.** There is no failure condition. Even if every single read misses, the script prints `0.00% consistent` and exits 0. Any CI integration would report green.

**[HIGH] Retry loop masks permanent data loss.** If a write is acknowledged by `callStorm` but the data is silently lost (e.g., nexus replay failure, layer corruption), the retry loop will spin for 10.85s and then record `found=False`. But this is reported as a staleness measurement, not a data loss event. The output conflates "stale read" with "lost write" — two fundamentally different failure modes.

**[HIGH] `_storm_has_node` only checks for node existence, not correctness.** It returns `True` on the first `node` message. It doesn't verify the node's properties, timestamps, or that it's the *correct* node. If the system returns a different node matching the query (shouldn't happen for FQDN, but the pattern is fragile), it's a false pass.

**[MEDIUM] Broad exception swallowing.** Line 119: `except Exception as exc: print(f'  Error: {exc}')` catches and continues on any error during a condition run. A connection failure, auth error, or server crash mid-condition is logged as a print statement and the test moves on. Partial results are silently treated as valid.

**[MEDIUM] `_storm_has_node` silently returns False on errors.** If the `storm` generator raises an exception (timeout, connection reset), the `async for` loop exits and the function returns `False`. This is indistinguishable from "node not found yet" — the retry loop continues as if the read was merely stale.

---

## 4. METHODOLOGY

**[HIGH] No warmup.** The first iteration of each condition includes connection establishment overhead, JIT-like effects in the Cortex, and cold LMDB page cache. This is mixed into the same statistics as steady-state measurements. For the `low` condition (1/s), the first few samples are a significant fraction of total samples in a short run.

**[HIGH] Percentile calculation is hand-rolled and only computed over stale reads.** `_compute_stats` filters to `s > 0` before computing percentiles (line 82). This means p50/p99 describe "how stale were the stale reads" — not "what staleness does a random read experience." If 99% of reads are instant and 1% are stale, p99 of the stale subset is meaningless for characterizing system behavior. The useful metric (p99 of *all* reads) is not computed.

**[MEDIUM] No statistical significance assessment.** The `low` condition at 1/s for 300s produces ~300 samples. The `sustained` condition at 10/s for 3600s produces ~36000. These have wildly different confidence intervals, but results are presented identically. No confidence intervals, no standard deviation, no indication of whether observed differences between conditions are signal or noise.

**[MEDIUM] Rate limiting is best-effort.** The sleep-based rate limiter (lines 73-75) doesn't account for event loop scheduling jitter. Under the `high` condition, if each iteration takes >20ms (1/50), the rate limiter never sleeps and actual rate is determined by iteration latency. The test doesn't report achieved vs. target rate.

**[LOW] `statistics` module is imported but never used.** The hand-rolled `pct()` function duplicates `statistics.quantiles()` (Python 3.10+).

---

## 5. MISSING TESTS

**[CRITICAL] No pass/fail criteria.** Add threshold-based assertions:
- Consistency rate must be ≥ X% (e.g., 99.9%)
- p99 staleness must be ≤ READONLY_REFRESH_PERIOD + margin
- Zero permanent misses (found=False after full retry)
- Exit code 1 on threshold violation

**[CRITICAL] No reader-targeted reads.** The test should connect directly to reader URLs (not just the router) to measure actual reader staleness in isolation. Without this, the test is measuring a mix of writer-direct reads and reader reads with unknown proportions.

**[HIGH] No write-then-immediate-read-on-known-reader test.** The most important scenario — write to writer, immediately read from a specific reader, measure exactly how long until visible — is not tested. This is the one thing the docstring promises.

**[HIGH] No data integrity verification.** After the test completes, there's no sweep to verify all written nodes still exist. A transient "found" during retry doesn't guarantee durability.

**[HIGH] No cleanup.** The test creates thousands of `inet:fqdn` nodes and never removes them. Repeated runs pollute the Cortex. This also means the test can't be run in production-like environments without side effects.

**[MEDIUM] No test of the `SYN_SLAB_READONLY_REFRESH` env var.** The refresh period is configurable via environment variable, but the test hardcodes 5.0s. If the Cortex is running with a different value, the backoff delays and timeout assumptions are wrong.

**[MEDIUM] No connection failure / reconnection testing.** What happens if a reader process dies mid-test? The `ReaderManager` has health-check and respawn logic, but this test can't exercise or observe it.

**[LOW] No JSON schema validation on output.** The output JSON structure is ad-hoc. No schema, no versioning.

---

## Summary

| Severity | Count | Key Theme |
|----------|-------|-----------|
| CRITICAL | 4 | No pass/fail, no routing control, can't detect failures |
| HIGH | 8 | False passes, missing boundary tests, methodology gaps |
| MEDIUM | 8 | Statistical rigor, error handling, environmental assumptions |
| LOW | 3 | Cosmetic, cleanup, minor |

**Bottom line:** This script is useful as an *observability probe* — run it, look at the numbers, form a human judgment. It is **not a test** in any meaningful sense. It cannot catch regressions, cannot gate a release, and cannot distinguish between "working correctly with expected staleness" and "silently losing data." To become a real test, it needs: (1) pass/fail thresholds, (2) direct reader connections, and (3) separation of "stale" from "lost."

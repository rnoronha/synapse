# Devil's Advocate Review: `scripts/test_pathological.py`

**Reviewer:** Kiro (AI agent, senior Python engineer role)
**Date:** 2026-05-03
**Branch:** phase2-multi-process

---

## 1. DOES IT TEST WHAT IT CLAIMS?

The module docstring claims **16 categories** of worst-case inputs. The actual test count is **15 steps** (1a, 1b, 2a, 2b, 3, 4, 5, 6, 7, 8a, 8b, 8c, 9, 10a, 10b, 11). That's at most 11 distinct categories, not 16. **[MEDIUM]**

| Claimed Category (docstring) | Actual Test | Assertions? |
|---|---|---|
| Regex backtracking | 1a, 1b | ❌ 1a: no assertion — "Completed" if no timeout. 1b: no assertion on count value |
| Exponentiation | 2a, 2b | ❌ No assertion on computed value |
| Concurrent blocking | 3 | ⚠️ Reports ratio but never fails — "BLOCKED" is in the detail string, not a pass/fail decision |
| Broad lifts | 4 | ❌ No assertion on count or rate |
| Compression | 5 | ❌ No assertion that length == 10000 |
| Yield budgets | 6, 7 | ❌ No assertion on expected count |
| Type normalization | 8a, 8b, 8c | ❌ Checks count > 0 implicitly (no exception), never checks the normalized value |
| List operations | 10a, 10b | ⚠️ 10a returns unique count but never asserts it == 500. 10b returns len but never asserts it == 10000 |
| Hashing | 11 | ❌ Never asserts the hash is correct |
| Filter regex | 9 | ❌ No assertion on count == 1 |

**Summary:** Every single test uses the same pass criterion: "did it complete without exception or timeout?" None verify correctness of results. The docstring implies these are *tests*; they are actually *smoke/liveness checks*. **[CRITICAL]**

## 2. EDGE CASES MISSED

### 2.1 Regex (1a, 1b)

- **[HIGH]** 1a constructs a 5001-char string and tests `^(a+)+$` against it. But the string ends with `b`, so the regex simply fails to match on the first character scan — it does NOT trigger catastrophic backtracking. The regex engine sees the `b` at the end and rejects quickly. A true backtracking bomb would use a string like `"a" * 5000` (no trailing `b`) where the engine must explore all partitions. The test is testing the wrong thing.
- **[MEDIUM]** 1b uses `~=` (lift by regex) but doesn't test the pathological case of regex on a *large result set*. It lifts `inet:fqdn` matching `^host[0-9]+` which is a simple regex. No backtracking risk.

### 2.2 Exponentiation (2a, 2b)

- **[HIGH]** The test checks `2^5000` but the real pathological case is `10^(10^7)` or similar — numbers that blow up memory, not just CPU. `2^5000` is ~1500 digits, trivial for Python. If the Cortex has no exponentiation limit, this test won't find it.
- **[MEDIUM]** No test for negative exponents, float exponents, or `0^0`.

### 2.3 Concurrent Blocking (3)

- **[HIGH]** The test uses a single Telepath connection (`prox`) for both the heavy query and the probe. If the connection is multiplexed, this tests connection-level concurrency, not Cortex-level concurrency. A real test needs two independent connections.
- **[HIGH]** `asyncio.sleep(0.1)` is a race condition. On a fast Cortex, the heavy query may complete before the probe starts. On a slow host, 0.1s may not be enough for the heavy query to begin. The test is timing-dependent and non-deterministic.
- **[MEDIUM]** The baseline measurement includes connection overhead on first call (no warmup).

### 2.4 Seed Data

- **[MEDIUM]** Seed check only looks at `inet:fqdn` count ≥ 100. If a previous run partially seeded (e.g., FQDNs but not geo:place), the test skips seeding entirely. Subsequent geo/interval tests run against missing data and silently pass with count=0.
- **[LOW]** `random.Random(42)` for geo data is deterministic, which is good, but the seed data doesn't include any edge-case coordinates (exactly ±90 lat, ±180 lon, 0/0).

### 2.5 Unicode / Special Characters

- **[MEDIUM]** 8b tests IDN FQDNs (`münchen`, `café`) but never asserts the punycode normalization is correct. It just checks count.
- **[HIGH]** No test for null bytes, control characters, or injection-style inputs in Storm queries (e.g., `$lib.print("}")` or strings containing Storm syntax).

### 2.6 Large Result Sets

- **[MEDIUM]** Test 4 lifts all `inet:fqdn` (~1500 nodes). That's not pathological for a database. 100K+ would be.

## 3. FALSE PASS SCENARIOS

### 3.1 The Universal False Pass: "Completed = Passed" **[CRITICAL]**

Every test follows this pattern:
```python
async def test_X():
    # run some storm query
    return 'Completed (some description)'
```
The `run_step` wrapper marks `passed = True` if the function returns without exception. This means:

- **Regex test passes even if the regex matched when it shouldn't** (or vice versa)
- **Exponentiation passes even if it returns the wrong value**
- **Hashing passes even if SHA256 returns garbage**
- **Normalization passes even if the URL/FQDN/CPE is stored incorrectly**
- **List unique() passes even if it returns 10000 instead of 500**

A Cortex that silently corrupts every computation would pass this entire suite.

### 3.2 Timeout Masking Failures **[HIGH]**

If a test times out, it's marked FAIL. But the *inverse* is the problem: a test that should be slow (indicating a bug like missing index) but happens to finish within 30s on fast hardware will pass. The timeout is a ceiling, not a correctness check.

### 3.3 Concurrent Blocking Reports but Doesn't Fail **[HIGH]**

Test 3 computes a ratio and puts "responsive" or "BLOCKED" in the detail string. But `step.passed` is always `True` regardless. A completely blocked Cortex passes this test as long as it doesn't timeout.

### 3.4 Count Without Value Checking **[HIGH]**

- Test 1b: `count` nodes matched — but what if the regex matched the *wrong* nodes?
- Test 6: interval scan returned N nodes — but what if they have wrong `.seen` values?
- Test 7: geospatial returned N nodes — but what if they're outside the 500km radius?
- Test 9: 1 node matched — but what if it's the wrong node?
- Test 10a: returns unique count — but what if `unique()` returned duplicates that happen to be the right length?

### 3.5 Seed Data Verification Gap **[MEDIUM]**

`_seed_if_empty` checks `inet:fqdn | count` but the `count` command in Storm emits a `print` message, not a `node` message. `_count_storm` counts `node` messages. So `existing` will always be 0 from the `count` command, and the actual node count from the bare lift is what gets counted. Wait — re-reading: `inet:fqdn | count` pipes nodes into count. The `count` command consumes nodes and emits a print. So `_count_storm` would return 0 (no node messages pass through `count`). **This means seeding runs every time regardless of existing data.** If seeding is idempotent this is just slow; if not, it could corrupt state. **[HIGH]**

*Actually* — on further analysis, `inet:fqdn | count` in Storm consumes all nodes and emits a `print` message with the count. `_count_storm` counts `mesg[0] == 'node'` messages. Since `count` consumes nodes, `_count_storm` returns 0. The seed check `if existing >= 100` is **never true**. Seeding runs on every invocation. **[HIGH]**

## 4. METHODOLOGY

### 4.1 Benchmarking Concerns

- **[HIGH]** Test 3 (concurrent blocking) has no warmup. The baseline measurement is a single cold probe. Variance from connection warmup, JIT, or cache effects could dominate.
- **[HIGH]** Test 4 (broad lift throughput) takes a single measurement. No statistical significance — one sample tells you nothing about performance characteristics.
- **[MEDIUM]** No percentile reporting. A single `elapsed` time hides tail latency.
- **[MEDIUM]** Tests run sequentially. Earlier tests warm caches that benefit later tests. No randomization or isolation.

### 4.2 Correctness Methodology

- **[CRITICAL]** Zero assertions on return values. This is a liveness/crash test suite masquerading as a correctness test suite. The docstring says "PASS/FAIL per test" but the only failure mode is exception/timeout.

### 4.3 Measurement Approach

- **[MEDIUM]** `time.monotonic()` is fine for wall-clock, but tests measure end-to-end including network round-trip over Telepath. For pathological *input* testing, you want to isolate Cortex processing time from network overhead.

## 5. MISSING TESTS

The docstring claims 16 categories. Mapping what's present vs. what's missing:

### Present but Inadequate
All 11 categories listed in §1 are present but lack assertions (see §3.1).

### Completely Missing

| Missing Category | Severity | Rationale |
|---|---|---|
| **Memory exhaustion** — e.g., `$lib.list()` with millions of items, deeply nested dicts | **HIGH** | Docstring implies worst-case inputs; OOM is the #1 pathological risk |
| **Recursive/nested Storm** — deeply nested subqueries, recursive pivots | **HIGH** | Stack overflow or runaway recursion is a classic pathological case |
| **Write-path stress** — concurrent writes, large transactions, write during read | **HIGH** | Only reads are tested; write-path pathologies are equally important |
| **Auth/permission edge cases** — queries that scan across permission boundaries | **MEDIUM** | Multi-tenant Cortex could have pathological auth checks |
| **Tag operations** — large tag hierarchies, tag glob with deep wildcards | **MEDIUM** | Tags are a core Synapse concept; `#foo.**` on deep hierarchies is pathological |
| **Pivot storms** — `inet:fqdn -> inet:dns:a -> inet:ipv4` chains that fan out | **HIGH** | Pivot fan-out is the most common real-world pathological pattern |
| **Deconfliction/merge** — adding nodes that collide with existing normalized forms | **MEDIUM** | Type normalization tests create nodes but don't test collision behavior |
| **String operations at scale** — `$str.replace()`, `.split()`, `.strip()` on huge strings | **LOW** | Only `.ljust()` is tested |
| **Error path testing** — malformed inputs that should produce clean errors, not crashes | **HIGH** | No test verifies that bad input produces a proper error rather than a hang or crash |
| **Connection/session limits** — many concurrent Telepath connections | **MEDIUM** | Only one connection is used throughout |

---

## Summary of Findings by Severity

| Severity | Count | Key Issues |
|---|---|---|
| **CRITICAL** | 2 | Zero value assertions (all tests are liveness-only); docstring overclaims |
| **HIGH** | 10 | Regex test doesn't trigger backtracking; concurrent test can't actually fail; seed check is broken (`count` consumes nodes); no memory/recursion/write/error-path tests; count-without-value checking throughout |
| **MEDIUM** | 9 | Benchmark methodology (no warmup, single sample, no isolation); partial seed detection; timing-dependent concurrency; network overhead in measurements |
| **LOW** | 2 | Edge-case coordinates; string operation coverage |

## Verdict

This file is a **crash/hang detection suite**, not a correctness test suite. It answers one question: "does the Cortex survive these inputs without dying?" That has value, but the docstring, test names, and PASS/FAIL framing imply correctness testing that doesn't exist. The most urgent fix is the `_count_storm` + `count` bug (§3.5) which makes seed detection non-functional, followed by adding actual value assertions to every test.

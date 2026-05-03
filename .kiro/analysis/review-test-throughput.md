# Devil's Advocate Review: `scripts/test_throughput.py`

**Date:** 2026-05-03
**Branch:** phase2-multi-process
**Reviewer:** Kiro (adversarial test review)

---

## 1. DOES IT TEST WHAT IT CLAIMS?

**Claim (docstring):** "Concurrent read throughput benchmark for Synapse Cortex. Measures sequential vs concurrent query performance across multiple runs, reporting speedup ratios."

### What it actually does

| Claimed behavior | Actual code | Verdict |
|---|---|---|
| Measures sequential query time | `_run_sequential` — runs queries in a for-loop, times wall clock | ✅ Backed |
| Measures concurrent query time | `_run_concurrent` — `asyncio.gather` over queries, times wall clock | ⚠️ Partially backed (see §2, §4) |
| Reports speedup ratios | `seq_time / con_time` | ✅ Backed |
| Seeds test data (1000 nodes) | `_seed_nodes` with SEED_SPEC totaling 1000 | ❌ Broken (see F1 below) |
| Benchmark (implies correctness validation) | No assertions anywhere | ❌ Not backed |

### F1: Seeding existence check is broken — **HIGH**

`_seed_nodes` calls `_count_storm(prox, f'{form} | count')` to check existing node counts. But Storm's `count` command does NOT yield `node` messages — it yields a `print` message (`"Counted N nodes."`). Since `_count_storm` only counts `mesg[0] == 'node'`, it always returns 0. The `existing >= target` skip logic never fires.

**Impact:** Every run re-seeds all nodes. If the form has natural-key dedup (likely for `inet:fqdn`, `inet:ipv4`), this wastes time but doesn't corrupt data. If it doesn't, you get duplicate nodes and the `limit 50` queries may return different result sets across runs, adding noise.

**Fix:** Use `f'{form} | count --yield' | count` or simply `await prox.count(f'{form}')`.

### F2: No correctness assertions — **CRITICAL**

The script measures *time* but never validates *results*. There is zero verification that:
- Sequential and concurrent runs return the same node counts
- The expected number of nodes (50, 50, 20) are actually returned
- The returned nodes are correct (not empty, not error messages)

This means the benchmark can report a "speedup" even if the concurrent path silently drops results, returns errors, or returns wrong data.

---

## 2. EDGE CASES MISSED

### E1: Single-proxy connection pool saturation — **HIGH**

The Telepath `Proxy` maintains a link pool of 4–12 connections (`_links_min=4`, `_links_max=12`). With `--concurrency=20` (default), 20 concurrent `storm()` calls each acquire a pool link via `getPoolLink()`. When the pool is exhausted, `getPoolLink` falls through to `_initPoolLink()` which creates a new TCP connection synchronously. This means:

- The first ~4 concurrent calls get pre-warmed links (fast)
- The remaining ~16 calls create new TCP connections (slow, includes handshake)
- The benchmark is measuring **connection establishment overhead**, not query concurrency

The test never opens multiple proxies or configures the pool size, so it's benchmarking the pool's scaling behavior rather than the Cortex's read throughput.

### E2: All queries hit the same single proxy — **HIGH**

For a "multi-process" branch, the test uses a single `openurl()` connection. If the architecture routes reads to worker processes, a single proxy may pin to one worker. The test would show no speedup even if the multi-process read path works correctly.

### E3: Empty Cortex / insufficient data — **MEDIUM**

If seeding fails silently (e.g., permission error, form doesn't exist), all benchmark queries return 0 nodes. `_count_storm` returns 0, times are near-zero, and `speedup = 0/0` is handled as `0` — the test prints results and exits 0. No failure signal.

### E4: Shutdown race in warmup — **LOW**

If `_shutdown` is set during warmup, `warmup_time` is still computed and included in the JSON report, but it represents a partial warmup. The report doesn't indicate this.

### E5: `con_time = 0` division — **LOW**

`speedup = seq_time / con_time if con_time > 0 else 0` — if concurrent time is 0 (fast system, low-resolution timer), speedup is reported as 0 instead of infinity. This silently hides an extreme result.

---

## 3. FALSE PASS SCENARIOS

### FP1: Concurrent path silently drops queries — **CRITICAL**

`_run_concurrent` uses `asyncio.gather(*[_count_storm(prox, q) for q in queries])` with no `return_exceptions=True`. If any query raises an exception, `gather` propagates the *first* exception and **cancels the remaining tasks**. But the test doesn't check the return values at all — it only measures elapsed time.

Worse: if `gather` is changed to `return_exceptions=True` (a common "fix"), exceptions become return values that are silently ignored since the counts are never inspected.

**A Cortex that fails 19 out of 20 concurrent queries would produce a fast `con_time` and report a high "speedup".**

### FP2: Queries return fewer nodes than expected — **HIGH**

The queries use `limit 50` / `limit 20`, but nothing checks that 50/20 nodes are actually returned. If the Cortex returns 1 node per query due to a read-isolation bug, the benchmark still reports timing numbers and exits 0.

### FP3: Warmup masks cold-start failures — **MEDIUM**

The warmup runs both sequential and concurrent queries `args.warmup` times (default 5). If the Cortex crashes or deadlocks on the first concurrent call, the warmup hangs forever (no timeout). The user sees no output and assumes the system is slow, not broken.

### FP4: Exit code 0 on partial results — **MEDIUM**

If `_shutdown` is set after 1 of 3 runs, the script prints partial results and returns 0 (success). A CI system would see this as a passing test.

---

## 4. METHODOLOGY

### M1: Warmup is excessive but wrong — **HIGH**

The warmup runs `args.warmup` (default 5) full iterations of *both* sequential and concurrent queries. With concurrency=20, that's `5 × (20 + 20) = 200` query executions before measurement. This is excessive for LMDB page cache warming (which only needs each page read once), but it warms the **connection pool** — which means the measured runs benefit from pre-established connections that wouldn't exist in production cold-start scenarios.

The warmup should either:
- Warm only the data cache (run each unique query once)
- Or explicitly document that it's warming the connection pool too

### M2: No statistical rigor — **HIGH**

- Default 3 runs is insufficient for statistical significance
- No standard deviation, confidence intervals, or outlier detection
- The "average speedup" is computed as `mean(speedup_per_run)` rather than `mean(seq) / mean(con)`, which is mathematically different and more susceptible to outlier distortion
- No check for measurement stability (e.g., coefficient of variation)

### M3: Sequential and concurrent run different workloads — **MEDIUM**

`_build_query_rotation(20)` produces 20 queries cycling through 3 templates. Both sequential and concurrent run the same 20 queries, so the workload is matched. However, sequential runs them in order (predictable LMDB access pattern, cache-friendly) while concurrent runs them interleaved (random access pattern). The comparison conflates concurrency benefit with cache effects.

### M4: `time.monotonic()` includes GIL contention — **MEDIUM**

Wall-clock time includes Python GIL contention, event loop scheduling overhead, and telepath protocol framing. For a multi-process architecture, this may be acceptable (the GIL is the bottleneck you're trying to escape). But the test doesn't distinguish between "Cortex is faster" and "less GIL contention."

### M5: No baseline or regression detection — **LOW**

The benchmark produces numbers but has no threshold for pass/fail. There's no way to detect a performance regression — a 50% slowdown would still exit 0.

---

## 5. MISSING TESTS

### MT1: Result correctness validation — **CRITICAL**

After each run, compare sequential and concurrent result counts. Assert they match. Assert they match expected values (50, 50, 20 per query template).

### MT2: Multi-proxy concurrency — **HIGH**

Open N separate proxies (simulating N clients) and run concurrent queries across them. This tests the Cortex's actual multi-process read scaling, not the single-proxy connection pool.

### MT3: Error propagation — **HIGH**

Verify that if a query fails during concurrent execution, the failure is detected and reported (not silently swallowed by `gather`).

### MT4: Write-read contention — **HIGH**

Run concurrent reads while writes are happening. This is the real-world scenario for a multi-process Cortex and is completely untested here. (Note: `test_read_after_write.py` exists but tests a different thing — serialized read-after-write, not concurrent read-during-write.)

### MT5: Scaling curve — **MEDIUM**

Run with concurrency 1, 2, 4, 8, 16, 32 and report the scaling curve. A single concurrency=20 data point can't distinguish "linear scaling" from "cliff at 12 connections."

### MT6: Per-query timing — **MEDIUM**

Report individual query latencies, not just aggregate time. A single slow query in the concurrent batch would be hidden by the aggregate timing.

### MT7: Resource leak detection — **LOW**

After the benchmark, verify that all pool links are returned, no file descriptors are leaked, and the proxy is cleanly closed. The `async with` handles the happy path, but signal interruption during `gather` could leak links.

---

## Summary

| # | Finding | Severity | Category |
|---|---|---|---|
| F1 | Seeding existence check always returns 0 (`count` doesn't yield nodes) | HIGH | Broken logic |
| F2 | No correctness assertions anywhere | CRITICAL | False pass |
| FP1 | `gather` silently drops failed concurrent queries | CRITICAL | False pass |
| FP2 | No validation that queries return expected node counts | HIGH | False pass |
| E1 | Connection pool saturation at concurrency > 12 | HIGH | Measures wrong thing |
| E2 | Single proxy may pin to one worker process | HIGH | Measures wrong thing |
| M1 | Warmup warms connection pool, hiding cold-start behavior | HIGH | Methodology |
| M2 | 3 runs, no stddev, wrong averaging formula | HIGH | Methodology |
| MT1 | Missing: result correctness validation | CRITICAL | Missing test |
| MT2 | Missing: multi-proxy concurrency | HIGH | Missing test |
| MT3 | Missing: error propagation verification | HIGH | Missing test |
| MT4 | Missing: write-read contention | HIGH | Missing test |
| FP3 | Warmup hangs forever on deadlock (no timeout) | MEDIUM | False pass |
| FP4 | Exit 0 on partial/interrupted results | MEDIUM | False pass |
| E3 | Silent success on empty Cortex | MEDIUM | Edge case |
| M3 | Sequential vs concurrent have different cache access patterns | MEDIUM | Methodology |
| M4 | Wall-clock time conflates GIL and Cortex performance | MEDIUM | Methodology |
| MT5 | Missing: scaling curve across concurrency levels | MEDIUM | Missing test |
| MT6 | Missing: per-query latency breakdown | MEDIUM | Missing test |
| E4 | Partial warmup reported without indication | LOW | Edge case |
| E5 | Speedup=0 when con_time=0 | LOW | Edge case |
| M5 | No pass/fail threshold for regression detection | LOW | Methodology |
| MT7 | Missing: resource leak detection | LOW | Missing test |

**Bottom line:** This script is a timing harness, not a test. It can report "2x speedup" when the concurrent path is silently failing, returning wrong data, or only exercising a single worker process. The three CRITICAL findings (no assertions, silent gather failures, missing correctness validation) mean this benchmark cannot be trusted to validate the multi-process read path.

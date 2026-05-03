# Devil's Advocate Review: `scripts/test_parallel_reads.py`

**Date:** 2026-05-03
**Branch:** phase2-multi-process
**File:** `scripts/test_parallel_reads.py`

---

## 1. Does It Test What It Claims?

**Rating: HIGH**

The docstring claims: *"High-concurrency parallel read benchmark … measuring wall time, queries/sec, and per-query latency percentiles."*

It partially delivers. The structural problems:

**a) `_count_storm` with `| count` is broken — seeding never skips.**

`_seed_nodes` calls:
```python
existing = await _count_storm(prox, f'{form} | count')
```

`_count_storm` counts messages where `mesg[0] == 'node'`. But Storm's `count` command *consumes* all inbound nodes and emits a `print` message (`"Counted N nodes."`). It does not yield `node` messages unless `--yield` is passed. So `_count_storm` always returns 0, the `existing >= target` guard never fires, and seeding runs unconditionally every invocation.

This doesn't invalidate the benchmark results (nodes get created either way, duplicates are idempotent in Synapse), but it means:
- The "skipping" log line is dead code — it can never execute.
- Every run re-ingests all seed data, adding ~30s of unnecessary I/O before the benchmark.
- The `existing` variable is a lie — it's always 0.

Fix: replace `f'{form} | count'` with just `f'{form} | limit {target}` in `_count_storm`, or parse the `print` message from `count`.

**b) "Parallel reads" are not truly parallel at the protocol level.**

All queries go through a single Telepath proxy (`prox`). `asyncio.gather` fires N coroutines, but they share one TCP connection and are multiplexed by the event loop. This measures *async concurrency on a single connection*, not *parallel reads across multiple connections or processes*. For a branch named `phase2-multi-process`, this is a significant gap — it doesn't exercise the multi-process read path at all.

**c) Latency measurement includes Telepath overhead, not just Cortex read time.**

`_timed_query` measures wall time from `storm()` call to full async iteration. This includes: network round-trip, Telepath message serialization/deserialization, event loop scheduling contention from N concurrent coroutines, and Cortex query execution. The benchmark conflates all of these. It's measuring "end-to-end query latency from a single client" — which is useful, but not what "parallel read performance" typically implies.

---

## 2. Edge Cases Missed

**Rating: MEDIUM**

| Edge Case | Status | Impact |
|-----------|--------|--------|
| Empty result set queries | Missing | A query returning 0 nodes has near-zero Cortex work but still has Telepath overhead. Including one would separate protocol cost from read cost. |
| Large result set queries | Missing | All queries use `limit 20-50`. Real workloads include unbounded scans. Latency characteristics change dramatically at 10k+ nodes (backpressure, memory, streaming). |
| Concurrent writes during reads | Missing | The benchmark seeds data, then reads. It never tests reads *while writes are happening* — the core concern for multi-process MVCC. |
| Connection failure mid-benchmark | Missing | If the Cortex drops the connection during an iteration, `_timed_query` raises an unhandled exception inside `asyncio.gather`. The entire benchmark crashes with no partial results saved. |
| Single-query baseline | Missing | No iteration runs concurrency=1 to establish a baseline. Without it, you can't compute speedup or detect contention. |

---

## 3. False Pass Scenarios

**Rating: CRITICAL**

The benchmark has **no pass/fail criteria**. It always "passes" — it prints numbers and exits 0 (unless it crashes). There are no assertions, no regression thresholds, no comparison to a baseline.

Concrete false-pass scenarios:

**a) Complete serialization goes undetected.** If the Cortex serializes all reads (e.g., a lock contention bug), wall time ≈ sum of latencies. QPS drops to ~1. The benchmark prints these numbers but doesn't flag them. A human might notice, but in CI or automated runs, exit code 0 means "passed."

**b) Degraded p99 is invisible.** If p99 latency is 100x the p50, the benchmark prints both but doesn't alert. For a "parallel reads" benchmark, a p99 blowup is the primary signal of lock contention or resource starvation — exactly what you'd want to catch.

**c) Seeding failure is silent.** If seeding fails (e.g., auth error, form doesn't exist), the `async for _ in prox.storm(chunk): pass` loop silently consumes error messages. The benchmark proceeds with 0 nodes, all queries return empty results in microseconds, and the numbers look great.

**d) Query errors are counted as fast successes.** If a query fails (syntax error, permission denied), `storm()` yields an `err` message, not a `node` message. `_count_storm` returns 0 and `_timed_query` returns a very short latency. Errors look like fast queries.

---

## 4. Methodology Sound?

**Rating: HIGH**

**a) `random.choice` without seeding makes results non-reproducible.** The query mix varies between runs. Two runs with identical parameters will execute different query sequences, making A/B comparison unreliable. Should use `random.Random(seed)` with a fixed or configurable seed.

**b) Warmup doesn't verify effectiveness.** The warmup runs N iterations but doesn't check that latencies stabilized. If the Cortex needs 10 warmup iterations but only 2 are configured, the first measured iterations include warmup noise. A proper warmup would run until p50 variance drops below a threshold, or at minimum, discard the first measured iteration if it's an outlier.

**c) QPS calculation is misleading.** `qps = concurrency / wall_time` assumes all queries ran for the full wall time. In reality, some finish early and sit idle. This is "offered load" not "sustained throughput." True QPS would be `total_queries_completed / wall_time` — which happens to be the same formula here since all queries do complete, but the interpretation is wrong. If queries were rate-limited or had timeouts, this formula would break.

**d) Percentile calculation on small samples.** With default concurrency=64, you're computing p99.9 from 64 data points. The 99.9th percentile of 64 values is interpolated between the 63rd and 64th sorted values — it's essentially just the max. The p99.9 column is meaningless noise at this sample size. You need ~1000 data points for p99.9 to be statistically meaningful.

**e) No statistical significance.** 5 iterations of 64 queries each = 320 total queries. No confidence intervals, no standard deviation, no coefficient of variation. The "Avg" row treats all iterations as equally valid with no outlier detection.

---

## 5. Missing Tests

**Rating: HIGH**

For a benchmark in a `phase2-multi-process` branch, these are conspicuously absent:

| Missing Test | Why It Matters |
|-------------|----------------|
| **Multi-connection concurrency** | The benchmark uses one Telepath connection. Multi-process reads imply multiple reader processes with separate connections. Should open N proxies and distribute queries across them. |
| **Read-under-write** | The core MVCC question: do reads degrade when a writer is active? Should run concurrent writes during the read benchmark and compare latencies to read-only baseline. |
| **Scaling curve** | Run at concurrency 1, 2, 4, 8, 16, 32, 64, 128. Plot QPS vs concurrency. A linear curve = good parallelism. A plateau or decline = contention. Single concurrency=64 run tells you nothing about scaling behavior. |
| **Result correctness validation** | No query checks that results are correct. A read benchmark should verify that returned node counts match expected counts (from seeding). Otherwise you're benchmarking how fast the system returns wrong answers. |
| **Tail latency under sustained load** | 5 iterations is a burst test. A sustained load test (minutes, not seconds) would reveal GC pauses, LMDB map resizes, transaction refresh effects, and memory leaks. |
| **Error rate tracking** | No counting of `err` messages from storm. Error rate is a first-class metric for any benchmark. |

---

## Summary

| Question | Rating | Key Finding |
|----------|--------|-------------|
| 1. Tests what it claims? | **HIGH** | Broken seeding check (dead code path). Single-connection async, not multi-process parallel. Measures Telepath overhead, not isolated read performance. |
| 2. Edge cases missed? | **MEDIUM** | No empty results, no large scans, no concurrent writes, no connection failure handling. |
| 3. False pass scenarios? | **CRITICAL** | No pass/fail criteria at all. Errors look like fast successes. Serialization and contention go undetected. Exit 0 always. |
| 4. Methodology sound? | **HIGH** | Non-reproducible randomness. p99.9 meaningless at n=64. No statistical rigor. QPS semantics misleading. |
| 5. Missing tests? | **HIGH** | No multi-connection test, no read-under-write, no scaling curve, no correctness validation — all essential for a multi-process read benchmark. |

## Recommendations

1. **Immediate:** Fix `_count_storm` usage in `_seed_nodes` — the `| count` query never produces `node` messages. Either drop `| count` and count nodes directly, or parse the `print` message.
2. **Immediate:** Add error detection — check for `err` messages in `_count_storm` and `_timed_query`. A query that returns an error is not a successful read.
3. **Immediate:** Add a `--threshold-qps` and `--threshold-p99-ms` flag so the script can exit non-zero when performance regresses. Without this, the benchmark is informational only and cannot gate a CI pipeline.
4. **High priority:** Open multiple Telepath connections to actually test multi-process read parallelism. One connection through `asyncio.gather` tests event loop scheduling, not Cortex read scaling.
5. **High priority:** Add a correctness check — after seeding N nodes, verify that `inet:fqdn | limit 50` actually returns 50 nodes. If it returns 0, the benchmark numbers are meaningless.
6. **Medium priority:** Fix randomness reproducibility with a configurable seed. Add `--seed` argument defaulting to a fixed value.

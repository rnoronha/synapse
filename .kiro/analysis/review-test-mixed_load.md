# Devil's Advocate Review: `scripts/test_mixed_load.py`

**Reviewer:** Kiro (devil's advocate mode)
**Date:** 2026-05-03
**Branch:** phase2-multi-process

---

## 1. DOES IT TEST WHAT IT CLAIMS?

The docstring claims:

> Sustains --concurrency simultaneous operations for --duration seconds,
> with each operation randomly chosen as a read or write based on --write-pct.
> Reports per-type latency percentiles and optional JSON output.

### Claim-to-code mapping

| Claim | Backed by code? | Verdict |
|---|---|---|
| `--concurrency` simultaneous operations | `asyncio.Semaphore(args.concurrency)` gates `_run_op` | **Partially.** All operations share a single Telepath proxy. The semaphore limits in-flight tasks, but the proxy's internal connection serializes I/O. This is cooperative concurrency on one TCP socket, not true parallel load. |
| Random read/write selection by `--write-pct` | `random.randint(1, 100) <= write_pct` | **Yes**, but see §2 for off-by-one. |
| Per-type latency percentiles | `Stats` class tracks `read_lats` / `write_lats`, `_percentile()` computes p50/p99/p999 | **Yes.** |
| Duration-bounded | `while` loop checks `elapsed >= args.duration` | **Yes**, but see §2 for drain-time gap. |

### Unsubstantiated claims

**[CRITICAL]** This is a **benchmark**, not a **test**. The script always returns 0. There are **zero assertions, zero pass/fail thresholds, zero regression checks**. It cannot detect:
- Latency regression (no p99 ceiling)
- Error rate regression (no error-rate threshold)
- Throughput degradation (no minimum ops/sec check)
- Read/write interference (no comparison of read latency under write load vs. pure-read baseline)

The `deploy-and-test.sh` wrapper treats exit code 0 as PASS. A run with 100% errors, 0 ops/sec, and 10x latency regression **still passes**.

**Severity: CRITICAL** — The script's name starts with `test_` but it cannot fail. It provides a false sense of coverage.

---

## 2. EDGE CASES MISSED

### **[HIGH] Off-by-one in write percentage**

```python
is_write = random.randint(1, 100) <= write_pct
```

When `write_pct=0`, `randint(1,100)` is always ≥ 1, so `<= 0` is always False — correct.
When `write_pct=100`, always True — correct.
When `write_pct=10`, values 1–10 are writes = 10% — correct.

Actually fine on inspection. No off-by-one.

### **[HIGH] Drain phase inflates latency stats**

After the duration expires, the loop breaks but `pending` tasks are still in flight. These tasks were *started* during the test window but may complete arbitrarily late (e.g., if the Cortex is under load). Their latencies are recorded via `stats.snapshot()` after `asyncio.wait(pending)`, and folded into `all_read_lats` / `all_write_lats`. This inflates tail latencies (p99, p999) with drain-phase measurements that don't represent steady-state behavior.

### **[HIGH] Single-connection bottleneck masks multi-process benefit**

The entire test runs through one `s_telepath.openurl()` proxy. In the multi-process Cortex architecture (the whole point of `phase2-multi-process`), reads should be routed to reader processes. A single connection means:
- The proxy may pin to one server process
- No validation that the query router distributes reads across readers
- Concurrency is bounded by the proxy's internal message serialization, not by the Cortex's actual capacity

This is the most important thing a "mixed load" test for phase2 should validate, and it's completely absent.

### **[MEDIUM] `_seed_nodes` count parsing is fragile**

```python
async for mesg in prox.storm('inet:fqdn | count'):
    if mesg[0] == 'print' and mesg[1].get('mesg', '').isdigit():
        existing = int(mesg[1]['mesg'])
```

The `count` command emits a print message, but the format depends on the Storm runtime. If the message includes commas (e.g., `"1,000"`) or a prefix (e.g., `"Counted 500 nodes"`), `isdigit()` returns False and `existing` stays 0, causing unnecessary re-seeding. Not a correctness bug, but a reliability issue.

### **[MEDIUM] No warmup phase**

Unlike `test_throughput.py` which runs explicit warmup iterations, this script jumps straight from seeding to measurement. The first N operations hit cold LMDB page caches and inflate early latency numbers. For a benchmark claiming to measure steady-state mixed load, this biases results.

### **[LOW] `_make_write_query` generates collisions**

```python
ts = int(time.time())
r = random.randint(0, 0xFFFFFF)
```

`time.time()` has second granularity. At 64 concurrency with 10% writes, ~6.4 writes/sec share the same `ts`. With `r` drawn from 16M values, collision probability is low but nonzero. More importantly, `inet:ipv4` writes use `10.{a}.{b}.{c}` where `a = r % 256` — only 16M unique IPs, and the test runs for 60s+ at high throughput. Duplicate node creation is a no-op in Synapse (idempotent), so this doesn't cause errors, but it means write latency includes an unknown mix of "create new node" vs. "node already exists" — two very different code paths.

### **[LOW] Signal handler registered but `_shutdown` never checked in `_run_op`**

`_handle_signal` sets `_shutdown`, and the main loop checks it. But in-flight `_run_op` tasks don't check it — they'll complete their Storm query regardless. With 64 concurrent tasks, shutdown can take as long as the slowest query. Not a bug, but the "graceful shutdown" claim is aspirational.

---

## 3. FALSE PASS SCENARIOS

### **[CRITICAL] Always exits 0**

`main()` unconditionally returns 0. There is no failure path. Scenarios that should fail but don't:

1. **100% error rate**: Every Storm call throws, `stats.errors` = total_ops, exit 0.
2. **Zero throughput**: Cortex hangs, 0 ops complete in 60s, exit 0.
3. **Massive latency regression**: p99 goes from 50ms to 30s, exit 0.
4. **Connection failure during test**: Telepath proxy reconnects or throws — if it throws, the bare `except Exception` in `_run_op` swallows it, exit 0.
5. **Write-read inconsistency**: Writes succeed but subsequent reads don't see the data. The test doesn't check this at all.

### **[HIGH] Error swallowing hides failures**

```python
except Exception:
    await stats.record_error()
```

No logging, no traceback, no exception type. If the Cortex returns `IsReadOnly` (the exact error the multi-process architecture introduces for writes to readers), `AuthDeny`, `BadSyntax`, or the connection drops — all are silently counted as "+1 error". The final report shows `errors=N` with zero diagnostic value.

### **[MEDIUM] Stats snapshot clears data — partial window reporting**

`stats.snapshot()` clears `read_lats` and `write_lats`. The 10-second status prints show per-window p50, but the final report aggregates all windows. If a latency spike occurs in one window, it's diluted across the full run in the final percentiles. The per-window data is printed to stdout but not captured in the JSON output, so automated analysis loses the time-series signal.

---

## 4. METHODOLOGY SOUNDNESS

### **[CRITICAL] Not a test — it's a benchmark with no assertions**

The fundamental methodology problem: this script measures but never judges. A benchmark is useful for human analysis; a test must have pass/fail criteria. This file is named `test_mixed_load.py` and is invoked by `deploy-and-test.sh` as a test, but it's structurally incapable of detecting regressions.

### **[HIGH] Concurrency model doesn't match production**

Production multi-process Cortex serves multiple clients over multiple connections. This test uses one connection with async task fan-out. The semaphore creates the illusion of concurrency, but:
- All tasks share one TCP socket and one Telepath message stream
- The proxy serializes request/response pairs
- True concurrent load requires multiple `openurl()` connections or multiple processes

The test measures "how fast can one client pipeline requests" — not "how does the Cortex handle mixed concurrent load from multiple clients."

### **[MEDIUM] Read queries are trivial**

```python
READ_QUERIES = [
    'inet:fqdn | limit 10',
    'inet:ipv4 | limit 10',
    'inet:fqdn:zone=com | limit 5',
]
```

These are the simplest possible queries — sequential scans with small limits. They don't exercise:
- Pivot operations (the primary use case for multi-process read offloading)
- Tag filters
- Subqueries
- Large result sets
- Property comparisons

A mixed load test should include queries representative of actual workloads, especially the expensive reads that motivated the multi-process architecture.

### **[MEDIUM] Write queries don't verify persistence**

Writes create nodes but the test never reads them back. There's no read-after-write check. In a multi-process architecture where writes go to the leader and reads go to followers, verifying that writes propagate to readers is essential. This is the core correctness property of the system under test, and it's not tested.

### **[LOW] `random.random()` without seed**

No `random.seed()` call, so results aren't reproducible. Two runs with identical parameters will exercise different read/write mixes and different write queries. For a benchmark, reproducibility matters for regression detection.

---

## 5. MISSING TESTS

### **[CRITICAL] No multi-connection concurrency test**

The phase2 multi-process architecture routes reads to reader processes. A mixed load test should open N connections and verify that:
- Reads are distributed across readers (observable via process-level metrics)
- Writes are routed to the writer
- Read latency under write load doesn't degrade beyond a threshold

This is the entire value proposition of phase2, and this test doesn't exercise it.

### **[HIGH] No read-after-write consistency check**

Write a node, then immediately read it back. In a leader-follower architecture, this tests replication lag. The test should:
1. Write a uniquely-identifiable node
2. Read it back (possibly with retries)
3. Assert it exists within a bounded time window

### **[HIGH] No error-rate threshold**

Add a `--max-error-pct` flag (default: 1%). If `errors / total_ops > threshold`, exit 1. This is the minimum viable "test" behavior.

### **[HIGH] No latency regression threshold**

Add `--max-p99-ms` flag. If read or write p99 exceeds the threshold, exit 1. Without this, the test cannot detect performance regressions.

### **[MEDIUM] No write-starvation / read-starvation detection**

The random selection means write percentage varies per window. Over a 60s run with 10% writes, some 10s windows might have 5% writes and others 15%. The test should verify that the actual write percentage is within tolerance of the target, and that neither reads nor writes are starved.

### **[MEDIUM] No time-series latency output**

The per-window stats are printed to stdout but not included in the JSON report. For regression analysis, the JSON should include per-window latency percentiles so that latency spikes can be correlated with specific time ranges.

### **[LOW] No comparison mode**

`test_throughput.py` compares sequential vs. concurrent. This test should support a comparison mode: run pure-read baseline, then mixed load, and report the read latency delta. This directly measures write-induced read degradation — the key metric for the multi-process architecture.

---

## Summary

| Category | Severity | Count |
|---|---|---|
| CRITICAL | Always-pass (no assertions), not a real test, no multi-connection concurrency | 3 |
| HIGH | Error swallowing, single-connection bottleneck, drain-phase inflation, no RAW check, no error threshold, no latency threshold | 6 |
| MEDIUM | Fragile count parsing, no warmup, trivial queries, no write verification, stats window loss, no starvation detection, no time-series JSON | 7 |
| LOW | Write collisions, shutdown semantics, no random seed, no comparison mode | 4 |

**Bottom line:** This file is a benchmark harness, not a test. It measures latency and throughput but has zero pass/fail criteria, making it structurally incapable of catching regressions. More fundamentally, it uses a single Telepath connection, which means it doesn't exercise the multi-process read routing that is the entire purpose of the phase2 branch. Renaming it to `bench_mixed_load.py` and adding a separate `test_mixed_load.py` with assertions, multi-connection load, and read-after-write checks would address the critical gaps.

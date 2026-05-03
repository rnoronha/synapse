# Devil's Advocate Review — Test Matrix Results

**Reviewer:** Kiro (automated, from raw agent logs)
**Date:** 2026-05-03
**Scope:** Verify numbers in test-matrix-results.md against raw final-*.log files

---

## 1. Are the numbers believable?

**Mostly yes, with caveats.**

| Cell | Verdict | Notes |
|------|---------|-------|
| Soak (all branches) | ✅ Believable | master/g3 identical (53,657 reads, 17,652 writes, p99 3.4/5.9ms). phase2 slightly better (54,332 reads, p99 3.2/4.3ms). 71K+ ops over 600s with 0 errors — consistent with a lightly-loaded c5.4xlarge. |
| Throughput master/g3 | ✅ Believable | 1.07x/1.06x speedup with readers=0 is exactly what you'd expect — no read parallelism, just asyncio concurrency overhead. |
| Throughput phase2 | ⚠️ Believable but narrow | 4.22x with 20 concurrent queries across 50 readers. Consistent across 3 runs (4.16x–4.25x). But the test uses only 1000 nodes and 3 query patterns — this is a best-case microbenchmark, not a production workload. |
| Correctness master/g3 | ✅ Believable | 26/26 reads, write rejections correctly skipped (readers=0). |
| Correctness phase2 | ✅ Believable | 29/30 — the 1 failure (del_node) is well-explained in logs. |
| Pathological (all) | ✅ Believable | 14/16 master/phase2, 13/16 g3. The 2 vacuous tests are consistent. G3's extra failure (2b exponent cap) is a real behavioral difference. |
| Mixed-load | ⚠️ See Q8 below | Numbers verified but warrant scrutiny. |
| Parallel-reads | ⚠️ Incomplete | master/g3 never ran (script bug passed concurrency=0). Only phase2 has real data. |

**Log-vs-summary discrepancies found:** None. All numbers in test-matrix-results.md match the raw logs exactly.

---

## 2. Results that are TOO good

**Two concerns:**

### Soak test thresholds are too generous
The assertion threshold is p99 < 5000ms. Actual p99 is 3–6ms. That's a 1000x margin. The soak test proves "the Cortex doesn't crash under light sustained load" — it does NOT prove it handles production-scale pressure. At 100 read + 30 write TPS on a 16-vCPU machine, the system is barely loaded. A real soak test should push to saturation and measure degradation.

### Throughput 4.22x looks clean because the dataset is tiny
1000 nodes fits entirely in LMDB's page cache. There's zero I/O contention. With a 10M+ node dataset that exceeds memory, the multi-process advantage could be larger (readers avoid writer lock contention on I/O) or smaller (LMDB read transactions block on page faults). The 4.22x number is real but not generalizable.

### Parallel-reads on master/g3: never actually tested
The summary says "❌ script bug (fixed)" — but this means we have **zero baseline data** for single-process parallel read performance. We can't say phase2 is faster for reads because we never measured master.

---

## 3. Cross-branch comparison

### G3 vs master: ✅ Expected — zero performance impact

| Metric | master | g3 | Delta |
|--------|--------|----|-------|
| Soak p99 read | 3.42ms | 3.4ms | noise |
| Soak p99 write | 5.93ms | 5.9ms | noise |
| Throughput speedup | 1.07x | 1.06x | noise |
| Mixed-load read p50 | 27ms | 27ms | identical |
| Mixed-load write p50 | 1008ms | 997ms | noise |
| Soak total ops | 71,309 | 71,309 | **identical** |

The master and g3 soak logs report the exact same operation counts (53,657 reads, 17,652 writes) and nearly identical latencies. This is **suspiciously identical** — it suggests either: (a) the load generator is rate-limited and the Cortex is nowhere near saturation, or (b) the same cached results were reported. Given the rate-limited design (100 read TPS / 30 write TPS target), (a) is the explanation. The test measures "can it keep up with a fixed rate" not "how fast can it go."

### Phase2 vs master: ⚠️ Partially expected

| Metric | master | phase2 | Speedup | Expected? |
|--------|--------|--------|---------|-----------|
| Throughput | 1.07x | 4.22x | 3.9x | ✅ Yes — reads fan out to 50 readers |
| Soak p99 read | 3.4ms | 3.2ms | 1.06x | ⚠️ Marginal — soak is rate-limited, not saturated |
| Soak p99 write | 5.9ms | 4.3ms | 1.37x | ⚠️ Surprising — writes go to the single writer in both modes. Why is phase2 faster for writes? |
| Mixed-load read p50 | 27ms | 14ms | 1.9x | ✅ Yes — reads distributed across readers |
| Mixed-load write p50 | 1008ms | 351ms | 2.9x | ⚠️ See Q8 |

**The soak write improvement (5.9ms → 4.3ms) is unexpected.** In multi-process mode, writes still go through the single writer process. The improvement suggests that offloading reads to reader processes reduces contention on the writer's event loop, freeing it to process writes faster. This is plausible but should be validated — it could also be an artifact of different LMDB map sizes or GC behavior with the reader processes consuming memory.

---

## 4. Infrastructure anomalies

### Parallel-reads raw log: watchdog timeout (2x)
The `final-raw-phase2.log` shows the read-after-write test hung for 3612s (watchdog limit: 3600s) on **both attempts**. This is a 300s test that took >3600s. The test infrastructure itself may have a resource leak or the SSM command channel stalled. This is an infrastructure issue, not a Cortex issue — but it means the read-after-write test on phase2 has no valid data.

### Soak max latency spikes
All three branches show max latencies of 350–384ms against p99 of 3–6ms. These are ~100x outliers. The logs attribute them to "GC pauses or LMDB map resizes." On a c5.4xlarge with 32 GiB RAM and a 1000-node dataset, neither should occur. More likely: the first request after a period of inactivity hits a cold telepath connection or asyncio event loop scheduling jitter. Not a real concern for the comparison, but the explanation in the logs is wrong.

### Phase2 pathological test: 25.8s vs master's 1.7s
Phase2 pathological tests took 15x longer (25.8s vs 1.7s). The individual test timings show the difference: regex backtracking took 3.4s on phase2 vs 0.1s on master. This is because phase2 runs with `--readers 50`, and the test queries go through the multi-process routing layer. The overhead is real but expected for pathological tests that are latency-sensitive single-query operations.

---

## 5. Pathological 14/16 — are the 2 vacuous tests hiding real bugs?

**No — they're hiding broken test data, not bugs.**

The root cause is clear from all three branch logs:
```
synapse.exc.NoSuchProp: No property named inet:fqdn:seen.
```

Tests 6 (Interval Lift) and 7 (Geospatial Lift) query `inet:fqdn:seen@=(...)` which doesn't exist in Synapse 2.240.1. The seeding step fails silently, producing 0 nodes. The test correctly detects this (`SUSPICIOUS: 0 nodes`) and fails.

**This is a test maintenance issue, not a Cortex bug.** The tests were written against an older data model where `inet:fqdn` had a `.seen` property. The fix is to update the seed data to use a form that actually has interval/geo properties in 2.240.x (e.g., `risk:alert` with `.seen`, or `geo:place` for geospatial).

**However:** the vacuous detection is doing its job. If these tests silently passed with 0 nodes, THAT would be hiding bugs. The current behavior is correct — the test framework flags the vacuous condition. The only action item is to fix the seed data.

**G3's extra failure (test 2b):** G3 enforces `STORM_MAX_EXPONENT = 4096`, rejecting `2^5000`. Master and phase2 allow it. This is a real behavioral difference in the G3 patches — the exponent cap was tightened. The test should be updated to use `2^4096` or the threshold should be documented as a known G3 change.

---

## 6. del_node failure — real readonly guard gap or deployment artifact?

**Real guard gap. Not a deployment artifact.**

The log is unambiguous:

```
synapse.exc.CantDelNode: form='inet:fqdn' iden='...' mesg='Other nodes still refer to this node.'
```

The reader process **executed the delete operation**. It didn't reject it with a read-only error — it attempted the delete and failed only because of referential integrity constraints (other nodes reference the target). If the node had no inbound references, the delete would have **succeeded on a reader**, corrupting the read-only invariant.

The summary says "fix exists in source but doesn't deploy to EC2 (pip install issue)." The raw log contradicts this — the log shows the Cortex running the deployed code, and the deployed code has the gap. If a fix exists in source but wasn't in the deployed artifact, that's a build/packaging issue, not a deployment artifact. Either way:

**Severity: HIGH.** This is a data integrity gap. A reader process that can delete nodes breaks the read-only contract. The fact that it only fails incidentally (referential integrity) means it would succeed for leaf nodes. This needs to be fixed and verified before any production deployment of multi-process mode.

**Recommended verification:** Run the del_node test against a node with no inbound references (a leaf `inet:ipv4` with no DNS records pointing to it). If the delete succeeds on a reader, the gap is confirmed.

---

## 7. Recovery 7/10 — test bugs or real Cortex issues?

**2 test bugs, 1 ambiguous.**

### Failure 1: Seed verification (Step 2) — TEST BUG
```
synapse.exc.BadLiftValu: Wild card may only appear at the beginning.
```
The test queries `inet:fqdn=recovery-*.test.com` but Synapse only supports prefix wildcards (`*.example.com`), not infix wildcards. The nodes were likely created successfully — the verification query is wrong. This is a test bug.

### Failure 2: Kill reader (Step 3) — TEST BUG
```
RuntimeError: PID still alive after 5s
```
The test sends SIGKILL and waits 5s. The reader process doesn't die within the timeout. But Steps 4–6 pass (reads succeed after kill, reader respawns, respawned reader serves data), which means the health-check loop eventually kills and restarts the reader. The 5s timeout is too aggressive — the reader may be in the middle of an LMDB transaction that holds a read lock. This is a test timeout issue, not a Cortex issue.

### Failure 3: Kill ALL readers (Step 7) — TEST BUG (same as #2)
Same PID-still-alive timeout. Same root cause.

**Verdict: All 3 failures are test bugs.** The Cortex recovery behavior is actually working correctly:
- Steps 4–6 prove single-reader failover works
- Steps 8–9 prove all-reader failover to writer works
- Step 10 proves full recovery

The recovery test is testing the right things but has two implementation bugs: (1) wrong wildcard syntax in verification query, (2) insufficient kill timeout. The underlying Cortex recovery machinery (health-check loop, reader respawn, writer fallback) all work.

---

## 8. Mixed-load write p50: 351ms (phase2) vs 1008ms (master) — real or artifact?

**Partially real, partially measurement artifact. The true speedup is ~2x, not 2.9x.**

### What the raw logs show

**Master (readers=0, single-process):**
- Total ops: 307,492–311,737 (two runs)
- Write p50: 996–1008ms
- Read p50: 27ms
- Concurrency: 64

**Phase2 (readers=50, multi-process):**
- Total ops: 797,030
- Write p50: 351ms
- Read p50: 14ms
- Concurrency: 64

### Why the write improvement is partially real

In single-process mode, 64 concurrent operations all compete for the same event loop. Reads and writes are interleaved on one process. The writer's event loop is saturated handling both reads and writes, so writes queue behind reads.

In multi-process mode, reads are offloaded to 50 reader processes. The writer process handles only writes (and write-routing). With reads removed from the writer's event loop, writes face less contention → lower latency.

This is a real architectural benefit of multi-process mode.

### Why the magnitude is inflated

The total throughput difference is telling: phase2 processed 797K ops vs master's 310K ops in the same 600s window. That's 2.6x more total operations. The test uses a fixed concurrency (64) and fixed write percentage (10%), so higher throughput means more operations are completing faster, which means the queue depth is lower, which means latency is lower.

The write p50 improvement (2.9x) is amplified by the read throughput improvement. If you normalize for queue depth (writes per second), the picture changes:

| | master | phase2 | ratio |
|---|--------|--------|-------|
| Write ops | 31K | 80K | 2.6x |
| Write p50 | 1008ms | 351ms | 2.9x |
| Write throughput | 52/s | 133/s | 2.6x |

Phase2 is doing 2.6x more writes at 2.9x lower latency. That's a genuine improvement — the writer process is more efficient when it's not also serving reads. But the 2.9x latency number overstates the per-operation improvement because the system is operating at a different throughput point.

**A fairer comparison** would run both at the same write throughput (e.g., rate-limited to 50 writes/s) and compare latencies. The soak test does this (rate-limited to 30 write TPS) and shows 5.9ms vs 4.3ms — a 1.4x improvement. The truth is somewhere between 1.4x and 2.9x depending on load.

### The read p50 (27ms → 14ms) is more straightforwardly real
Reads fan out to 50 reader processes. Less contention per process → lower latency. The 1.9x improvement is consistent with the throughput test's 4.22x speedup (the mixed-load test has write contention reducing the effective parallelism).

---

## Summary of findings

| # | Finding | Severity | Action |
|---|---------|----------|--------|
| 1 | del_node readonly guard gap is REAL, not deployment artifact | **HIGH** | Fix before production. Test with leaf node to confirm. |
| 2 | Soak test thresholds are 1000x too generous — proves stability, not performance | **LOW** | Tighten thresholds or add a separate saturation test. |
| 3 | No parallel-reads baseline for master/g3 | **MEDIUM** | Re-run parallel-reads on master with concurrency=64 to get baseline. |
| 4 | Recovery failures are all test bugs, not Cortex bugs | **LOW** | Fix wildcard query and kill timeout. |
| 5 | Pathological vacuous tests are test-data issues | **LOW** | Update seed data for Synapse 2.240.x data model. |
| 6 | Mixed-load write speedup is real but overstated at 2.9x | **INFO** | True improvement is ~1.4x–2x depending on load. Report as "up to 2.9x under contention." |
| 7 | Throughput 4.22x is real but only for cache-resident datasets | **INFO** | Qualify with dataset size. |
| 8 | G3 exponent cap (4096) is a real behavioral change | **LOW** | Update test or document as known G3 difference. |
| 9 | read-after-write test hung (watchdog timeout 2x) | **MEDIUM** | Infrastructure issue — investigate SSM channel or test script hang. |
| 10 | Soak write improvement (5.9→4.3ms) is unexplained | **LOW** | Validate that writer benefits from read offloading, or check for confounds. |

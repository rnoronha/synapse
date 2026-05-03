# Devil's Advocate Review: `scripts/test_soak.py`

**Reviewer:** Kiro (devil's advocate mode)
**Date:** 2026-05-03
**Branch:** phase2-multi-process

---

## 1. DOES IT TEST WHAT IT CLAIMS?

The docstring claims:

> Sustains concurrent read and write workloads at target TPS for hours,
> tracking per-operation latency and reporting p50/p99/p999 percentiles.

### Claim-to-code mapping

| Claim | Backed by code? | Verdict |
|---|---|---|
| Concurrent read and write workloads | `_read_loop` and `_write_loop` run as separate `asyncio.create_task` | **Partially.** Both loops share a single `prox` (Telepath proxy). This is cooperative concurrency on one connection, not true concurrent load. A real multi-client soak would use separate connections or multiple processes. |
| Target TPS | Sleep-based rate limiting: `interval = 1.0 / tps`, sleep for `interval - elapsed` | **Partially.** See §4 for why this undershoots. |
| Tracks per-operation latency | `time.monotonic()` around each `storm()` call, appended to `Stats` | **Yes.** |
| Reports p50/p99/p999 | `_percentile()` and `_build_final_report()` | **Yes.** |
| "Heavy soak test" | Default 3h, 100 read TPS + 30 write TPS | **Misleading.** Single async loop, single connection. This is a serial-with-sleeps test, not a heavy concurrent load generator. |

### Unsubstantiated claims

**[HIGH]** The script name and docstring say "heavy soak test" but there is **zero assertion of pass/fail criteria**. The script always exits 0 (via `return 0` in `main()`). It cannot detect:
- Latency regression (no threshold for p99)
- Error rate regression (no threshold for error percentage)
- Throughput degradation (no check that actual TPS met target)

The `deploy-and-test.sh` wrapper treats exit code 0 as PASS. This means **the soak test always passes**, regardless of results.

---

## 2. EDGE CASES MISSED

### **[CRITICAL] Error swallowing**
Both `_read_loop` and `_write_loop` catch bare `Exception` and silently count it as an error. There is no logging of *what* failed. If the Cortex returns `AuthDeny`, `SchemaViolation`, or the connection drops entirely, the test happily continues counting errors with no visibility into root cause. A 100% error rate still produces exit code 0.

### **[HIGH] Single connection, shared proxy**
Both loops share one `prox` object. If the Telepath connection drops or enters a bad state:
- Both loops fail simultaneously — this doesn't test independent client resilience
- Telepath proxy reconnection behavior (if any) is untested
- A real multi-process deployment would have many independent clients

### **[HIGH] IPv4 address collisions in `_make_write_query`**
```python
a, rem = divmod(idx, 256 * 256)
b, c = divmod(rem, 256)
return f'[inet:ipv4=10.{a % 256}.{b % 256}.{c % 256}]'
```
At `idx=256*256*256` (16.7M), addresses wrap around and collide. At 30 write TPS with 25% being IPv4 writes (~7.5/s), this wraps in ~26 days — unlikely for a 3h test, but the modular arithmetic is needlessly fragile. More importantly, **duplicate writes are silently treated as successful new writes** in the latency stats, inflating apparent write throughput.

### **[MEDIUM] FQDN collisions**
```python
ts = int(time.time())
return f'[inet:fqdn=soak-{ts}-{idx}.test.com]'
```
`ts` has 1-second granularity. If the test is restarted within the same second with the same `idx` sequence, it writes duplicates. The test doesn't distinguish "created new node" from "merged into existing node" — both count as successful writes.

### **[MEDIUM] Read queries assume pre-existing data**
`READ_QUERIES` includes `inet:fqdn#soak | limit 10` — querying for a `#soak` tag. Nothing in the test applies this tag. If the Cortex is empty or the tag doesn't exist, this query returns 0 results. The test doesn't check whether reads actually returned data, so **reads against an empty Cortex are indistinguishable from reads against a full one**.

### **[LOW] No unicode/special character coverage**
All generated data uses ASCII hostnames and hex strings. No test of unicode FQDNs, URLs with query parameters, or edge-case encodings.

---

## 3. FALSE PASS SCENARIOS

### **[CRITICAL] No pass/fail criteria — always returns 0**
`main()` unconditionally `return 0`. The deploy script checks this exit code to determine PASS/FAIL. A soak test where:
- 100% of operations error out
- p99 latency is 60 seconds
- Actual TPS is 1% of target
- The Cortex crashes mid-test

...all report PASS.

### **[HIGH] Latency recorded but never validated**
The test computes p50/p99/p999 and writes them to JSON, but never compares them against any baseline or threshold. A 10x latency regression is invisible unless a human reads the JSON output.

### **[HIGH] Error count recorded but never validated**
`read_errors` and `write_errors` are tracked but never checked. The test could accumulate thousands of errors and still pass.

### **[HIGH] Reads don't validate result content**
```python
async for _ in prox.storm(query):
    pass
```
Results are consumed and discarded. The test cannot detect:
- Wrong data returned (data corruption)
- Fewer results than expected (lost writes)
- Stale reads from a lagging reader process

This is especially concerning for a multi-process Cortex where read replicas could serve stale data.

### **[MEDIUM] Connection failure masks as graceful completion**
If `s_telepath.openurl()` fails or the proxy disconnects, the `async with` block exits, `_shutdown` may or may not be set, and the test proceeds to report whatever partial data it collected — still exit 0.

### **[MEDIUM] Reporter interval hides bursty failures**
The reporter snapshots every 60 seconds and resets. If there's a 5-second burst of 100% errors followed by 55 seconds of success, the 60-second window dilutes the error signal. No per-second or sliding-window error rate tracking exists.

---

## 4. METHODOLOGY

### **[HIGH] Rate limiting undershoots target TPS**
```python
interval = 1.0 / tps
# ...
sleep = interval - elapsed
if sleep > 0:
    await asyncio.sleep(sleep)
```
This is a single-worker rate limiter. If a single operation takes longer than `interval` (10ms for 100 TPS), the loop falls behind and **cannot catch up**. There's no burst/backfill mechanism. Actual TPS will always be ≤ target, potentially much less under load. The test doesn't report actual achieved TPS vs. target.

### **[HIGH] No warmup period**
The first operations hit a cold Cortex (empty caches, no LMDB pages in memory). These cold-start latencies are mixed into the overall stats, skewing p50 upward and making p99/p999 unreliable for steady-state characterization.

### **[MEDIUM] `_percentile` is a custom implementation**
Python's `statistics.quantiles()` (3.8+) or `numpy.percentile` are battle-tested. The custom `_percentile` uses linear interpolation which is fine, but it's untested code computing the primary output metric. A bug here silently corrupts all reported latencies.

### **[MEDIUM] Latency list grows unbounded**
`all_read_lats` and `all_write_lats` accumulate every latency sample for the entire run. At 100 read TPS for 3 hours = 1,080,000 floats (~8.2MB). Not a memory problem, but sorting 1M+ elements for percentile calculation at the end is O(n log n) when a streaming percentile (t-digest, HDR histogram) would be O(1) per sample.

### **[MEDIUM] `asyncio.Lock` contention**
Both loops and the reporter contend on `Stats._lock`. At 130 combined TPS this is negligible, but the lock acquisition itself adds unmeasured overhead to the latency recording path. The `time.monotonic()` call is *before* the lock, so lock wait time isn't included in latency — this is correct, but the lock could cause the rate limiter to undershoot further.

### **[LOW] `time.monotonic()` includes full async iteration**
```python
t0 = time.monotonic()
async for _ in prox.storm(query):
    pass
await stats.record_read(time.monotonic() - t0)
```
Latency includes time-to-first-result + full iteration + any async scheduling delays. This is arguably the right thing to measure for end-to-end latency, but it conflates query execution time with result streaming time. For `limit 10` queries this is minor; for larger result sets it would be misleading.

---

## 5. MISSING TESTS

### **[CRITICAL] No correctness validation during soak**
The entire point of a soak test is to find bugs that emerge over time: memory leaks, resource exhaustion, data corruption, stale reads. This test measures *latency* but validates *nothing*. It should:
- Periodically write a known value and read it back (read-after-write consistency)
- Check that node counts are monotonically increasing
- Verify that tagged nodes are actually tagged
- Detect duplicate or missing nodes

### **[HIGH] No resource monitoring**
A soak test should track system-level metrics over time:
- Memory usage (RSS) of the Cortex process(es) — detect leaks
- File descriptor count — detect handle leaks
- LMDB map size / disk usage growth rate
- CPU utilization — detect spin loops or GIL contention

### **[HIGH] No multi-connection testing**
For a multi-process Cortex, the soak test should:
- Open connections to both writer (27492) and reader (27493/27494) ports
- Verify that reads on reader ports return data written through the writer
- Measure replication lag between writer and readers

### **[HIGH] No degradation detection over time**
The test should compare latency percentiles across time windows to detect:
- Monotonic latency increase (resource leak)
- Periodic latency spikes (GC, compaction, checkpoint)
- Throughput decay

The 60-second snapshots are printed but not analyzed.

### **[MEDIUM] No connection resilience testing**
What happens when:
- A reader process crashes and restarts mid-soak?
- The network has a brief interruption?
- The Cortex hits a resource limit?

The test uses a single connection and has no reconnection logic.

### **[MEDIUM] No write verification**
Writes are fire-and-forget. The test doesn't verify that written nodes actually persist. A Cortex that accepts writes but silently drops them would pass this test.

### **[LOW] No tag/property write coverage**
All writes create bare nodes. No testing of:
- Property setting (`.set()`)
- Tag application (`+#tag`)
- Edge creation (`+(refs)>`)
- Node deletion

---

## Summary

| Severity | Count | Key theme |
|---|---|---|
| CRITICAL | 3 | No pass/fail criteria; no correctness validation; errors swallowed silently |
| HIGH | 8 | Single connection; no content validation; no resource monitoring; methodology gaps |
| MEDIUM | 7 | Collision risks; reporter dilution; custom percentile; missing write verification |
| LOW | 3 | Unicode coverage; token measurement; write diversity |

**Bottom line:** This script is a **latency measurement harness**, not a test. It cannot fail. It validates no correctness properties. It runs a single-connection serial workload against a system designed for multi-process concurrency. Calling it a "heavy soak test" is misleading — it's closer to a single-threaded benchmark logger.

The most urgent fix is adding pass/fail thresholds (error rate < X%, p99 < Y ms, actual TPS > Z% of target) so the script can actually return non-zero and trigger a FAIL in the deploy pipeline.

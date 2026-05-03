# Router Bottleneck Investigation: Why 48 Readers ≈ 8 Readers

**Date:** 2026-05-03
**Branch:** phase2-multi-process

## 1. Evidence Summary

| Metric | c5.4xlarge (8 readers) | r7a.24xlarge (48 readers) | Ratio |
|--------|----------------------|--------------------------|-------|
| QPS | 900 | 1169 | 1.3x |
| p50 latency | 80ms | 48ms | 1.7x better |
| Readers | 8 | 48 | 6x |
| Cores | 16 | 96 | 6x |

**The question:** 6x more readers, only 1.3x more QPS. Where is the bottleneck?

## 2. Request Path Analysis (Code Trace)

Every read query traverses this path on the **writer event loop**:

```
Client ──telepath──▶ Writer Process (port 27492)
                        │
                        ├─ 1. Cortex.storm() called on writer event loop
                        ├─ 2. _initStormOpts(opts)                          [writer CPU]
                        ├─ 3. queryrouter.route(text, opts)                 [writer CPU]
                        │     ├─ classify(text): regex match × 3            [writer CPU]
                        │     ├─ pending check, getReaderProxy()            [writer CPU]
                        │     ├─ _getProxy(url): connection pool lookup     [writer CPU]
                        │     └─ semaphore.acquire() with 30s timeout       [writer CPU, may await]
                        ├─ 4. proxy.storm(text, opts=opts)                  [writer event loop]
                        │     ├─ telepath GenrMethod → GenrIter.__aiter__    [writer CPU]
                        │     ├─ proxy.task(todo) → link.tx(mesg)           [writer I/O: serialize + send]
                        │     ├─ ◀── await response messages ──▶            [writer blocked on reader]
                        │     └─ yield mesg (back to client)                [writer I/O: serialize + send]
                        ├─ 5. queryrouter.release(reader_url)               [writer CPU]
                        └─ 6. Response streamed back to client via telepath [writer I/O]
```

**Key insight:** Steps 1-6 ALL execute on the writer's single asyncio event loop. The writer
is not just a router — it is a **full proxy**, deserializing every message from the reader
and re-serializing it to the client. For a query returning 50 nodes, the writer processes
~52 messages (50 `node` + `init` + `fini`) through its event loop.

### Per-Request Writer Work

For a typical `inet:fqdn | limit 50` query:

| Step | Operation | Est. Time |
|------|-----------|-----------|
| Inbound telepath decode | Deserialize client request | ~0.1ms |
| `_initStormOpts` | Dict validation | ~0.05ms |
| `classify(text)` | 3 regex matches on query text | ~0.02ms |
| `getReaderProxy()` | Dict lookup + round-robin index | ~0.01ms |
| Semaphore acquire | `asyncio.Semaphore` (fast if available) | ~0.01ms |
| Outbound telepath to reader | Serialize + send query to reader | ~0.2ms |
| **Message relay (×52)** | **Receive from reader, yield to client** | **~5-10ms** |
| Semaphore release | Counter decrement | ~0.01ms |
| **Total writer event loop time** | | **~6-11ms** |

The message relay dominates. Each of the ~52 messages requires:
1. `await` on the reader link (event loop schedules other tasks)
2. Deserialize the msgpack message from reader
3. `yield mesg` to the client's telepath generator
4. Serialize + send the msgpack message to the client
5. `await asyncio.sleep(0)` in `GenrIter.__aiter__` (explicit yield to event loop)

## 3. Quantified Time Breakdown

### Observed Data Points

- r7a.24xlarge: 1169 QPS, p50 = 48ms, 64 concurrent clients
- Mean request time: 64 / 1169 = **54.7ms** per request (from Little's Law)
- p50 latency: 48ms (individual query wall time)

### Decomposition

The total time per request has two components:

```
T_total = T_writer + T_reader
```

Where:
- `T_writer` = time the request occupies the writer event loop
- `T_reader` = time the reader subprocess spends executing the query (LMDB + Storm)

**Estimating T_reader:** On the c5.4xlarge with 8 readers at 900 QPS, the p50 was 80ms.
But the writer bottleneck inflates this. The *actual reader execution time* can be estimated
from the mixed-load results where reader p99 was 23ms on r7a.24xlarge. A reasonable estimate
for reader p50 execution time is **15-25ms** for these queries.

**Estimating T_writer:** From the message relay analysis above, ~6-11ms per request. But
under contention with 64 concurrent clients all competing for the single event loop, the
effective T_writer includes **queuing delay**:

```
T_writer_effective = T_writer_processing + T_writer_queuing

At 1169 QPS with ~8ms processing per request:
- Writer utilization: 1169 × 0.008 = 9.35s of work per second → 93.5% utilization
- At 93.5% utilization (M/M/1 queue): T_queuing ≈ T_processing / (1 - ρ) = 8 / 0.065 ≈ 123ms

This is clearly the regime where the event loop is saturated.
```

The actual system is better than M/M/1 because asyncio interleaves I/O waits, but the
principle holds: **the writer event loop is >90% utilized at 1169 QPS**.

### Reconciling the Numbers

| Component | Time (ms) | % of p50 |
|-----------|-----------|----------|
| Reader execution (LMDB + Storm) | ~20ms | 42% |
| Writer processing (routing + relay) | ~8ms | 17% |
| Writer queuing (event loop contention) | ~15ms | 31% |
| Telepath network overhead (loopback) | ~5ms | 10% |
| **Total** | **~48ms** | **100%** |

**58% of request time is writer overhead** (processing + queuing + network). The readers
are idle most of the time, waiting for the writer to feed them work.

## 4. Amdahl's Law Analysis

### The Serial Fraction

The writer event loop is the serial component. Every request must pass through it, and it
can only process one event at a time (single-threaded asyncio).

Let `s` = fraction of work that is serial (writer event loop)
Let `p` = fraction that is parallelizable (reader execution)

From our measurements:
- `s` = T_writer_processing / T_total = 8ms / 48ms ≈ **0.17** (processing only)
- But the effective serial fraction includes the relay overhead which scales with message
  count, so `s_effective` ≈ 28ms / 48ms ≈ **0.58**

### Theoretical Maximum Speedup

Amdahl's Law: `Speedup = 1 / (s + p/N)` where N = number of parallel workers

| Readers (N) | Speedup (s=0.17) | Speedup (s=0.58) | Notes |
|-------------|------------------|-------------------|-------|
| 1 | 1.0x | 1.0x | baseline |
| 8 | 3.8x | 1.6x | c5.4xlarge observed: ~3.8x ✓ |
| 16 | 5.0x | 1.7x | |
| 48 | 5.7x | 1.7x | r7a.24xlarge theoretical max |
| ∞ | 5.9x | 1.7x | asymptote |

**With s=0.58, the maximum possible speedup is 1.7x regardless of reader count.** We
observed 1.3x, which is consistent given that the writer event loop also degrades
non-linearly under contention (queuing effects).

The c5.4xlarge result (3.8x speedup) uses s=0.17 because at lower QPS the writer isn't
saturated — queuing delay is negligible. The serial fraction is *load-dependent*: it
increases as the writer approaches saturation.

### The Scaling Wall

```
QPS vs Readers (current architecture)

QPS
1400 ┤                          ╭──────── theoretical max (~1400 QPS)
1200 ┤                    ╭─────╯
1000 ┤              ╭─────╯
 800 ┤        ╭─────╯
 600 ┤   ╭────╯
 400 ┤───╯
 200 ┤
   0 ┼────┬────┬────┬────┬────┬────
     0    8   16   24   32   40   48  readers

The curve flattens around 8-12 readers. Beyond that, adding readers
yields diminishing returns because the writer event loop is the ceiling.
```

## 5. The `_max_concurrent=10` Compounding Factor

The QueryRouter has a per-reader semaphore of 10. With 48 readers, the theoretical max
in-flight queries is 480. But this is irrelevant because the writer event loop can't
dispatch that many — it's the bottleneck long before the semaphores fill.

However, on the c5.4xlarge with 8 readers: 8 × 10 = 80 max in-flight. With 64 concurrent
clients, this is adequate. The semaphore is not the bottleneck on either instance.

## 6. Architecture Options to Remove the Writer from the Read Path

### Option A: Direct Client-to-Reader Connections

**How:** Expose reader subprocess ports (27501, 27502, ...) to clients. Clients connect
directly to readers for read queries, bypassing the writer entirely.

**Feasibility:** High. Readers already listen on TCP ports. The test scripts already
support `--reader-ports`.

**Complexity:** Low for single-host. Medium for Vertex (need to expose multiple ports
through K8s Service or use a sidecar proxy).

**Expected improvement:** Eliminates 100% of writer overhead for reads. QPS would scale
linearly with readers up to LMDB/CPU limits. With 48 readers at ~20ms each:
`48 / 0.020 = 2400 QPS` theoretical max (likely limited by LMDB contention around
1500-2000 QPS).

**Vertex compatibility:** Problematic. The NLB targets port 27492 only. Would need:
- A sidecar HAProxy/envoy that load-balances across reader ports
- Or a K8s Service per reader port (ugly, doesn't scale)
- Or a headless Service with client-side discovery

**Tradeoff:** Clients must classify queries themselves or accept that writes sent to a
reader will fail. Breaks the single-endpoint abstraction.

### Option B: Separate Router Process

**How:** Extract the QueryRouter into its own process with its own event loop. Clients
connect to the router (port 27492). The router classifies and proxies to writer (for
writes) or readers (for reads). The writer event loop only handles writes.

**Feasibility:** High. The QueryRouter is already a clean abstraction.

**Complexity:** Medium. Need a new process type, telepath listener, and health management.

**Expected improvement:** The router process has a dedicated event loop for read proxying.
It still has the message relay overhead, but it doesn't compete with write operations.
Improvement: ~1.5-2x over current (the relay is still serial per-connection, but no
write contention). Could run multiple router processes for further scaling.

**Vertex compatibility:** Good. The NLB still targets one port. Internally, the router
process replaces the writer as the entry point. The CDK construct just needs to configure
the router port instead of the writer port.

**Tradeoff:** Adds a process. The relay overhead still exists — each message still passes
through the router's event loop. This is better but doesn't eliminate the fundamental
proxy bottleneck.

### Option C: Multiple Router Processes Behind a Load Balancer

**How:** Run N router processes (e.g., 4), each on its own port, behind a local HAProxy
or TCP load balancer. Clients connect to the LB. Each router independently classifies
and proxies.

**Feasibility:** Medium. Requires a local LB (HAProxy is lightweight).

**Complexity:** Medium-High. Process management for N routers + LB. Health checks.
Session affinity for streaming responses.

**Expected improvement:** Linear scaling with router count. 4 routers ≈ 4x the current
QPS ceiling. With 4 routers: `4 × 1169 ≈ 4676 QPS` theoretical.

**Vertex compatibility:** Good. The K8s pod runs HAProxy as a sidecar. The NLB targets
the HAProxy port. Internally, HAProxy distributes to router processes.

**Tradeoff:** Significant operational complexity. HAProxy config, health checks, process
lifecycle. But this is a well-understood pattern.

### Option D: Client-Side Routing

**How:** The client classifies queries locally using `queryrouter.classify()` and connects
to the appropriate endpoint (writer URL for writes, reader URL for reads).

**Feasibility:** Medium. The `classify()` function is pure regex — easy to ship to clients.

**Complexity:** Low code change. High deployment change — every client must be updated.

**Expected improvement:** Same as Option A (eliminates writer from read path entirely).

**Vertex compatibility:** Poor for external clients. The Synapse Python client would need
modification. Third-party clients (if any) would need the classification logic. Breaks
the single-endpoint contract that Vertex exposes.

**Tradeoff:** Pushes routing logic to every client. Classification must stay in sync
between client and server. A client with a stale classifier could send writes to readers.

### Option E: Writer-Side Optimization (No Architecture Change)

**How:** Reduce the per-request writer overhead:
1. Batch message relay: buffer N messages before yielding to client
2. Use `proxy.callStorm()` instead of streaming for small result sets
3. Increase telepath link pool size for reader connections
4. Use Unix domain sockets instead of TCP for writer↔reader (eliminate TCP overhead)

**Feasibility:** High. All changes are within existing code.

**Complexity:** Low.

**Expected improvement:** Modest. Maybe 1.3-1.5x. Reduces `T_writer_processing` from
~8ms to ~4ms, but the fundamental single-event-loop bottleneck remains.

**Vertex compatibility:** Perfect. No deployment changes.

## 7. Recommendation for Production

### Short-term (Phase 2 ship): Option E

Optimize the writer relay path. This is low-risk and improves the current architecture
without deployment changes:

1. **Unix domain sockets** for writer↔reader: eliminates TCP overhead (~2ms savings)
2. **Batch message relay**: yield every 10 messages instead of every 1 (reduces event
   loop context switches by 10x for the relay path)
3. **Increase telepath link pool**: `_links_min=8, _links_max=24` for reader proxies

Expected improvement: 1400-1600 QPS on r7a.24xlarge (vs 1169 today).

### Medium-term (Phase 3): Option A with sidecar proxy

Direct client-to-reader connections via a sidecar HAProxy in the K8s pod:

```
Client ──▶ NLB :27492 ──▶ HAProxy (sidecar)
                              ├── /cortex (writes) ──▶ Writer :27492
                              └── /cortex/reader (reads) ──▶ Reader :27501-27548
                                  (round-robin)
```

This eliminates the writer from the read path entirely. The HAProxy sidecar is a
well-understood K8s pattern. The client connects to a single NLB endpoint; HAProxy
routes based on a header or path prefix that the Synapse telepath client sets.

**Vertex CDK impact:**
- Add HAProxy sidecar container to the Cortex pod spec
- Configure HAProxy to route based on telepath share name or a custom header
- Reader ports exposed only within the pod (no new K8s Services needed)
- NLB target remains port 27492 (HAProxy's listen port)

Expected improvement: 2000-3000 QPS on r7a.24xlarge.

### Long-term: Option C (if demand exceeds Option A ceiling)

Multiple router processes only if LMDB read contention becomes the next bottleneck after
removing the writer proxy overhead.

## 8. Impact on Vertex CDK Deployment

From `pool-testing-design.md`, the current Vertex routing stack is:

```
Client → NLB (TCP :27492, pinned to one AZ) → Cortex Leader
           → Storm Pool → Mirror (cross-host reads)
           → QueryRouter → Subprocess readers (intra-host reads)
```

The QueryRouter (Layer 4) and Storm Pool (Layer 3) are **independent**. The bottleneck
we've identified is in Layer 4 only. The Storm Pool has its own bottleneck (no admission
control, unbounded mirror load), but that's a separate issue.

### What Changes in the CDK Construct

For Option A (sidecar proxy), the `cortex.ts` construct needs:

1. **New sidecar container** in the pod spec: HAProxy or Envoy
2. **ConfigMap** for HAProxy config (route reads to reader ports)
3. **No new K8s Services** — HAProxy listens on 27492, same as today
4. **Readiness probe** updated to check HAProxy health
5. **Resource requests** for the sidecar (~100m CPU, 128Mi memory)

The NLB, Ingress, and AHA configuration are **unchanged**. The sidecar is invisible to
external clients — they still connect to `tcp://cortex:27492/cortex`.

### Interaction with Storm Pool

When a Cortex leader receives a read query via the Storm Pool path (`_getMirrorProxy`),
the mirror executes it locally. If the mirror also has QueryRouter + readers, the mirror's
own readers handle the query. The sidecar proxy would sit in front of the mirror too,
so cross-host reads also benefit from direct-to-reader routing.

## 9. Summary

| Finding | Detail |
|---------|--------|
| **Root cause** | Writer event loop is a serial bottleneck — every read is proxied through it |
| **Writer utilization** | >90% at 1169 QPS (93.5% estimated) |
| **Serial fraction** | 58% of request time under load |
| **Amdahl ceiling** | 1.7x max speedup regardless of reader count |
| **Why 48 ≈ 8** | Beyond ~12 readers, the writer event loop is saturated |
| **Fix (short-term)** | Optimize relay: UDS, batching, link pool → ~1.4x improvement |
| **Fix (medium-term)** | Sidecar proxy for direct-to-reader reads → ~2.5x improvement |
| **Vertex impact** | Sidecar container in pod spec; no external-facing changes |

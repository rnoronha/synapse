# Thin Router Layer: Research Findings

**Date:** 2026-05-04
**Branch:** phase2-multi-process
**Prerequisite:** [router-bottleneck-investigation.md](router-bottleneck-investigation.md)

---

## Q1: Separate Process vs Thread for the Router?

### What the router actually does

In the current architecture, the writer event loop performs all of these steps for every read query (see bottleneck investigation §2):

1. Accept telepath connection
2. Deserialize client request (msgpack)
3. `_initStormOpts(opts)` — dict validation
4. `queryrouter.classify(text)` — 3 regex matches
5. `queryrouter.route()` — round-robin proxy lookup + semaphore acquire
6. `proxy.storm(text, opts)` — forward to reader via telepath
7. **Message relay ×N** — deserialize from reader, re-serialize to client (dominates: ~5–10ms for 50 nodes)
8. Release semaphore

In the Phase 2 fork architecture (worker.py), workers already accept connections directly via EPOLLEXCLUSIVE and serve reads locally. The "router" question is about what sits in front when we want a single entry point that dispatches to the right process.

A thin router's job reduces to:
- Accept TCP connection
- Read the telepath handshake (share name lookup)
- For the session lifetime, proxy messages between client and target process

Classification is **not needed** at the router level. The fork workers already handle classification internally: they attempt `readonly=True` execution and fall back to writer forwarding on `IsReadOnly`. The regex classifier is a fast-path optimization inside the worker, not a routing decision.

### Thread vs process analysis

| Factor | Router Thread (in writer) | Router Process (separate) |
|--------|--------------------------|--------------------------|
| GIL contention | Shares GIL with writer. Message relay is CPU-bound (msgpack ser/deser). Under load, relay work starves write commits. This is the **current bottleneck**. | Own GIL. Writer event loop only handles writes. |
| IPC overhead | None — shared memory | Needs IPC to hand off connections. But if using fd passing (Q2), IPC is one-shot per connection, not per message. |
| Failure isolation | Router crash kills writer | Router crash leaves writer healthy; arbiter respawns |
| Complexity | Low — just a thread pool | Medium — new process type, lifecycle management |
| CPU cost of routing | Negligible. The routing decision (classify + round-robin) is ~0.03ms. The cost is in the **message relay**, not the routing. | Same relay cost, but on a dedicated core |

### Recommendation: Separate process

A router thread doesn't solve the problem. The bottleneck investigation showed the writer event loop is >90% utilized at 1169 QPS, and 58% of request time is writer overhead. A thread shares the GIL with the writer, so relay work still competes with write commits.

A separate router process gets its own event loop and GIL. The writer event loop becomes write-only. This is the minimum viable change to break the Amdahl ceiling.

However, the **best** option is to eliminate the relay entirely — see Q2.

---

## Q2: FD Passing (sendmsg/SCM_RIGHTS) vs Stream Proxy?

### Option A: Stream proxy (current approach, relocated)

The router accepts the client connection, reads from client, writes to target, reads response, writes back. This is what `cortex.storm()` does today through the QueryRouter — it's a full message-level proxy.

**Cost:** Every message crosses the router's event loop. For a 50-node response, that's ~52 msgpack deserialize/serialize cycles through the router. The bottleneck investigation measured this at ~5–10ms per query. Moving it to a separate process helps (no GIL contention with writer), but the relay overhead still limits per-router throughput to ~1200 QPS.

**Advantage:** Simple. No changes to telepath. Works today.

### Option B: FD passing (sendmsg/SCM_RIGHTS)

The router accepts the TCP connection, decides the target (worker or writer), and passes the raw file descriptor to the target process via a Unix domain socket using `sendmsg` with `SCM_RIGHTS`. The target process then owns the connection for its full lifetime.

**Cost:** One IPC message per connection (not per query message). Zero ongoing relay overhead.

**Feasibility check:**
- Python's `socket.sendmsg()` / `socket.recvmsg()` support `SCM_RIGHTS` natively since Python 3.3.
- Telepath does **not** support mid-stream fd handoff. The telepath handshake (share name negotiation, auth) happens on the link. You can't start a handshake on one process's link and continue on another's.
- **But:** The router can pass the fd **before** the telepath handshake. The target process receives the raw TCP socket and runs the full telepath protocol from scratch. The client sees a normal telepath connection.

**The critical constraint:** The router must decide the target *before* the telepath handshake completes, because after handshake the link has state (auth context, share reference) that can't be transferred. This means the router needs a pre-telepath signal to classify the connection.

**Options for pre-handshake routing:**
1. **Port-based:** Writer listens on :27492, workers on :27501+. Router accepts on :27492 and passes fd to a worker (reads) or writer (writes). But this requires the client to know which port to use — defeats the single-endpoint purpose.
2. **All-to-workers by default:** Router passes every new connection fd to a worker. Workers serve reads locally and forward writes to the writer via UDS (current behavior). The router is just a load balancer.
3. **Peek-based:** Router reads the first telepath message (which contains the share name), uses it to route, then passes the fd + buffered bytes. Complex and fragile.

### Option C: Hybrid — fd passing to workers, workers forward writes

This is option B.2 above and it's the natural fit for the fork architecture:

```
Client ──TCP──▶ Router Process
                  │ accept()
                  │ sendmsg(fd) via SCM_RIGHTS
                  ▼
              Worker Process (round-robin selection)
                  │ recvmsg(fd) → raw TCP socket
                  │ run telepath handshake
                  │ classify query
                  ├─ read → execute locally (LMDB readonly)
                  └─ write → forward to writer via UDS (existing path)
```

**Advantages:**
- Zero relay overhead for reads (the dominant case)
- Write forwarding already works (worker.py `_forward_storm`)
- Router is trivially simple: accept + round-robin + sendmsg
- Router process is stateless — can run multiple for HA
- No telepath changes needed

**Disadvantages:**
- Requires `socket.sendmsg`/`recvmsg` with `SCM_RIGHTS` — Linux-only (fine for production, all targets are Linux)
- The router can't do per-query routing (it routes per-connection). A client that sends a mix of reads and writes on one connection will have all queries handled by one worker, with writes forwarded to the writer. This is the same as the current fork worker behavior.
- Need to implement fd receiving in the worker's accept loop

### Recommendation: FD passing to workers (Option C)

This eliminates the relay bottleneck entirely for reads. The router becomes a thin fd dispatcher — accept, round-robin, sendmsg. Its CPU cost is negligible (~0.1ms per connection), so a single router process can handle thousands of connections/second.

The implementation requires:
1. A new `RouterProcess` that listens on the public port, accepts connections, and passes fds to workers via per-worker UDS control channels.
2. Workers add a second accept path: in addition to EPOLLEXCLUSIVE on the shared listening socket, they also `recvmsg` on their control UDS to receive passed fds.
3. The shared listening socket (EPOLLEXCLUSIVE) can be kept as a fallback or removed entirely.

Estimated complexity: ~200 lines of new code (router process + worker fd receive).

---

## Q3: Interaction with Storm Pool

### What the Storm Pool does (cortex.py analysis)

The Storm Pool is a **cross-host** read offloading mechanism:

1. **Configuration:** `cortex.setStormPool(url, opts)` stores a telepath URL (typically an AHA pool URL pointing to mirror Cortex instances) and options (connection timeout, sync timeout).

2. **Proxy acquisition:** `_getMirrorProxy(opts)` iterates the pool, skipping self, checking each mirror's nexus offset is within `MAX_NEXUS_DELTA` of the leader. Returns the first sufficiently-in-sync mirror proxy.

3. **Routing decision:** In `cortex.storm()`, the routing priority is:
   ```
   1. QueryRouter (local fork workers) — if configured
   2. StormPool (remote mirrors) — if configured and opts.mirror != False
   3. Local execution — fallback
   ```

4. **Data consistency:** The pool passes `nexsoffs` (current nexus offset) to the mirror, which waits until it has replicated up to that offset before executing the query. This provides read-after-write consistency at the cost of latency (the `timeout:sync` option).

5. **Scope:** The Storm Pool handles `storm()`, `callStorm()`, `count()`, and `exportStorm()`. Each method independently checks for pool availability and falls back to local execution on timeout.

### Can the router replace the Storm Pool's routing logic?

**No.** They operate at different layers:

| | Thin Router | Storm Pool |
|---|---|---|
| Scope | Connection-level dispatch | Per-query dispatch |
| Target | Local fork workers (same host, shared LMDB) | Remote mirrors (different hosts, nexus replication) |
| Consistency | Immediate (shared LMDB, MVCC) | Eventual (nexus offset sync) |
| Protocol | FD passing (no telepath awareness) | Full telepath proxy with nexus offset negotiation |
| Failure mode | Worker crash → respawn | Mirror lag → skip, try next |

The Storm Pool requires per-query nexus offset checking, which is fundamentally incompatible with connection-level fd passing. The router can't know the nexus offset at connection time.

### Should the router delegate to the Storm Pool?

**No delegation needed.** They're complementary and already compose correctly in `cortex.storm()`:

```python
# cortex.py storm() — current priority chain:
if self.queryrouter is not None:     # Layer 1: local fork workers
    ...route to worker...
if self.stormpool is not None:       # Layer 2: remote mirrors
    ...route to mirror...
# Layer 3: local execution (fallback)
```

With the thin router + fd passing design, the QueryRouter in cortex.py is **eliminated** — workers accept connections directly. The Storm Pool continues to operate inside each worker's `cell.storm()` method. If a worker receives a read query and the Storm Pool is configured, the worker can still offload to a remote mirror. In practice this is unlikely (the worker can serve reads locally from LMDB), but the code path remains available.

### Recommendation: Keep Storm Pool independent

The thin router handles local dispatch (connection → worker). The Storm Pool handles cross-host dispatch (query → mirror). They don't overlap and shouldn't be merged. The only change needed is ensuring the Storm Pool code path still works inside fork workers (it should — workers inherit the cell with stormpool configuration).

---

## Q4: Can the Router Unify Local + Remote Routing?

### The two routing domains

**Local routing (thin router):**
- Targets: fork workers on the same host
- Mechanism: fd passing over UDS
- Granularity: per-connection
- Consistency: immediate (shared LMDB)
- Latency: ~0.1ms (sendmsg)

**Remote routing (Storm Pool):**
- Targets: mirror Cortex instances on different hosts
- Mechanism: telepath proxy with nexus offset sync
- Granularity: per-query
- Consistency: eventual (bounded by nexus delta)
- Latency: ~5–50ms (network + sync wait)

### Is there a clean abstraction?

**No.** The two domains differ on every axis: granularity, consistency model, failure semantics, and protocol. A unified abstraction would be a leaky one — it would need to expose "is this local or remote?" at every decision point, defeating the purpose of unification.

The closest thing to a unifying concept is a **routing table**:

```
Route { target_type: local|remote, target: worker_id|mirror_url, ... }
```

But this adds indirection without value. The local router is a syscall (`sendmsg`). The remote router is a complex protocol with offset negotiation, timeout handling, and fallback logic. Forcing them through a common interface means the interface is either too simple (loses remote semantics) or too complex (local routing carries unused baggage).

### Recommendation: Keep them separate

Local routing (thin router process) and remote routing (Storm Pool inside the cell) should remain independent mechanisms. They compose naturally: the router dispatches connections to workers, and workers can independently use the Storm Pool for cross-host offloading.

If a future need arises for a "global query planner" that considers both local workers and remote mirrors for each query, that would be a Layer 3 concern (inside the cell's storm method), not a Layer 1 concern (connection dispatch). The thin router should remain connection-level only.

---

## Q5: Write Throughput Ceiling

### Observed data

From the matrix results and bottleneck investigation:

| Metric | master (single-process) | phase2 (fork workers) | Source |
|--------|------------------------|----------------------|--------|
| Write throughput (mixed load, 64 concurrent) | 52 ops/s | 133 ops/s | matrix-results-review.md |
| Write p50 (mixed load) | 1008ms | 351ms | matrix-results-review.md |
| Write p50 (soak, rate-limited 30 TPS) | 5.9ms | 4.3ms | matrix-results-review.md |

The 2.6x throughput improvement (52 → 133 ops/s) comes from removing read contention from the writer event loop. The soak test at 30 TPS shows the per-operation improvement is ~1.4x when the system isn't saturated.

### What limits write throughput?

Three potential ceilings, in order of tightness:

**1. LMDB commit period (current ceiling):**
The Slab sync loop commits every `COMMIT_PERIOD = 0.2s` (200ms), or when `max_xactops_len = 10000` operations accumulate. With `writemap=True` and `map_async=True`, commits are asynchronous — the OS flushes dirty pages in the background. But the write transaction is single-threaded: only one `lenv.begin(write=True)` can be active at a time.

Theoretical max: At 200ms commit intervals, if each commit batches N writes, throughput = N / 0.2. For small writes (single node creation), N can be very large within 200ms. The LMDB write lock is held for the duration of the transaction, but with writemap the actual I/O is deferred. **LMDB itself is not the bottleneck at current throughput levels.**

**2. Event loop overhead (current ceiling at scale):**
Each write operation goes through: telepath deserialize → nexus push → LMDB put → telepath serialize response. The nexus push is the expensive part — it involves serialization, logging, and potentially replication. At 133 ops/s with the writer handling only writes, the event loop is spending ~7.5ms per write operation. This is consistent with the soak test's 4.3ms p50 (the difference is queuing delay at higher concurrency).

**3. Nexus replication (ceiling for HA deployments):**
In HA mode, every write is replicated to followers via the nexus log. This adds network latency per write. Not relevant for single-node deployments.

### Theoretical max with dedicated writer

With reads fully offloaded (no read contention on writer event loop):

- **Per-write cost:** ~4ms (from soak test at low contention)
- **Single event loop max:** 1000ms / 4ms = **~250 ops/s** per write type
- **With batching optimization:** If multiple writes are coalesced into fewer nexus pushes, throughput could reach **500–1000 ops/s**. This requires changes to the nexus layer.
- **LMDB hard ceiling:** With writemap, LMDB can sustain **10,000+ small puts/s** within a single transaction. The ceiling is the event loop and nexus, not LMDB.

### Is it event-loop-bound or LMDB-bound?

**Event-loop-bound.** The evidence:

1. The soak test shows 4.3ms per write at 30 TPS — well below LMDB's capability. LMDB can do a put + commit in <1ms for small values.
2. The mixed-load test shows 133 ops/s at 351ms p50 — the high latency is queuing delay from 64 concurrent clients competing for the single event loop.
3. The LMDB commit period (200ms) means writes are batched. At 133 ops/s, each commit batch contains ~26 writes. LMDB handles this trivially.

The bottleneck chain is: **asyncio event loop scheduling → nexus serialization → telepath message handling → LMDB (not reached)**.

### What did mixed-load show for write latency when reads were offloaded?

The phase2 mixed-load test (64 concurrent, 10% writes, 50 fork workers):
- Write p50: **351ms** (down from 1008ms on master)
- Write throughput: **133 ops/s** (up from 52 ops/s)

The improvement is real but the writer event loop is still doing relay work for reads that come through the QueryRouter proxy path. With the thin router + fd passing design (eliminating relay entirely), the writer would handle **only** forwarded writes from workers. Expected improvement: write p50 should drop to **~10–30ms** at the same concurrency, and throughput should reach **200–300 ops/s**.

### Recommendation

The write throughput ceiling with a dedicated writer (no read relay) is **~250 ops/s** per event loop, bounded by nexus serialization and telepath overhead, not LMDB. This is sufficient for the current workload profile (ACTI ingestion peaks at ~50 writes/s).

If write throughput becomes a bottleneck in the future, the path forward is nexus batching (coalesce multiple writes into a single nexus entry), not multiple writer processes (LMDB's single-writer lock prevents that).

---

## Summary of Recommendations

| Question | Recommendation | Rationale |
|----------|---------------|-----------|
| Q1: Process vs thread | **Separate process** | Thread shares GIL with writer, doesn't solve the bottleneck |
| Q2: FD passing vs proxy | **FD passing to workers** | Eliminates relay overhead entirely; ~200 LOC; Linux-only is acceptable |
| Q3: Storm Pool interaction | **Keep independent** | Different layers (connection vs query), different consistency models |
| Q4: Unified local+remote | **Keep separate** | No clean abstraction exists; they compose naturally without coupling |
| Q5: Write ceiling | **~250 ops/s, event-loop-bound** | LMDB is not the limit; nexus + telepath overhead is; sufficient for current workload |

### Proposed architecture

```
Client ──TCP :27492──▶ Router Process (thin, stateless)
                          │
                          │ sendmsg(fd, SCM_RIGHTS) — round-robin
                          ▼
                      Worker Process ×N
                          │ recvmsg(fd) → own the TCP socket
                          │ telepath handshake + auth
                          │ classify(query)
                          ├─ read → LMDB readonly (local, ~20ms)
                          └─ write → UDS forward to Writer (~4ms)
                                        │
                                    Writer Process
                                        │ nexus push + LMDB write
                                        │ (no read traffic)
```

This eliminates the writer from the read path entirely, breaking the Amdahl ceiling identified in the bottleneck investigation. Expected improvement: linear QPS scaling with worker count up to LMDB read contention (~2000–3000 QPS on r7a.24xlarge).

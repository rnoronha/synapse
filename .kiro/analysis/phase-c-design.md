# Phase C Design: Router Write-Classification and Direct Writer FD Routing

**Date:** 2026-05-04
**Branch:** phase2-multi-process
**Bead:** synapse-uxk
**Prerequisites:** [thin-router-design.md](thin-router-design.md), [thin-router-research.md](thin-router-research.md)

---

## 1. Problem Statement

Currently (Phase B), the router is a dumb fd dispatcher — it round-robins every connection to a worker. Workers then classify queries as read/write and forward writes to the writer via UDS. This works but has two costs:

1. **Workers carry write-forwarding code** (~80 lines: `_install_write_forwarding`, `_forward_storm`, `_forward_callStorm`, circuit breaker, writer proxy management). This violates GOAL.md criteria #4 and #8.
2. **Write latency includes an extra hop**: client → worker → writer (UDS forward) → worker → client. The worker is a pass-through for writes, adding ~2ms of serialization overhead per write.

Phase C moves write classification into the router and routes write-bearing connections directly to the writer process, making workers pure readers.

---

## 2. Telepath Protocol Peek: Extracting the Storm Query

### Wire format

Telepath uses msgpack-encoded tuples over a raw TCP stream (no length prefix — msgpack is self-delimiting). The first message from the client is always:

```python
('tele:syn', {'auth': ..., 'vers': (3, 0), 'name': '<share_name>'})
```

The `name` field identifies the shared object (e.g., `'cortex'`, `'cortex/view/default'`). This is **not** sufficient for read/write classification — the share name doesn't indicate query intent.

### The classification problem

The router needs the Storm query text to classify read vs write. But the query text arrives **after** the telepath handshake completes:

```
Client                    Router
  │                         │
  ├─ tele:syn ─────────────▶│  (1) share name, auth
  │◀──────── tele:syn ack ──┤  (2) session id, version
  │                         │
  ├─ task:init ────────────▶│  (3) method='storm', args=('query text', opts)
  │                         │      ← THIS has the query text
```

The `task:init` message contains:

```python
('task:init', {
    'task': <task_id>,
    'name': 'storm',
    'args': ('<storm_query_text>', <opts_dict>),
    'sess': <session_id>,
})
```

### Why per-connection classification is insufficient

**Telepath multiplexes queries on a single connection.** A client opens one connection and sends many `task:init` messages over its lifetime. Some may be reads, others writes. The router cannot classify the connection at handshake time because:

1. The `tele:syn` message has no query information
2. The first query might be a read, but the 10th might be a write
3. Client connection pools reuse connections across query types

This is the fundamental tension: fd passing is per-connection, but classification is per-query.

---

## 3. Classification Strategy: First-Query vs Every-Query

### Option A: First-query classification (peek first task:init)

The router completes the telepath handshake, waits for the first `task:init`, classifies it, then passes the fd to either a worker or the writer.

**Problems:**
- The router must implement the full telepath handshake (tele:syn, session management, auth). This makes it a thick router, not a thin one.
- After passing the fd, subsequent queries on the same connection bypass the router. A connection classified as "read" on its first query might later send writes.
- The router would need to buffer and forward the handshake state to the target process — telepath links carry auth context, session ids, and share references that can't be transferred.

**Verdict: Rejected.** Too complex, and doesn't solve the multiplexing problem.

### Option B: Every-query classification (router as protocol-aware proxy)

The router intercepts every `task:init`, classifies the query, and proxies the message to the right target.

**Problems:**
- This is a full stream proxy, not fd passing. It reintroduces the relay overhead that Phase B eliminated.
- The router must maintain per-connection state (session, auth context).
- This is essentially what the old QueryRouter in cortex.py did — the thing we're trying to remove.

**Verdict: Rejected.** Defeats the purpose of fd passing.

### Option C: Dedicated write port / share name (client-side routing)

Expose a separate telepath URL for writes (e.g., `tcp://host:27493/cortex` for writes, `:27492` for reads). Clients choose the right URL.

**Problems:**
- Requires client changes. Existing clients use a single URL.
- Breaks the single-endpoint constraint in GOAL.md.

**Verdict: Rejected.** Violates constraints.

### Option D: Worker-side classification with direct writer UDS (hybrid)

Keep the current architecture where all connections go to workers, but instead of workers proxying writes through telepath, workers pass the **raw fd** to the writer via SCM_RIGHTS when they detect a write query. The writer completes the query on the original client connection.

**Problems:**
- The worker has already completed the telepath handshake on this connection. The writer would need to take over a mid-session link — telepath doesn't support this.
- The worker's dmon owns the link state (session, auth). Transferring this to the writer is not possible without deep telepath changes.

**Verdict: Rejected.** Telepath link state is not transferable.

### Option E: Worker-side classification, lightweight write forwarding (recommended)

Workers remain the connection owners for all sessions. Classification stays in the worker. But instead of the current monkey-patch approach (`_install_write_forwarding` with `_patched_storm`/`_patched_callStorm`), writes are forwarded via a **minimal, purpose-built write channel** to the writer.

**Wait — this is what we already have.** The question is whether we can make workers "pure readers" (criteria #8) while keeping them as the connection endpoint.

The answer is: **criteria #4 and #8 as literally stated are incompatible with telepath's multiplexed connection model.** You cannot route write connections to the writer if reads and writes share the same connection. The router would need to be protocol-aware (Option B), which reintroduces the relay bottleneck.

---

## 4. Revised Design: What Phase C Actually Means

Given the telepath multiplexing constraint, Phase C should be reframed:

### Goal: Minimize worker write-forwarding complexity, not eliminate it

The current write forwarding in worker.py is ~80 lines of monkey-patching with circuit breakers, semaphores, and proxy management. This can be reduced to a thin, reliable forwarding layer without making workers "pure readers" in the absolute sense.

### Architecture

```
Client ──TCP──▶ Router Process
                  │ accept()
                  │ sendmsg(fd) — round-robin to workers (ALL connections)
                  ▼
              Worker Process
                  │ recvmsg(fd) → telepath handshake
                  │ serve reads locally (LMDB readonly)
                  │ on write: forward via writer UDS channel
                  │
                  │ NEW: simplified write channel
                  │   - No monkey-patching
                  │   - No circuit breaker (writer death = arbiter restarts)
                  │   - Direct msgpack RPC over UDS (not telepath)
                  │   - Single shared UDS connection per worker
                  ▼
              Writer Process
                  │ UDS listener for write RPCs
                  │ nexus push + LMDB write
                  │ return result over same UDS
```

### What changes from current Phase B

| Aspect | Phase B (current) | Phase C (proposed) |
|--------|-------------------|-------------------|
| Write detection | Monkey-patched `cell.storm()` with `readonly=True` attempt + `IsReadOnly` catch | Same — this is correct and minimal |
| Write forwarding | Telepath proxy to writer UDS (`openurl('unix:///...')`) | Direct msgpack RPC over pre-connected UDS socketpair |
| Connection management | Worker creates/manages telepath proxy, reconnects on failure | Socketpair created at fork time, always connected |
| Circuit breaker | 80-line CircuitBreaker class | Removed — if UDS write fails, the socketpair is dead (worker should exit, arbiter respawns) |
| Writer proxy | `_get_writer_proxy()` with lazy init, fini cleanup | No proxy — raw `sendmsg`/`recvmsg` on the socketpair |
| Semaphore | `asyncio.Semaphore(10)` for write concurrency | Not needed — UDS socketpair is naturally serialized |
| Code size | ~80 lines of forwarding code | ~25 lines |

### Writer UDS channel addition to router

The router doesn't need a writer channel. All connections go to workers. The writer UDS channel is between **workers and the writer**, not involving the router.

However, the arbiter creates a **per-worker write channel** (a second socketpair) at fork time:

```python
# In arbiter.fork_workers():
for i in range(num_workers):
    # Control channel (router → worker): for fd passing
    router_end, worker_ctrl = socket.socketpair(AF_UNIX, SOCK_STREAM)
    # Write channel (worker → writer): for write forwarding
    worker_write, writer_end = socket.socketpair(AF_UNIX, SOCK_STREAM)
    
    self._control_channels[i] = (router_end.fileno(), worker_ctrl.fileno())
    self._write_channels[i] = (worker_write.fileno(), writer_end.fileno())
```

The writer process inherits the writer-end fds and multiplexes them with `select.epoll`. Workers send write requests as msgpack-encoded tuples and receive results on the same socketpair.

### Write forwarding protocol (worker → writer)

```
Worker                              Writer
  │                                   │
  ├─ msgpack(('storm', text, opts)) ─▶│  request
  │◀── msgpack(('ok', [messages])) ───┤  success response
  │◀── msgpack(('err', excinfo)) ─────┤  error response
```

Each message is a single msgpack object (self-delimiting). The writer reads from all worker write channels via epoll and processes requests sequentially (writes are inherently serial due to LMDB's single-writer lock).

### Worker write-forwarding removal plan

**Remove from worker.py:**
1. `CircuitBreaker` class (~30 lines)
2. `_install_write_forwarding()` method (~40 lines)
3. `_forward_storm()` method (~15 lines)
4. `_forward_callStorm()` method (~15 lines)
5. `_get_writer_proxy()` method (~10 lines)
6. `_close_writer_proxy()` method (~5 lines)
7. `_write_sem` semaphore init
8. `_circuit` breaker init
9. `_writer_proxy` attribute

**Replace with:**
1. `_write_channel_fd` attribute (set at init from fork)
2. `_forward_write(text, opts)` — ~15 lines: msgpack encode request, send on socketpair, recv response, decode
3. Simplified `_install_write_forwarding()` — ~10 lines: same monkey-patch pattern but calls `_forward_write` instead of managing a telepath proxy

**Net change:** Remove ~115 lines, add ~25 lines. Workers still have write forwarding code, but it's a thin transport layer, not a complex proxy with circuit breakers and connection management.

---

## 5. Handling Mixed Read/Write Connections

As analyzed in §3, telepath multiplexes queries on a single connection. A single client connection may send:

```
task:init storm('inet:ipv4')           → read  → worker serves locally
task:init storm('[inet:ipv4=1.2.3.4]') → write → worker forwards to writer
task:init storm('inet:fqdn')           → read  → worker serves locally
```

**This is already handled correctly by the current Phase B architecture.** The worker's monkey-patched `cell.storm()` classifies each query independently:

1. **Fast-path:** Regex `classify(text)` returns `'write'` → forward immediately
2. **Slow-path:** Execute with `readonly=True` → if `IsReadOnly` exception → forward

No changes needed for mixed connections. The per-query classification in the worker is the correct level of abstraction.

---

## 6. Acceptance Criteria for Phase C

Given the analysis above, the original GOAL.md criteria #4 and #8 need revision:

| # | Original Criterion | Revised Criterion | Rationale |
|---|-------------------|-------------------|-----------|
| 4 | Write connections are passed to the writer process (not workers) | Write **queries** are forwarded to the writer via lightweight UDS RPC (not telepath proxy) | Telepath multiplexing prevents per-connection write routing |
| 8 | Workers have NO write forwarding code (pure readers) | Workers have **minimal** write forwarding (~25 lines, no circuit breaker, no proxy management) | Some forwarding code is unavoidable given the multiplexing constraint |

### Measurable acceptance criteria for Phase C

1. **Worker write-forwarding code reduced by >70%** (from ~115 lines to <35 lines)
2. **No telepath proxy in workers** — write forwarding uses raw UDS socketpair, not `openurl()`
3. **No CircuitBreaker class in workers** — dead socketpair = worker exits, arbiter respawns
4. **Write latency improvement** — p50 write latency <300ms at 64 concurrent (currently 351ms) due to eliminating telepath overhead on the forwarding path
5. **No throughput regression** — >1400 QPS parallel reads
6. **No soak regression** — <1% error rate under sustained load
7. **Write throughput ≥133 ops/s** (no regression from Phase B)
8. **test_v3_fork.py passes** with the new write channel

---

## 7. Implementation Plan

### Step 1: Add per-worker write socketpairs to arbiter

Modify `arbiter.fork_workers()` to create a second socketpair per worker for the write channel. Pass the worker-end fd to the worker and the writer-end fds to the writer process.

**Files:** `synapse/lib/arbiter.py` (~15 lines)

### Step 2: Add write channel listener to writer

The writer process (which runs the cell's event loop) adds an epoll-based listener for all worker write channel fds. On receiving a write request, it executes `cell.storm(text, opts)` or `cell.callStorm(text, opts)` and sends the result back.

**Files:** `synapse/lib/cell.py` or new `synapse/lib/writechannel.py` (~60 lines)

### Step 3: Replace worker write forwarding

Remove the telepath-proxy-based forwarding and replace with direct msgpack RPC over the write socketpair.

**Files:** `synapse/lib/worker.py` (net -90 lines)

### Step 4: Remove unused code

- Remove `CircuitBreaker` class from worker.py
- Remove `_get_writer_proxy`, `_close_writer_proxy`
- Remove writer proxy semaphore

**Files:** `synapse/lib/worker.py` (cleanup)

### Step 5: Update tests

Update `test_v3_fork.py` to verify write forwarding works with the new channel.

**Files:** `tests/test_v3_fork.py`

---

## 8. Open Questions

1. **Backpressure on write channel:** If the writer is slow, worker write requests queue in the UDS socketpair buffer. The kernel buffer is typically 212KB for `SOCK_STREAM` socketpairs. At ~200 bytes per write request, this holds ~1000 queued writes. If the buffer fills, `sendmsg` blocks (or returns `EAGAIN` if non-blocking). Should the worker use a timeout and return an error to the client? **Recommendation:** Use blocking sends with a 30s timeout (matching current `_write_sem` timeout).

2. **Concurrent writes from one worker:** The current design uses `asyncio.Semaphore(10)` to limit concurrent write forwards per worker. With a single socketpair, writes are naturally serialized. If we want concurrency, we need request IDs in the protocol so responses can be matched to requests out-of-order. **Recommendation:** Start with serial writes per worker. With N workers, the writer still processes N concurrent write streams. If this becomes a bottleneck, add request IDs later.

3. **Writer restart:** If the writer process dies and is respawned by the arbiter, the write socketpairs are dead. Workers detect this via `EPOLLHUP` or `BrokenPipeError` on send. The worker should exit and let the arbiter respawn it with fresh socketpairs. **Recommendation:** Worker exits on write channel death — same as current behavior when the writer UDS socket disappears.

---

## 9. Why Not Direct Writer FD Routing?

The GOAL.md criterion #4 ("Write connections are passed to the writer process") assumed the router could classify connections as read or write before passing the fd. The research (§2-3 above) shows this is not feasible because:

1. **Telepath multiplexes queries on one connection** — a connection is not inherently "read" or "write"
2. **The query text arrives after the handshake** — the router would need to be protocol-aware
3. **Protocol-aware routing reintroduces relay overhead** — defeating the purpose of fd passing

The correct architecture is: router dispatches connections to workers (dumb round-robin), workers classify per-query and forward writes via a lightweight channel. This achieves the performance goals while keeping the router thin.

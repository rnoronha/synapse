# Thin Router Process — Design Document

**Date:** 2026-05-04
**Branch:** phase2-multi-process
**Prerequisite:** [thin-router-research.md](thin-router-research.md)

---

## Architecture

```
                          ┌─────────────────────────────┐
                          │         Arbiter              │
                          │  (parent process, no I/O)    │
                          │  SIGCHLD → respawn children  │
                          └──────┬──────────────────┬────┘
                                 │ fork              │ fork
                    ┌────────────┘                   └──────────────┐
                    ▼                                               ▼
          ┌─────────────────┐                            ┌──────────────────┐
          │  Router Process  │                            │  Writer Process   │
          │  listen(:27492)  │                            │  nexus + LMDB rw  │
          │  accept() loop   │                            │  UDS: worker.sock │
          └───┬──────────┬───┘                            └──────────────────┘
              │          │                                         ▲
    sendmsg(fd)    sendmsg(fd)                                     │
    SCM_RIGHTS     SCM_RIGHTS                          UDS forward │
              │          │                              (writes)   │
     ┌────────┘          └────────┐                                │
     ▼                            ▼                                │
┌──────────┐              ┌──────────┐                             │
│ Worker 0 │              │ Worker N │─────────────────────────────┘
│ LMDB ro  │              │ LMDB ro  │
│ telepath │              │ telepath │
│ serve()  │              │ serve()  │
└──────────┘              └──────────┘
```

### Process roles

| Process | Responsibility | Listens on | Accepts connections? |
|---------|---------------|------------|---------------------|
| Arbiter | Fork children, SIGCHLD respawn | — | No |
| Router | Accept TCP, fd-pass to workers | :27492 (TCP) | Yes — sole acceptor |
| Worker ×N | Telepath handshake, serve reads, forward writes | UDS control channel (from router) | No — receives fds via recvmsg |
| Writer | Nexus push, LMDB writes | worker.sock (UDS) | No (existing UDS listener) |

### Key change from current architecture

Current: Workers call `accept()` on the shared TCP socket via EPOLLEXCLUSIVE.
New: Router is the sole `accept()` caller. Workers receive pre-accepted fds over a per-worker UDS control channel.

This eliminates the thundering-herd concern entirely and gives the router full control over load distribution.

---

## Lifecycle Sequence

```
Arbiter.startmain()
  │
  ├─ Phase 1: asyncio.run(cell._initForFork())
  │    Cell fully initialized, event loop destroyed
  │
  ├─ Phase 2: arbiter.fork_workers()
  │    ├─ _shutdown_forkpool()
  │    ├─ _stop_memlock_threads()
  │    ├─ _close_all_slabs()
  │    ├─ Create per-worker UDS socketpairs
  │    │    for i in range(N):
  │    │      parent_fd, child_fd = socket.socketpair(AF_UNIX, SOCK_STREAM)
  │    │      control_channels[i] = (parent_fd, child_fd)
  │    ├─ fork() × N workers
  │    │    child: close parent_fd, keep child_fd → worker_main(child_fd, ...)
  │    │    parent: close child_fd, keep parent_fd
  │    └─ fork() × 1 router
  │         child: router_main(listen_fd, {worker_id: parent_fd, ...})
  │         parent: close listen_fd (router owns it now)
  │
  └─ Phase 3: asyncio.run(_writer_serve())
       Writer re-opens LMDB rw, serves on worker.sock UDS
```

### Startup ordering

1. Workers fork first — they need time to re-open LMDB readonly.
2. Router forks after workers — it needs the worker UDS fds.
3. Writer enters its event loop last — it re-opens LMDB read-write.

The router does NOT accept connections until all workers signal readiness over their control channels (a single `b'READY'` message). This prevents fd-passing to a worker that hasn't finished LMDB re-open.

### Shutdown ordering

1. Arbiter sends SIGTERM to router first — stops new connections.
2. Arbiter sends SIGTERM to workers — they drain active sessions.
3. Arbiter sends SIGTERM to writer last — it drains forwarded writes.
4. Grace period (5s), then SIGKILL stragglers.

---

## FD Passing Protocol

### Control channel: per-worker UDS

Created via `socket.socketpair(AF_UNIX, SOCK_STREAM)` before fork. One end goes to the router, the other to the worker.

### Message format (router → worker)

```
┌──────────────────────────────────────────────────┐
│  sendmsg() ancillary data: SCM_RIGHTS [conn_fd]  │
│  payload: 1 byte = 0x01 (NEW_CONN)               │
└──────────────────────────────────────────────────┘
```

The protocol is deliberately minimal — one byte of data to satisfy sendmsg's requirement for a non-empty payload, plus the fd in ancillary data. No share name, no buffered bytes, no metadata. The worker does the full telepath handshake from scratch.

Why not pass the share name? The research (Q2, Option B.3) showed that peeking the telepath message and forwarding buffered bytes is fragile. The router doesn't need to understand telepath. It's a dumb fd dispatcher.

### Message format (worker → router)

```
┌──────────────────────────────┐
│  1 byte = 0x52 (READY)       │  — sent once at startup
└──────────────────────────────┘
```

Workers send a single READY byte after LMDB re-open completes. The router waits for all READY signals before entering its accept loop.

### Message format (router control — future)

Reserved for future use:
- `0x02` DRAIN — router tells worker to stop accepting new sessions
- `0x03` STATS — worker reports active session count

### Wire-level implementation

```python
# Router side — send fd to worker
import socket, array

def send_fd(uds_sock: socket.socket, conn_fd: int) -> None:
    fds = array.array('i', [conn_fd])
    uds_sock.sendmsg(
        [b'\x01'],
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)],
    )

# Worker side — receive fd
def recv_fd(uds_sock: socket.socket) -> int | None:
    msg, ancdata, flags, addr = uds_sock.recvmsg(1, socket.CMSG_SPACE(4))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds = array.array('i')
            fds.frombytes(cmsg_data[:4])
            return fds[0]
    return None
```

### Fd lifecycle

1. Router calls `accept()` → gets `conn_fd`
2. Router calls `send_fd(worker_uds, conn_fd)` → kernel duplicates fd into worker
3. Router calls `os.close(conn_fd)` — router's copy is done
4. Worker calls `recv_fd()` → gets its own fd number for the same socket
5. Worker wraps in `socket.socket(fileno=fd)` → full telepath handshake
6. Worker closes socket when session ends

The kernel refcounts the underlying socket object. The connection stays alive as long as at least one process holds an fd to it.

---

## Connection Routing Strategy

### Phase 1 (this design): Round-robin to workers

All connections go to workers. Workers handle reads locally and forward writes to the writer via the existing UDS path (`worker.sock`).

```python
class RouterProcess:
    def __init__(self, listen_fd, worker_uds_fds):
        self._listen_fd = listen_fd
        self._worker_fds = worker_uds_fds  # {worker_id: uds_socket}
        self._rr_index = 0

    def _next_worker(self) -> socket.socket:
        workers = list(self._worker_fds.values())
        idx = self._rr_index % len(workers)
        self._rr_index += 1
        return workers[idx]
```

This is the simplest correct strategy. The research showed classification is not needed at the router level — workers already handle the read/write split internally.

### Phase 2 (future): Weighted round-robin

Workers report active session counts via the control channel. Router biases toward less-loaded workers. Simple change to `_next_worker()`.

### Phase 3 (future): Direct writer routing

If the GOAL.md acceptance criterion "Write connections are passed to the writer process (not workers)" becomes a hard requirement, the router would need to peek the `tele:syn` message to extract the share name. This is more complex:

1. Router reads the first telepath frame (msgpack-encoded `('tele:syn', {...})`)
2. Extracts `name` field
3. If name matches a write-only share → send fd to writer
4. Otherwise → send fd to worker, but also send the buffered bytes

This is explicitly deferred. The current GOAL.md says "OR: all connections go to workers, workers forward writes via existing UDS (simpler, keeps current behavior)" — we take that path.

---

## Error Handling

### Router process dies

**Detection:** Arbiter receives SIGCHLD for the router pid.

**Impact:** No new connections accepted. Existing sessions (already handed off to workers) are unaffected — workers own those fds.

**Recovery:** Arbiter respawns the router. New router inherits the listening socket fd (dup'd before fork), re-creates UDS connections to workers, waits for READY, resumes accepting.

**Client experience:** New connections get `ECONNREFUSED` for the duration of the respawn (~100ms). Existing sessions are uninterrupted.

### Worker dies mid-session

**Detection:** Arbiter receives SIGCHLD for the worker pid. Router detects broken UDS control channel (EPOLLHUP on the worker's socketpair end).

**Impact:** All sessions on that worker are dropped (TCP RST). The router removes the dead worker from its round-robin pool.

**Recovery:** Arbiter respawns the worker. New worker sends READY on a new control channel. Router adds it back to the pool.

**Client experience:** Active sessions on the dead worker get connection reset. New connections are routed to surviving workers immediately.

### Worker not ready (slow LMDB re-open)

**Detection:** Router hasn't received READY from the worker.

**Handling:** Router excludes that worker from the round-robin pool. Once READY arrives, worker is added.

### sendmsg fails (EAGAIN / EMSGSIZE)

**Handling:** Router retries once. If still failing, closes the client connection (TCP RST) and logs a warning. This should be extremely rare — UDS sendmsg with a single fd is tiny.

### All workers dead

**Detection:** Router's round-robin pool is empty.

**Handling:** Router stops accepting (removes listen fd from epoll). Logs critical error. Resumes when any worker sends READY.

### Writer dies

**Impact on router:** None. Router doesn't talk to the writer.

**Impact on workers:** Write forwarding fails. Workers' circuit breaker opens. Read queries continue to work. This is the existing behavior, unchanged.

---

## Files to Create/Modify

### New: `synapse/lib/router.py`

```
RouterProcess
├── __init__(listen_fd, worker_uds_map, writer_uds_fd=None)
├── router_main()          — entry point after fork
├── _wait_for_workers()    — block until all READY received
├── _accept_loop()         — epoll on listen_fd, accept, round-robin sendmsg
├── _next_worker()         — round-robin selection (skip dead workers)
├── _send_fd(worker_uds, conn_fd) — sendmsg with SCM_RIGHTS
├── _on_worker_hup(worker_id)     — remove from pool on broken UDS
└── shutdown()             — drain and exit
```

~120 lines. Synchronous (no asyncio needed — the router does no I/O beyond accept + sendmsg). Uses `select.epoll` directly.

### Modified: `synapse/lib/arbiter.py`

Changes:
1. Create `socket.socketpair()` per worker before forking
2. Pass child-end fd to worker via `_worker_entry`
3. Fork router process after workers, passing parent-end fds
4. Track router pid alongside worker pids for SIGCHLD handling
5. Shutdown: signal router first, then workers, then writer

New fields on Arbiter:
```python
self._router_pid = None
self._control_channels = {}  # {worker_id: (parent_fd, child_fd)}
```

~40 lines of changes.

### Modified: `synapse/lib/worker.py`

Changes:
1. `worker_main()` accepts a `control_fd` parameter (the UDS socketpair child end)
2. Replace EPOLLEXCLUSIVE accept loop with `recvmsg` loop on `control_fd`
3. On `recv_fd()`, wrap in socket, create asyncio streams, hand to dmon
4. Send `READY` byte after LMDB re-open completes
5. Remove `listen_fd` parameter — workers no longer need the listening socket

The `ReadOnlyWorker.serve()` method changes from:
```python
# OLD: epoll on listen_fd, accept()
epoll.register(self._listen_fd, EPOLLIN | EPOLLEXCLUSIVE)
events = epoll.poll(1.0)
conn, addr = listen_sock.accept()
```

To:
```python
# NEW: recvmsg on control_fd
fd = recv_fd(self._control_sock)
conn = socket.socket(fileno=fd)
```

~30 lines of changes (net reduction — EPOLLEXCLUSIVE setup code is removed).

### Modified: `synapse/lib/cell.py`

Changes to `startmain()`:
1. After `arbiter.fork_workers()`, fork the router process
2. Pass the listen_fd to the router instead of keeping it for the writer
3. Writer no longer calls `_restoreDmonListener(listen_fd)` for the TCP socket — it only listens on UDS

~15 lines of changes.

### Modified: `synapse/cortex.py`

Changes to `_initForkMode()`:
1. Add `router: True` to `_forkinfo` dict (signals that router process should be spawned)

~2 lines.

---

## Migration Path

### Phase A: Router coexists with EPOLLEXCLUSIVE (safe rollback)

1. Add `router.py` with the RouterProcess
2. Workers keep their EPOLLEXCLUSIVE accept loop as fallback
3. Router is spawned but workers also accept directly
4. Config flag: `multi:process:router: true` (default false)
5. If router dies and isn't respawned, workers still accept via EPOLLEXCLUSIVE

This allows A/B testing and safe rollback.

### Phase B: Router-only mode

1. Workers remove EPOLLEXCLUSIVE accept loop
2. Workers only receive fds via control channel
3. `multi:process:router` defaults to true when `multi:process:readers > 0`
4. EPOLLEXCLUSIVE code path deleted

### Phase C: Writer direct routing (optional)

1. Router peeks `tele:syn` for share name
2. Write-designated shares route to writer directly
3. Workers become pure readers (no write forwarding code)
4. This matches GOAL.md criterion #8: "Workers have NO write forwarding code"

Phase C is optional — the research showed that worker-side write forwarding is sufficient for current throughput needs (~250 write ops/s ceiling is event-loop-bound, not forwarding-bound).

---

## Performance Expectations

| Metric | Current (EPOLLEXCLUSIVE) | Expected (Router + FD passing) |
|--------|------------------------|-------------------------------|
| Connection setup | ~0.5ms (accept + handshake) | ~0.6ms (+0.1ms for sendmsg/recvmsg hop) |
| Per-query read latency | ~20ms | ~20ms (unchanged — worker serves locally) |
| Per-query write latency | ~4ms (direct) / ~30ms (forwarded) | ~30ms (forwarded, same as current) |
| Max read QPS | ~1400 (EPOLLEXCLUSIVE contention) | ~2000+ (no accept contention) |
| Router CPU | N/A | <1% (accept + sendmsg only) |

The router adds ~0.1ms per connection (one sendmsg + one recvmsg). This is negligible compared to the telepath handshake (~0.4ms) and query execution (~20ms). The win is eliminating EPOLLEXCLUSIVE contention and giving the arbiter centralized control over connection distribution.

---

## Open Questions

1. **SSL termination:** Currently the dmon handles SSL. With fd passing, the router passes the raw TCP fd before SSL negotiation. The worker's dmon must handle SSL on the received fd. This should work — `asyncio.start_server(ssl=ctx, sock=sock)` accepts a pre-connected socket. Needs verification.

2. **Health checks:** Load balancers (ALB) send health check connections. These will be routed to workers via round-robin. Workers should handle them efficiently (the telepath handshake will fail gracefully for non-telepath probes). May want the router to handle health probes directly in the future.

3. **Connection draining on worker restart:** When a worker is being replaced, the router should stop sending new fds to it. The DRAIN message (0x02) is reserved for this but not implemented in Phase 1.

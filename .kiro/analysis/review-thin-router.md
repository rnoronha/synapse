# Devil's Advocate Review: Thin Router Implementation

**Date:** 2026-05-04
**Branch:** `phase2-multi-process`
**Reviewer:** Kiro (AI agent, senior Python engineer perspective)
**Files:** `router.py` (new, 194 lines), `arbiter.py` (changes), `worker.py` (changes)

---

## Summary

The thin router is a clean, minimal design. The synchronous epoll loop in the router is the right call — no asyncio overhead for what is essentially `accept()` + `sendmsg()`. The fd-passing protocol is correctly implemented at the wire level. However, there are several fd lifecycle bugs, a race condition, and a design-level conflict with the GOAL.md acceptance criteria that need resolution before this ships.

---

## Findings

### F-1. Writer process also accepts on the TCP listen socket — CRITICAL

**Location:** `cell.py:4759` (`_restoreDmonListener(listen_fd)`) and `router.py:_accept_loop`

After `fork_router(listen_fd)`, the parent (writer) continues into `_writer_serve()` which calls `_restoreDmonListener(listen_fd)`. This wraps the same `listen_fd` in an asyncio server and starts accepting connections on it.

Both the router process AND the writer process are now calling `accept()` on the same listening socket. This is exactly the thundering-herd / split-brain scenario the router was designed to eliminate. Connections will be non-deterministically split between the router (which dispatches to workers) and the writer (which handles them directly). The router's round-robin accounting becomes meaningless.

**Fix:** The writer must NOT call `_restoreDmonListener(listen_fd)` when the router is active. The writer should only listen on the UDS endpoint (`worker.sock`) for forwarded writes. Either:
- Remove the `_restoreDmonListener` call in fork mode, or
- Close `listen_fd` in the parent after `fork_router()` returns (the router owns it now).

**Impact:** Defeats the entire purpose of the router. Connections bypass workers unpredictably.

---

### F-2. Parent (arbiter/writer) leaks router-end and worker-end UDS fds — HIGH

**Location:** `arbiter.py:fork_router()` (parent path, line ~190) and `fork_workers()` (parent path)

After `fork_router()` returns in the parent, the parent still holds open file descriptors for both ends of every control channel socketpair (`self._control_channels`). The router child closes the worker-end fds. The worker children close the router-end fds. But the parent closes neither.

Consequences:
1. **Broken HUP detection.** The router monitors worker UDS fds for `EPOLLHUP` to detect dead workers. But `EPOLLHUP` only fires when *all* fds referencing one end of the socketpair are closed. Since the parent holds a copy of the worker-end fd, killing a worker won't trigger `EPOLLHUP` in the router — the parent's fd keeps the socketpair alive. Dead worker detection is silently broken.
2. **fd table bloat.** The writer process carries 2×N unnecessary fds for the lifetime of the process.

**Fix:** After `fork_router()` returns, the parent should close all fds in `self._control_channels`:
```python
for wid, (router_fd, worker_fd) in self._control_channels.items():
    os.close(router_fd)
    os.close(worker_fd)
```

**Impact:** Dead workers are never removed from the router's round-robin pool. Connections sent to dead workers are silently dropped.

---

### F-3. `_do_accept` accepts only one connection per epoll wake — MEDIUM

**Location:** `router.py:_do_accept` (line 144)

The listen socket is non-blocking and level-triggered. When a burst of connections arrives, epoll returns one `EPOLLIN` event, but `_do_accept` calls `accept()` exactly once. The remaining pending connections sit in the backlog until the next `ep.poll()` iteration (up to 1 second if no other events fire, though in practice level-triggered EPOLLIN will re-fire immediately).

This isn't a correctness bug — level-triggered epoll will keep firing — but it adds unnecessary latency under burst load. Each connection pays one extra epoll syscall round-trip.

**Fix:** Loop `accept()` until `BlockingIOError`:
```python
def _do_accept(self, listen_sock):
    while True:
        try:
            conn, addr = listen_sock.accept()
        except (BlockingIOError, OSError):
            return
        # ... dispatch conn
```

**Impact:** Under burst load, connection setup latency increases by one epoll cycle per queued connection. Negligible at steady state.

---

### F-4. `conn.getpeername()` can raise on a racing client disconnect — MEDIUM

**Location:** `worker.py:277`

```python
loop.create_task(self._handle_connection(conn, conn.getpeername()))
```

Between the router's `accept()` and the worker's `getpeername()`, the client can disconnect (TCP RST). `getpeername()` on a disconnected socket raises `OSError: [Errno 107] Transport endpoint is not connected`. This crashes the task creation and leaks the fd (the `conn` socket object is never closed).

**Fix:** Wrap in try/except:
```python
try:
    addr = conn.getpeername()
except OSError:
    conn.close()
    continue
loop.create_task(self._handle_connection(conn, addr))
```

**Impact:** Under normal conditions, rare. Under adversarial conditions (port scanners, health check probes that RST immediately), this will leak fds until the worker hits `ulimit -n` and dies.

---

### F-5. `restart_worker` reuses stale control channel fds — HIGH

**Location:** `arbiter.py:restart_worker` (line 315)

When a worker dies and is respawned, `restart_worker` calls `_in_child(idx)` which reads `self._control_channels[idx]` to get the worker's control fd. But the original socketpair is dead — the worker-end was closed when the old worker exited, and the router-end saw `EPOLLHUP` (assuming F-2 is fixed).

The respawned worker inherits a stale fd number that either:
- Points to nothing (closed) → `send(READY)` fails, worker is never added to router pool
- Has been reused by the OS for something else → data corruption

Additionally, the router has no mechanism to accept a new control channel for the respawned worker. The router's `_worker_fds` map is immutable after init.

**Fix:** Worker restart requires creating a new socketpair and somehow delivering the router-end to the running router process. Options:
1. **Don't restart workers individually.** Restart the entire router + worker set. Simpler, matches the "cattle not pets" philosophy.
2. **Add a control channel from arbiter → router** for injecting new worker fds. More complex but enables true hot restart.
3. **Pre-allocate spare socketpairs** at startup. Router has slots for N+spare workers. Restart uses the next spare.

**Impact:** Worker restart is broken. Respawned workers can never receive connections.

---

### F-6. `_next_worker` sorts on every call — LOW

**Location:** `router.py:_next_worker` (line 163)

```python
pool = sorted(self._alive & self._ready)
```

This creates a new sorted list on every accepted connection. With N workers, this is O(N log N) per connection. Negligible for small N (2-8 workers), but it's unnecessary work in the hot path.

**Fix:** Cache the sorted pool and invalidate on worker add/remove. Or just use a list and rotate index.

**Impact:** Negligible for expected worker counts. Pedantic.

---

### F-7. `EPOLLHUP`-only registration may not fire on all kernel versions — LOW

**Location:** `router.py:118`

```python
ep.register(fd, select.EPOLLHUP)
```

On Linux, `EPOLLHUP` is always reported regardless of the requested mask — you can't filter it out. But registering with *only* `EPOLLHUP` and no `EPOLLIN` is unusual. Some older kernel versions (pre-4.5) had edge cases where HUP-only registrations didn't wake epoll. Modern kernels (5.x+) handle this correctly.

Since this is Linux-only (SCM_RIGHTS requires it), and the deployment target is presumably modern, this is low risk. But `EPOLLIN | EPOLLHUP` is the conventional and safer registration.

**Fix:** `ep.register(fd, select.EPOLLIN | select.EPOLLHUP)` — no downside.

**Impact:** Theoretical. Would only matter on very old kernels.

---

### F-8. No backpressure when all workers are busy — MEDIUM

**Location:** `router.py:_do_accept` and `_next_worker`

The router accepts connections as fast as they arrive and immediately dispatches via `sendmsg`. There's no concept of worker load. If all workers are saturated (e.g., 1000 active sessions each), the router keeps piling on connections via round-robin.

The UDS `sendmsg` will eventually block if the worker's recvmsg isn't keeping up (UDS buffer full), but the router's UDS sockets are inherited as blocking from `socketpair()` — so the router's accept loop will stall on `sendmsg`, which stalls `accept()`, which fills the TCP backlog, which causes `ECONNREFUSED` at the client. This is actually reasonable degradation behavior, but it's accidental rather than intentional.

**Fix (documentation):** Document that backpressure is provided by UDS buffer pressure. Consider making it explicit: set UDS send buffer size, and handle `EAGAIN` on non-blocking UDS sends with a connection queue.

**Impact:** Under extreme load, the router becomes unresponsive to all workers because one slow worker's UDS buffer is full and the blocking `sendmsg` stalls the entire accept loop. This is the real bug — one slow worker blocks dispatch to all workers.

Wait — re-reading `_do_accept`: the UDS socket is created per-call via `socket.socket(fileno=uds_fd)` and the underlying fd came from `socketpair()` which defaults to blocking. So yes, `sendmsg` can block indefinitely on a slow worker, stalling the entire router.

**Revised severity: HIGH.** One slow worker can block the entire router.

**Fix:** Set all worker UDS fds to non-blocking in the router. Handle `EAGAIN` by either queuing or dropping the connection with a log warning.

---

### F-9. Security: telepath auth is preserved — OK (no finding)

The router passes raw TCP fds before any application-layer processing. The worker performs the full telepath handshake including TLS negotiation (if configured) and auth. The fd-passing does not bypass any auth layer.

One subtlety: `_restoreDmonListener` in cell.py checks for SSL context and wraps the asyncio server with it. The worker's `_handle_connection` does:
```python
reader, writer = await asyncio.open_connection(sock=conn)
```
This does NOT apply SSL. If the deployment uses TLS, the worker needs to wrap the socket with the SSL context before creating the asyncio streams. This needs verification against the dmon's `_onLinkInit` — if the dmon handles TLS upgrade internally, it's fine. If it expects the server to have already terminated TLS, connections will fail or be unencrypted.

**Severity: Needs investigation.** If TLS is in use, this could be CRITICAL (connections served without encryption) or a non-issue (dmon handles it).

---

### F-10. GOAL.md acceptance criteria mismatch — HIGH (design)

The GOAL.md states:
- **Criterion 4:** "Write connections are passed to the writer process (not workers)"
- **Criterion 8:** "Workers have NO write forwarding code (pure readers)"

The implementation does the opposite: all connections go to workers, workers forward writes via UDS. The design doc acknowledges this explicitly (§Connection Routing Strategy: "we take that path") and the GOAL.md has an "OR" clause allowing it.

However, the GOAL.md acceptance criteria as written are not met. If these criteria are hard requirements for sign-off, the implementation needs to either:
1. Update GOAL.md to reflect the chosen approach, or
2. Implement direct writer routing in the router (Phase C in the design doc).

**Impact:** Acceptance criteria mismatch. Needs explicit stakeholder alignment.

---

## Findings Summary

| ID | Severity | Category | Summary |
|----|----------|----------|---------|
| F-1 | **CRITICAL** | Thundering herd | Writer also accepts on listen_fd — router is bypassed |
| F-2 | **HIGH** | FD leak | Parent leaks control channel fds → broken HUP detection |
| F-5 | **HIGH** | Lifecycle | Worker restart uses stale socketpair fds — broken |
| F-8 | **HIGH** | Performance | Blocking sendmsg — one slow worker stalls entire router |
| F-10 | **HIGH** | Design | GOAL.md criteria 4 & 8 not met |
| F-3 | MEDIUM | Performance | Single accept per epoll wake under burst |
| F-4 | MEDIUM | FD leak | `getpeername()` races with client disconnect |
| F-6 | LOW | Performance | Sorted pool on every accept |
| F-7 | LOW | Portability | EPOLLHUP-only registration |
| F-9 | — | Security | TLS passthrough needs verification |

## Recommendation

**Do not merge.** F-1 alone defeats the purpose of the router. F-2 makes dead worker detection non-functional. F-5 makes worker restart non-functional. These three must be fixed before the router adds value over the previous EPOLLEXCLUSIVE approach.

Suggested fix order:
1. F-1 — Remove `_restoreDmonListener` call in fork mode (1 line)
2. F-2 — Close control channel fds in parent after `fork_router` (4 lines)
3. F-8 — Set worker UDS fds non-blocking in router, handle EAGAIN (10 lines)
4. F-5 — Decide on restart strategy (design decision, then ~30 lines)
5. F-4 — Wrap `getpeername()` (3 lines)
6. F-3 — Loop accept until EAGAIN (5 lines)

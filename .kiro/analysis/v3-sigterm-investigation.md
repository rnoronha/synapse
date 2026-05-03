# V3 Fork Worker SIGTERM Investigation

**Branch:** phase2-multi-process
**Date:** 2026-05-03
**Symptoms:** 9/16 subtests fail with LinkShutDown; 0/3 reader ports online; "Caught SIGTERM, shutting down" in logs within seconds of fork.

---

## Candidate 1: Signal Propagation in `_initForkMode` — NOT ROOT CAUSE

**Files:** `synapse/lib/cell.py:4645-4770`, `synapse/lib/arbiter.py:94-145`

**Findings:**
- `_initForFork` (cell.py:4801) explicitly does NOT call `cell.main()` or `addSignalHandlers()`. Comment confirms: "main() has not been called — no signal handlers, no waitfini."
- `asyncio.run(_initForFork)` tears down the Phase 1 event loop cleanly. The cell holds an extra `_syn_refs` to prevent fini during teardown.
- After `asyncio.run()` returns, no event loop is running. `arbiter.fork_workers()` calls `os.fork()` in a clean state (no running loop, no threads from the loop).
- The parent does NOT send any signals after fork. It proceeds directly to Phase 3 (`asyncio.run(_writer_serve())`).
- `sys.exit()` / `SystemExit` are not raised anywhere in this path.

**Verdict:** Clean. No signal propagation from the init phase.

---

## Candidate 2: Deploy Script Kill Step — NOT ROOT CAUSE (but fragile)

**File:** `scripts/deploy-and-test.sh:329-344`

**Findings:**
- Kill step: `pkill -f 'synapse.servers.cortex' || true; sleep 2`
- Start step: `nohup setsid python3.11 -m synapse.servers.cortex ... &`
- Kill happens BEFORE start, with a 2-second sleep. No race condition.
- `setsid` creates a new session for the cortex, isolating it from the SSM shell's process group.
- `pkill -f` matches command-line patterns. It kills ALL processes matching `synapse.servers.cortex`. If a previous cortex's workers are still dying during the 2-second window, they'd be killed too — but that's the old cortex, not the new one.

**Verdict:** Not the cause. The kill-before-start sequence is correct. However, `pkill -f` is a blunt instrument — it would kill workers from a previous run if they're still alive. Consider `pkill -P $OLD_PID` or storing the PID.

---

## Candidate 3: Forkserver Pool Interference — CONTRIBUTING FACTOR

**File:** `synapse/lib/processpool.py:22-33`

**Findings:**
- The forkserver pool (`concurrent.futures.ProcessPoolExecutor` with `forkserver` context) is created at import time in `MainProcess`.
- It spawns 2 background threads: `_ExecutorManagerThread` and `QueueFeederThread`.
- After `os.fork()`, children inherit the forkpool object with dead threads and stale pipe fds. This is a classic fork-with-threads hazard.
- Children call `os._exit(1)` after `worker_main` returns, which skips atexit handlers (including `forkpool.shutdown()`). **But see Candidate 4 — if `worker_main` raises, `os._exit` is skipped.**
- The parent's forkpool remains functional (threads survive fork in the parent).
- The forkpool is NOT shut down before fork. `_stop_memlock_threads()` only stops LMDB memlock threads.

**Verdict:** Not the direct SIGTERM source, but a latent hazard. If a child ever exits normally (exception escape — see Candidate 4), the inherited `atexit.register(forkpool.shutdown)` runs in the child, potentially corrupting shared forkserver state.

---

## Candidate 4: SIGTERM Handler Inheritance — ROOT CAUSE (of the log message)

**File:** `synapse/lib/base.py:582-603`

**Findings:**
- `addSignalHandlers()` installs `loop.add_signal_handler(signal.SIGTERM, sigterm)` where `sigterm()` logs **"Caught SIGTERM, shutting down."** and calls `cell.fini()`.
- This handler is installed by `cell.main()`, which is called in `_writer_serve` (Phase 3, writer process only).
- **Workers do NOT have this handler.** `_in_child()` (arbiter.py:142-149) explicitly resets SIGTERM to `signal.SIG_DFL` before calling `worker_main`.
- With `SIG_DFL`, workers die silently on SIGTERM — no log message.

**Critical deduction:** The "Caught SIGTERM, shutting down" message in the logs is from the **writer process**, not the workers. Workers die silently. The writer logs the message, calls `cell.fini()`, `waitfini()` returns, `_writer_serve` returns, and the `finally: arbiter.shutdown()` block sends SIGTERM to all workers — killing them too.

**What sends SIGTERM to the writer?** See Candidate 5.

---

## Candidate 5: Process Group — ROOT CAUSE (of worker death)

**Files:** `synapse/lib/arbiter.py:130-153`, `synapse/lib/worker.py:120-140`

**Findings:**
- Workers do NOT call `os.setsid()` or `os.setpgrp()` after fork.
- All processes (writer + workers) share the same process group (PGID = session leader PID from `setsid` in deploy script).
- Any signal sent to the process group (`kill -TERM -$PGID`, or `pkill` matching the group) hits ALL processes simultaneously.
- The writer has the `addSignalHandlers` SIGTERM handler → logs "Caught SIGTERM" → `cell.fini()` → `arbiter.shutdown()` → sends SIGTERM to workers (redundant, they already got it from the group signal).
- Workers have `SIG_DFL` → immediate death, no log.

**The SIGTERM source:** Most likely the `pkill -f 'synapse.servers.cortex'` from a subsequent deploy-and-test run, or from SSM session cleanup. Since all processes match the pattern, `pkill` sends SIGTERM to every one of them.

**Fix:** Workers should call `os.setpgrp()` after fork to create their own process group, OR the arbiter should manage worker lifecycle exclusively (workers should only receive signals from the arbiter, not from external process-group-wide signals).

---

## Candidate 6 (NEW): No-op `_handle_connection` — ROOT CAUSE (of LinkShutDown)

**File:** `synapse/lib/worker.py:247-265`

**Findings:**
```python
async def _handle_connection(self, conn, addr):
    loop = asyncio.get_running_loop()
    reader, writer = await asyncio.open_connection(sock=conn)
    try:
        pass  # <-- NO-OP! Connection accepted but nothing served
    except Exception:
        logger.exception(...)
    finally:
        writer.close()
```

- Workers accept connections on the shared listening socket via EPOLLEXCLUSIVE.
- The `_handle_connection` method is a **stub** — it accepts the TCP connection, does nothing, and immediately closes it.
- Clients see: connection accepted → connection closed → `LinkShutDown`.
- This is the direct cause of 9/16 subtests failing with LinkShutDown.
- Workers are alive and accepting connections, but they can't serve Telepath protocol because the handler is unimplemented.

**The race:** Workers start accepting connections immediately after fork (Phase 2). The writer doesn't re-create its listener until Phase 3 (`_writer_serve` → `_restoreDmonListener`). During this window, ALL incoming connections go to workers (they're the only ones listening). Workers accept and immediately drop them.

Even after the writer's listener is up, EPOLLEXCLUSIVE distributes connections across all processes. ~N/(N+1) connections go to workers (where N = worker count), all of which are dropped.

---

## Candidate 7 (NEW): Exception Escape in `_fork_one` — LATENT BUG

**File:** `synapse/lib/arbiter.py:130-140`

```python
def _fork_one(self, worker_id):
    pid = os.fork()
    if pid == 0:
        self._in_child(worker_id)  # if this raises...
        os._exit(1)               # ...this is SKIPPED
    self._worker_pids.append(pid)
```

- If `_in_child` → `worker_main` raises an exception (e.g., LMDB open failure), `os._exit(1)` is never reached.
- The child continues executing the parent's code path: the `for` loop in `fork_workers` forks MORE children, then `asyncio.run(_writer_serve())` runs — creating a second writer.
- The `atexit` handlers run on normal exit, including `forkpool.shutdown()` on the stale inherited pool.
- **Fix:** Wrap `_in_child` in try/except with `os._exit(1)` in a `finally` block.

---

## Summary of Root Causes

| # | Cause | Severity | Explains |
|---|-------|----------|----------|
| **6** | No-op `_handle_connection` stub | **Critical** | 9/16 LinkShutDown failures — workers accept connections but serve nothing |
| **5** | No process group isolation | **High** | Workers killed by group-wide SIGTERM; "Caught SIGTERM" in writer logs |
| **4** | SIGTERM handler on writer only | **Info** | Log message is from writer, not workers (workers die silently) |
| **7** | Exception escape in `_fork_one` | **Medium** | Latent: child becomes zombie writer if `worker_main` raises |
| **3** | Inherited forkpool in children | **Low** | Latent: stale threads/pipes after fork; atexit hazard if child exits normally |

## Recommended Fixes (priority order)

### 1. Implement `_handle_connection` (Critical)
Wire the worker's connection handler to the Telepath/Dmon protocol so workers can actually serve read queries. Until this is done, fork mode is non-functional — every connection routed to a worker is dropped.

### 2. Process group isolation (High)
```python
# In arbiter.py _in_child(), add before signal resets:
os.setpgrp()  # Detach from parent's process group
```
This prevents external `pkill`/`kill -$PGID` from hitting workers directly. Only the arbiter should manage worker lifecycle.

### 3. Bulletproof `_fork_one` (Medium)
```python
def _fork_one(self, worker_id):
    pid = os.fork()
    if pid == 0:
        try:
            self._in_child(worker_id)
        except BaseException:
            logger.exception('Worker %d failed to start', worker_id)
        finally:
            os._exit(1)
    self._worker_pids.append(pid)
```

### 4. Neutralize inherited forkpool in children (Low)
```python
# In arbiter.py _in_child(), add:
import synapse.lib.processpool as s_processpool
if s_processpool.forkpool is not None:
    s_processpool.forkpool = None  # Prevent accidental use or atexit shutdown
```

### 5. Delay worker accept until writer is ready (Medium)
Add a readiness signal (e.g., pipe or file) from the writer to workers. Workers should not call `epoll.register` on the listening socket until the writer signals that Phase 3 setup is complete. This prevents the race where workers accept connections before the writer's UDS endpoint exists.

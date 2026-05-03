# V3 Fork Architecture — Devil's Advocate Code Review

**Branch:** `phase2-multi-process`
**Date:** 2026-05-03
**Modules:** `synapse/lib/arbiter.py`, `synapse/lib/worker.py`, `synapse/cortex.py` (diff), `synapse/lib/cell.py` (diff)

---

## 1. Fork Safety

### F-1. `_memorylockloop` thread survives into init phase — CRITICAL

`Slab.__anit__` spawns a background thread via `s_coro.executor(self._memorylockloop)` when `lockmemory=True`. This thread calls `mmap()`, `mlock()`, and blocks on `self.resizeevent.wait()`.

`_close_all_slabs()` in arbiter.py calls `slab.lenv.close()` and clears `allslabs`, but **does not join or signal the memlock thread**. If `lockmemory` was enabled on any slab during init, the thread is still alive when `os.fork()` is called. POSIX fork only duplicates the calling thread — the memlock thread vanishes in the child, leaving `resizeevent` in an undefined state and the `threading.Event` lock potentially corrupted.

In the parent, the thread continues running against a closed `lenv`, which will segfault or raise.

**Fix:** Before `_close_all_slabs()`, iterate slabs and for each with `memlocktask is not None`: set `isfini = True`, `resizeevent.set()`, and await/join the memlock task. This requires the init event loop to still be running, which conflicts with the current "loop closed before fork" design. Alternative: ensure `lockmemory=False` is enforced when fork mode is active, and assert on it.

### F-2. `atexit` handler iterates all `Base` objects in forked children — HIGH

`synapse/lib/base.py` registers `_fini_atexit` via `atexit.register()`. After fork, the child inherits this handler. When `worker_main` eventually exits (or crashes), the atexit handler runs `gc.get_objects()` and attempts to call `.fini()` on every `Base` instance — including the parent's Cortex, Cell, AuthGate, etc. These objects have stale event loop references and partially-closed resources.

The arbiter's `_in_child` resets signal handlers but **never clears or disables the atexit handler**.

**Fix:** In `_in_child()`, call `atexit.unregister(_fini_atexit)` (import from `synapse.lib.base`), or have `worker_main` always exit via `os._exit()` which skips atexit handlers. Currently `worker_main` does NOT call `os._exit()` — it returns normally after `loop.close()`, which triggers atexit.

### F-3. `asyncio` default `ThreadPoolExecutor` threads — MEDIUM

`asyncio.run()` in Phase 1 (`_initForFork`) creates a default `ThreadPoolExecutor` for `loop.run_in_executor(None, ...)`. Any `s_coro.executor()` calls during init submit work to this pool. When `asyncio.run()` returns, it calls `executor.shutdown(wait=True)`, so threads *should* be joined. However, if any executor task is still running (e.g., a slow cert generation or disk I/O), the shutdown blocks indefinitely, or if it times out, threads leak into the fork.

**Mitigation:** This is likely safe in practice because `asyncio.run()` cancels all tasks and shuts down the executor. But there's no explicit assertion that no threads are alive before fork. Add `assert threading.active_count() == 1` before `os.fork()` as a safety check.

### F-4. `processpool.forkpool` may be initialized during init — MEDIUM

`synapse/lib/processpool.py` creates a `ProcessPoolExecutor` registered with `atexit`. If any code path during Cortex init triggers the process pool, those child processes are inherited across fork. The arbiter doesn't account for this.

**Fix:** Check if `processpool.forkpool` is initialized and shut it down before forking.

---

## 2. LMDB Handling

### L-1. Double-close in arbiter + worker — CRITICAL

`_close_all_slabs()` in arbiter.py calls `slab.lenv.close()` on every slab and clears `allslabs`. Then in the child, `_reopen_lmdb_readonly()` iterates `s_lmdbslab.Slab.allslabs.values()` — but the arbiter already cleared it. The list is empty. **No slabs are re-opened.**

The arbiter's `_close_all_slabs()` runs in the parent *before* fork. After fork, the child inherits the already-cleared `allslabs` dict. `_reopen_lmdb_readonly` finds nothing to re-open.

**Fix:** Either (a) don't clear `allslabs` in the arbiter — just close the `lenv` handles so the child inherits the slab metadata (paths) but not the mmap, then let the worker re-open; or (b) pass the list of slab paths explicitly to `worker_main` so it can re-open them independently of `allslabs`.

### L-2. `_reopen_lmdb_readonly` uses raw `lmdb.open`, bypasses `Slab` — HIGH

The worker re-opens LMDB via `lmdb.open()` directly and stashes envs in `_readonly_envs`, a module-level dict. But all existing Synapse code reads data through `Slab` methods (`scanByFull`, `get`, `scanByPref`, etc.). The worker's `_execute_read` is `NotImplementedError` — when it's wired up, it will need `Slab` instances, not raw `lmdb.Environment` objects.

This means the re-open strategy will need to be rewritten for Phase 2 anyway. The current code is dead — it can't serve any reads.

**Risk:** When Phase 2 wires this up, the temptation will be to construct `Slab` objects around the raw envs, which will fight with `Slab.__anit__`'s assumptions (it expects to open the env itself, checks `allslabs` for duplicates, starts sync loops, etc.).

### L-3. `map_size` set to current file size — MEDIUM

```python
mapsize = os.path.getsize(os.path.join(path, 'data.mdb'))
```

If the writer grows the database after the worker opens it, the worker's readonly env has a stale `map_size` and reads of new pages will fail with `MDB_MAP_RESIZED`. LMDB readonly mode requires the reader to detect this and re-open or call `env.set_mapsize()`.

**Fix:** Either set `map_size=0` (LMDB uses the current file size) or implement a resize-detection loop. The `Slab` class already handles this for its readonly refresh loop — another reason to use `Slab` instead of raw `lmdb.open`.

### L-4. `readahead=True` on workers — LOW

Workers set `readahead=True`. For random-access read patterns (Storm queries hitting arbitrary nodes), this causes the kernel to prefetch pages that won't be used, wasting I/O bandwidth and polluting the page cache. The parent Cortex likely uses `readahead=False` for this reason.

**Fix:** Default to `readahead=False` for worker readonly envs, or inherit the parent's setting.

---

## 3. Event Loop Lifecycle

### E-1. `_initForFork` returns a live `Cell` with a dead event loop — CRITICAL

`asyncio.run(cls._initForFork(...))` creates the Cell, initializes all subsystems (nexus, hive, auth, dmon, etc.), then the loop is destroyed when `asyncio.run()` returns. The Cell object is returned to `startmain()` with:

- `self.loop` pointing to a closed loop
- All `schedCoro` tasks cancelled
- All `onfini` callbacks referencing the dead loop
- `Slab.synctask` cancelled (the class-level sync loop)
- `s_glob._glob_loop` stale

Phase 3 (`_writer_serve`) creates a new loop via `asyncio.run()`, but the Cell's internal `self.loop` is never updated. Any code that does `self.loop.call_soon_threadsafe(...)` or checks `self.loop.is_running()` will fail or silently drop work.

**Fix:** `_writer_serve` must call something like `cell._rebindToLoop(asyncio.get_running_loop())` that updates `self.loop`, re-registers `s_glob._glob_loop`, and re-initializes `Slab.synctask`. This is a deep change — `Base.__anit__` captures `self.loop = asyncio.get_running_loop()` and many subsystems cache it.

### E-2. `_restoreDmonListener` re-creates server but not the dmon's link tracking — HIGH

`_restoreDmonListener` calls `asyncio.start_server(onconn, sock=sock, ssl=sslctx)` with a hand-rolled `onconn` callback. But the real dmon listener setup (in `Daemon.listen()`) does additional work: hostname validation, protocol negotiation, link info population, and registering the server for graceful shutdown. The hand-rolled version skips all of this.

When a client connects to the writer after fork, the `info` dict only has `{'tls': bool}` — missing `hostname`, `addr`, and other fields that downstream code may expect.

### E-3. `_fireActiveCoros` re-fires coros that may not be safe to restart — MEDIUM

Active coros are re-fired in the new event loop, but some coros may have accumulated state (e.g., a cron scheduler that already computed next-fire times against the old loop's clock, or a service mirror that holds stale connection state). Blindly re-firing them assumes they're idempotent from a cold start, which isn't guaranteed.

### E-4. `listenservers.clear()` without closing servers — LOW

In `_writer_serve`, `cell.dmon.listenservers.clear()` drops references to the old asyncio servers without calling `server.close()`. The servers are already dead (their loop is gone), so this is cosmetically fine, but it means any `onfini` callbacks that iterate `listenservers` to close them will miss the stale entries. Not a real bug since the loop is dead, but it's sloppy.

---

## 4. Error Handling

### R-1. Fork failure is unhandled — HIGH

`os.fork()` can raise `OSError` (e.g., `ENOMEM`, `EAGAIN`). Neither `_fork_one` nor `fork_workers` catches this. If fork fails mid-way (e.g., worker 2 of 4 fails), the arbiter has partially-forked workers but no error recovery:

- `_worker_pids` has entries for workers 0-1 but not 2-3
- The parent continues to `_writer_serve` with fewer workers than expected
- No log message indicates the fork failure

**Fix:** Catch `OSError` in `_fork_one`, log it, and either retry or abort. If aborting, kill already-forked workers before raising.

### R-2. Worker crash during init (`_reopen_lmdb_readonly`) — HIGH

If `_reopen_lmdb_readonly` fails (e.g., `data.mdb` doesn't exist, permission error), the exception propagates up through `worker_main`, through `_in_child`, and hits the `os._exit(1)` in `_fork_one`. But `os._exit(1)` is in the *parent's* code path after `_in_child` returns — it's the belt-and-suspenders guard. The actual failure path is:

1. `worker_main` raises
2. `_in_child` doesn't catch it
3. Exception propagates to `_fork_one`'s `if pid == 0:` block
4. `os._exit(1)` runs

This works, but the exception is silently swallowed — no log, no indication of *why* the worker died. The parent sees SIGCHLD with exit code 1 and respawns, creating a crash loop.

**Fix:** Wrap `_in_child` body in try/except, log the exception, then `os._exit(1)`.

### R-3. Infinite respawn loop — MEDIUM

If a worker crashes immediately on startup (e.g., LMDB re-open always fails), `_handle_sigchld` calls `restart_worker`, which forks again, which crashes again, ad infinitum. There's no backoff, no max-restart count, no circuit breaker on respawns.

**Fix:** Track restart timestamps per slot. If a worker crashes more than N times in M seconds, stop respawning and log a critical error.

---

## 5. Signal Handling

### S-1. `_handle_sigchld` calls `restart_worker` (which calls `os.fork()`) inside a signal handler — CRITICAL

Signal handlers in Python run between bytecodes in the main thread. Calling `os.fork()` from inside a signal handler is async-signal-unsafe. POSIX only guarantees a small set of functions are safe to call from signal handlers, and `fork()` is technically on that list, but:

- Python's signal handler runs with the GIL held
- `os.fork()` in CPython acquires internal locks (import lock, etc.)
- If the main thread was in the middle of a malloc or import when SIGCHLD arrived, the child inherits a corrupted heap or deadlocked import lock

More practically: `restart_worker` also calls `logger.info()`, which acquires the logging lock. If the main thread was logging when SIGCHLD fired, **deadlock**.

**Fix:** The signal handler should only set a flag or write to a `self-pipe`. The main loop (which in the parent is `asyncio.run(_writer_serve)`) should poll the flag and call `restart_worker` from normal code flow. Use `loop.add_signal_handler(signal.SIGCHLD, ...)` inside the writer's event loop instead.

### S-2. SIGCHLD handler installed before first fork, races with fast child exit — LOW

`_install_parent_signals` is called before the fork loop. If a child exits extremely fast (before the parent returns from `os.fork()`), SIGCHLD fires before `_worker_pids.append(pid)`. The handler calls `os.waitpid(-1, WNOHANG)`, gets the pid, but `pid in self._worker_pids` is False, so the dead child is reaped but not tracked — it's silently lost.

**Fix:** Append the pid to `_worker_pids` *before* the child can possibly exit (not possible with fork), or handle unknown pids in the SIGCHLD handler by logging a warning.

### S-3. No SIGTERM handler in the parent writer process — MEDIUM

The arbiter installs SIGCHLD but not SIGTERM. `startmain` in cell.py runs `asyncio.run(_writer_serve)` which eventually calls `cell.main()` → `cell.waitfini()`. If SIGTERM arrives, the default handler raises `SystemExit`, which unwinds through `asyncio.run`, hits the `finally: arbiter.shutdown()` block. This *works* but is fragile — if any code catches `SystemExit` (or `BaseException`) along the way, shutdown stalls.

**Fix:** Explicitly install SIGTERM in the writer's event loop via `loop.add_signal_handler` to call `cell.fini()` cleanly.

---

## 6. EPOLLEXCLUSIVE

### EP-1. No fallback if `EPOLLEXCLUSIVE` is unavailable — HIGH

`select.EPOLLEXCLUSIVE` was added in Python 3.9 / Linux 4.5. The code does:

```python
epoll.register(self._listen_fd, select.EPOLLIN | select.EPOLLEXCLUSIVE)
```

If the kernel doesn't support it, `epoll.register` raises `OSError` with `errno.EINVAL`. This crashes the worker on older kernels with no fallback.

**Fix:**
```python
flags = select.EPOLLIN
if hasattr(select, 'EPOLLEXCLUSIVE'):
    flags |= select.EPOLLEXCLUSIVE
epoll.register(self._listen_fd, flags)
```

Log a warning when falling back so operators know they'll get thundering herd.

### EP-2. `epoll.poll()` in a thread executor blocks the event loop's thread pool — MEDIUM

```python
events = await loop.run_in_executor(None, epoll.poll, 1.0)
```

This uses the default `ThreadPoolExecutor` (typically 5 threads per CPU). Each call blocks a thread for up to 1 second. Since this is the main accept loop, one thread is permanently occupied. If other code in the worker also uses `run_in_executor(None, ...)`, the pool can starve.

**Fix:** Use `loop.add_reader(self._listen_fd, callback)` instead of epoll-in-executor. This integrates with the event loop's native epoll instance. EPOLLEXCLUSIVE can be set on the fd before registering it with the loop via `epoll_ctl` directly if needed, or accept the thundering-herd tradeoff for the simpler `add_reader` approach.

### EP-3. Only one connection accepted per epoll wakeup — LOW

`_do_accept` calls `listen_sock.accept()` once. Under high load with EPOLLEXCLUSIVE, only one worker wakes per incoming connection, but if multiple connections arrive simultaneously, the worker only accepts one and must wait for the next epoll cycle. This limits throughput under burst load.

**Fix:** Loop `accept()` until `BlockingIOError` in `_do_accept`.

---

## 7. UDS Write Forwarding

### U-1. No backpressure on the Telepath UDS proxy — HIGH

`_forward_write` acquires a semaphore (limit 5) and then streams results from `proxy.storm(text, opts=opts)`. The semaphore limits concurrent *forwards*, but there's no limit on the total bytes buffered. If the writer is slow and 5 concurrent forwards are all yielding large result sets, memory grows unbounded in the worker.

The Telepath proxy uses `asyncio.StreamWriter` under the hood, which buffers writes. If the UDS socket's send buffer fills, `StreamWriter.write()` buffers in userspace with no upper bound (asyncio's `StreamWriter` doesn't have built-in high-water-mark backpressure for the caller).

**Fix:** Monitor `writer.transport.get_write_buffer_size()` and pause reading from the proxy when it exceeds a threshold. Or set `set_write_buffer_limits()` on the transport and handle `pause_writing`/`resume_writing` protocol callbacks.

### U-2. Semaphore timeout of 30s is arbitrary and not configurable — LOW

The 30-second timeout on `self._write_sem.acquire()` is hardcoded. Under sustained write load, legitimate writes will be rejected with `'Write forwarding backlogged'` even though the writer is healthy — just busy.

**Fix:** Make configurable, or use an adaptive timeout based on observed write latency.

### U-3. `_get_writer_proxy` reconnect has no backoff — MEDIUM

If the writer's UDS endpoint is temporarily unavailable (e.g., during writer restart), every incoming write request calls `s_telepath.openurl()`, which attempts a TCP/UDS connect. Under load, this creates a connection storm against the UDS path.

**Fix:** Use the circuit breaker's state to gate reconnection attempts. When the circuit is half-open, allow exactly one probe connection.

---

## 8. Circuit Breaker

### CB-1. `is_open` has a side effect that creates a race — HIGH

```python
@property
def is_open(self):
    if self._state == 'open':
        if time.monotonic() - self._last_failure > self._recovery_timeout:
            self._state = 'half-open'
            return False
        return True
    return False
```

`is_open` mutates `_state` from `open` → `half-open` as a side effect of being *read*. In an async context, multiple coroutines can call `is_open` concurrently (between awaits). Two coroutines can both see `_state == 'open'`, both transition to `half-open`, and both proceed to send a probe request. The half-open state should allow exactly one probe.

**Fix:** Use an `asyncio.Lock` or atomic flag to ensure only one coroutine transitions to half-open and sends the probe. Or restructure: `is_open` should be pure; add a separate `try_acquire_probe() -> bool` method.

### CB-2. `record_success` from half-open doesn't verify it was the probe — MEDIUM

Any successful write (even one that was already in-flight before the circuit opened) calls `record_success()`, which resets to `closed`. This can mask an ongoing failure — the circuit opens, a stale in-flight request succeeds, circuit closes, new requests fail again, circuit re-opens. Oscillation.

**Fix:** Track a generation counter. Only `record_success` from the probe generation should close the circuit.

### CB-3. `_failures` counter never resets on partial success — LOW

If the system has 2 failures, then 1 success (resetting to 0), then 2 more failures, the circuit never opens (threshold is 3). This is arguably correct (the success proves the writer recovered), but it means transient failures that alternate with successes never trip the breaker, even if the failure *rate* is high.

This is a design choice, not a bug. Document the behavior.

### CB-4. `recovery_timeout` of 1.0s is very aggressive — LOW

A 1-second recovery timeout means the circuit tries to re-probe almost immediately. If the writer is down for maintenance (e.g., LMDB resize), 1 second is too short and the circuit will oscillate between open and half-open rapidly, generating log spam.

**Fix:** Use exponential backoff on the recovery timeout (e.g., 1s → 2s → 4s → ... → 30s cap).

---

## 9. API / Type Consistency

### A-1. Arbiter docstring says `socket.socket`, code receives `int` fd — MEDIUM

`fork_workers` docstring:
> listen_sock: The bound/listening ``socket.socket`` workers inherit.

But `cell.py` passes `listen_fd` (an `int`):
```python
arbiter.fork_workers(count, listen_fd, uds_path, _worker_entry)
```

The parameter is named `listen_sock` in the arbiter but it's an fd. The worker correctly treats it as an fd. The docstring is wrong and will mislead future maintainers.

**Fix:** Rename to `listen_fd: int` in the arbiter, update docstring.

### A-2. `_worker_entry` ignores `worker_id` — LOW

```python
def _worker_entry(listen_sock, uds_path_arg, worker_id):
    s_worker.worker_main(listen_sock, uds_path_arg, datadir)
```

`worker_id` is accepted but not passed to `worker_main`. Workers can't identify themselves in logs. `worker_main` doesn't accept a `worker_id` parameter either.

**Fix:** Pass `worker_id` through to `worker_main` and use it in log messages (e.g., `f'Worker {worker_id} (pid {os.getpid()})'`).

---

## Summary by Severity

| Severity | Count | IDs |
|----------|-------|-----|
| CRITICAL | 4 | F-1, L-1, E-1, S-1 |
| HIGH | 7 | F-2, L-2, R-1, R-2, EP-1, U-1, CB-1 |
| MEDIUM | 8 | F-3, F-4, L-3, E-2, E-3, R-3, S-3, EP-2, U-3, CB-2, A-1 |
| LOW | 6 | L-4, E-4, S-2, EP-3, U-2, CB-3, CB-4, A-2 |

## Recommendation

The four CRITICALs are all structural — they can't be fixed with point patches:

1. **F-1 + E-1** (memlock thread + stale loop): The "init loop → destroy → fork → new loop" lifecycle doesn't account for threads spawned during init or the Cell's deep coupling to its birth loop. This needs a design rethink — either (a) don't destroy the init loop (fork with loop quiesced but alive, then close in child), or (b) add a `Cell.rebind(loop)` method that walks the entire object graph.

2. **L-1** (allslabs cleared before fork): The arbiter and worker have contradictory assumptions about who owns slab cleanup. Pick one owner.

3. **S-1** (fork inside signal handler): Move all mutation out of the SIGCHLD handler. Use a self-pipe or `loop.add_signal_handler`.

I'd fix the CRITICALs before any further development on this branch. The HIGHs are fixable incrementally.

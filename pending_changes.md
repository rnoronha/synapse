# Pending Changes — Thick Router

## To Apply

1. **callStorm should be classified like storm, not always routed to writer**
   - File: synapse/lib/thickrouter.py line 322
   - Current: `if methname == 'callStorm':` → always writer
   - Fix: classify with regex like storm, route reads to workers
   - Reason: callStorm is often a pure read (e.g., `return($lib.layer.get().iden)`). Routing all callStorm to writer bypasses the worker pool unnecessarily.

2. **Replace bare `except OSError: break/pass/return` with info logging**
   - File: synapse/lib/worker.py (PureWorker)
   - Current: `except OSError: break` with no context
   - Fix: Add `logger.info('PureWorker %d: router disconnected (fd %d), shutting down', self._pid, self._task_fd)` or similar before break/return
   - Reason: bare except/break is opaque when debugging. An info log with the worker ID and fd makes it clear what happened and why.

3. **Restore `if __debug__:` guards on `_migration_evnt`**
   - File: synapse/cortex.py lines 962, 1803, 1811
   - Current: always creates/sets/clears the event
   - Fix: wrap in `if __debug__:` to match upstream master
   - Reason: this is a test-only mechanism (tests wait on it to know migrations finished). The guard makes it zero-cost in production. We accidentally removed it.

4. **Pass readonly via conf dict to Axon and JsonStor, not as separate kwarg**
   - File: synapse/cortex.py lines 4730, 4815
   - Current: `Axon.anit(path, conf=conf, parent=self, readonly=self.readonly)`
   - Fix: `conf['readonly'] = self.readonly` then `Axon.anit(path, conf=conf, parent=self)`
   - Reason: readonly is a cell configuration concern, should flow through conf like other settings. Or better: the child cell should inherit readonly from its parent.

5. **Create session at link init time, not lazily in _onTeleSyn**
   - File: synapse/daemon.py
   - Current: Session created in _onTeleSyn, with fallback re-creation in _onTaskV2Init for pool/fork scenarios
   - Fix: Create Sess in _onLinkInit (every link gets a session immediately). _onTeleSyn and _onTaskV2Init just look it up — no lazy creation, no "does it exist?" branching.
   - Reason: Simplifies the session lifecycle. Eliminates the convoluted fallback path in _onTaskV2Init. Every link always has a session.

## Blocked (waiting on startmain fix)

2. **startmain path: workers steal connections from router** ⚠️ BLOCKER
   - **Root cause CONFIRMED**: Workers inherit the listen fd (via the cell's Daemon server socket). Multiple processes accept() on the same fd. The kernel load-balances connections across them. Workers accept connections, process tele:syn, send replies — but the client connected to the ROUTER's port, so the worker's reply goes to a different accepted socket than the client expects.
   - drain() does NOT hang. The server sends bytes successfully. But the client never receives them because a WORKER accepted the connection instead of the router.
   - **Why the integration test works**: It creates a NEW socket in the router child (on a dynamic port), so there's no fd sharing.
   - **Fix plan**: In `_initForFork` (cortex.py), after getting the listen_fd:
     1. `os.dup(listen_fd)` → `router_fd` (private copy for the router)
     2. Close the original listen_fd AND the Daemon's server socket
     3. Pass `router_fd` in fork_info
     4. Workers never see the listen socket
     5. Router uses `router_fd` in `_router_main`
   - Alternative: Set `SO_REUSEPORT` on the socket at creation time (in `initServiceNetwork`), then the router can create a fresh socket. But this requires all holders to have SO_REUSEPORT.
   - **Key insight**: The dup must happen BEFORE `asyncio.run()` returns in `_initForFork`, while we still have access to the server's socket objects.

## Already Applied (this session)

- Removed genpath/gendir readonly conditional (use gendir unconditionally)
- _initSlabFile uses self.readonly (no parameter)
- MultiSlabSeqn: readonly in slabopts, simplified conditional
- nexus.py: readonly from cell (no explicit param)
- Removed READONLY_REFRESH_PERIOD dead constant
- Removed internal bug names (E-1, E-6, S-1, F-1, etc.)
- Renamed thick_router → router in arbiter.py
- Removed ReadOnlyWorker class (~300 lines dead code)
- Removed queryrouter.py (dead code)
- Simplified worker_main signature (dispatch_fd, datadir, cell)
- Workers create fresh forkpool for AST parsing
- Wrapped req_id counter at 32 bits

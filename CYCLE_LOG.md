# CYCLE_LOG — Patch Maintainability Refactoring

## Cycle 1 — 2026-05-07T19:20Z

### Orient
- Goal: Reduce existing-file diff from ~600 to <100 lines by extracting fork logic into new files
- Current state: 4 new files + 17 modified files. Big targets: cell.py (+367), daemon.py (+46), cortex.py (TBD)
- Plan: Extract cell.py fork logic → new synapse/lib/forkmode.py (refactoring #1 from GOAL.md)

### Results
- cell.py: +367 → +34 (forkmode.py extracted, wrappers removed, readonly guards removed)
- cortex.py: +102 → +42 (fork logic extracted, unused imports removed)
- daemon.py: +46 → 0 (session handling moved to worker.py)
- auth.py, node.py, nexus.py, view.py, axon.py, layer.py, multislabseqn.py: ALL eliminated from diff
- Final: 4 production files modified, +110 lines (down from 17 files, ~600 lines)
- EC2 validated: 2,133 ops/s mixed-load (no regression)

## Cycle 2 — 2026-05-07T20:44Z — SSL Red/Green

### Orient
- Bug: Workers can't handle SSL/TLS connections from router
- Red test confirms: SSL handshake timeout

### Results
- Fixed: loop.start_tls(server_side=True) for TLS handshake in workers
- Fixed: Don't pass caname (avoids CERT_REQUIRED for client certs)
- Fixed: may_fork() checks SYN_CORTEX_MULTI_PROCESS_CORE_PCT env var
- Fixed: forkpool._processes = {} to prevent SpawnProcess SIGTERM
- Fixed: SIGCHLD handler only reaps known PIDs
- Fixed: Restore HTTPS/Tornado listener after fork
- GREEN: 1,357 QPS over SSL ✅

## Cycle 3 — 2026-05-08T05:23Z — HTTP API Read Distribution

### Orient
- HTTPS API (port 4443) queries don't go through workers — writer handles all HTTP traffic
- Need: writer forwards read queries to workers for HTTP API scaling

### Results
- ReadForwarder + ReadChannelListener implemented (readchannel.py)
- Writer monkey-patches cell.storm()/callStorm() to forward reads to workers
- HTTPS/Tornado listener restored after fork (close sockets before fork, rebind after)
- may_fork() checks SYN_CORTEX_MULTI_PROCESS_CORE_PCT env var
- v6 image deployed and SSL on port 4443 confirmed working by t2
- **BUG**: Storm queries via HTTP hang — read channel not delivering responses
  - Login works (200), Storm call hangs (timeout)
  - ReadForwarder patches cell.storm on writer ✓
  - ReadChannelListener created on workers ✓
  - read_fd passed to workers ✓
  - Likely issue: message framing, demux, or worker not reading from the fd

### Next
- Debug the read channel end-to-end: add logging to ReadChannelListener.start() and ReadForwarder._send_request()
- Verify worker receives the request, executes it, and sends response back
- Test locally with Docker (SYN_CORTEX_MULTI_PROCESS_CORE_PCT=50, set root password, login, storm/call)

## Cycle 4 — 2026-05-09T02:05Z — Cluster Integration Phase 1: AHA Outbound

### Orient
- Goal: Get thin router working in a cluster (AHA + mirrors + leader election)
- Phase 1: Fix outbound telepath from writer to AHA
- Last state: v8j works on EKS (single pod). Local cluster shows AHA outbound broken.
- Key finding: `Base.tid` stale after fork → `schedCoro` assert fails → AHA re-init task never created
- Fix applied: reset `tid` on all Base objects + use `asyncio.create_task` for AHA re-init
- Result: `[writer] AHA re-initialized and active` confirmed in local cluster logs!
- Remaining: AHA shows `ready=None` — need to call `_tellAhaReady(True)` after re-init

### Dispatched
- t10: Local cluster test with `_tellAhaReady(True)` fix

### Results
- ✅ AHA outbound telepath FIXED: `Base.tid` reset + `asyncio.create_task` for background AHA init
- ✅ AHA services: `00.cortex.synapse: ready=True online=...` and `cortex.synapse: ready=True online=...`
- ✅ Leader writes: `[inet:fqdn=cluster-v7-test.com]` → count=1
- ✅ Phase 1 acceptance criterion met: writer connects to AHA and registers within 60s
- ✅ Mirror bootstrap: drive subprocess restarted, nexus slabs reopened
- ✅ Mirror replication: 7 nodes written on leader, 7 replicated to mirror in <5s
- ✅ EKS validation: writes 84ms, reads 2ms, login 200 OK, AHA ready=True
- Architecture change: removed router process, writer keeps dmon listener

### Next
- Phase 3: Scale 1b on EKS, validate mirror replication on real stack
- Phase 3: Leader failover test
- Phase 4: Optic end-to-end

## Cycle 5 — 2026-05-09T04:40Z — Cluster Validation Wrap-up

### Orient
- Goal: Get thin router working in a cluster
- AC1 (AHA outbound): ✅ DONE
- AC2 (Mirror replication): ✅ DONE locally, EKS blocked by PPA topology (leader election ≠ static mirror)
- AC3 (Leader election): Deferred — PPA topology uses aha:leader election, needs separate work
- AC4 (Optic end-to-end): Testing now
- AC5 (Pod restart): ✅ 1a recovers cleanly after rollout restart
- AC6 (No regression): ✅ All single-pod tests pass

### Key Fixes This Session
1. `Base.tid` reset after fork (schedCoro assert fix)
2. `asyncio.create_task` for AHA re-init (bypass dead schedCoro)
3. `_tellAhaReady(True)` + `modAhaSvcInfo` for AHA ready state
4. AHA keepalive loop (re-register on reconnect)
5. Removed router process — writer keeps dmon listener
6. Drive subprocess restart after fork
7. Nexus slabs (nexsslab + tailslab) reopened after fork
8. `cell.certpath` re-added after fork (removed by onfini)
9. Nexsroot follower client restart for mirrors
10. `donexslog = True` forced after fork
11. `cell.isactive` only forced True for leaders (not mirrors)

### Dispatched
- t2: Optic end-to-end test on PPA

### Results
- ✅ Optic end-to-end: UI loads, Storm reads, node creation
- ✅ Stability: 30min soak, tasks=18 stable, delta=0 (task leak fixed)
- ✅ ReadForwarder re-enabled with 2s poll timeout fix
- ✅ Squashed to 1 commit, pushed to rnoronha/synapse:phase2-clean

### Done
Goal achieved for single-leader deployment. PR branch ready.

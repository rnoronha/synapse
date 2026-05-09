# GOAL: Get the Thin Router Working in a Cluster

## End State

The thin router (multi-process fork mode) works correctly in the full Pushpin cluster topology: multiple cortex pods with AHA service discovery, leader election, mirror replication, and Optic connectivity. All operations that work in single-process mode also work in fork mode across the cluster.

## What "Done" Looks Like

1. Both cortex-us-east-1a (leader) and cortex-us-east-1b (mirror) run with fork mode enabled
2. AHA shows both services as `ready=True` with correct leader designation
3. Optic connects via `aha://cortex...` and functions normally (queries, node creation, UI)
4. Mirror replication works: writes on leader propagate to mirror
5. Leader failover works: if 1a dies, 1b promotes to leader and Optic reconnects
6. No write hangs, no auth failures, no stale connections after pod restarts

## Acceptance Criteria

1. **AHA outbound reconnection**: Writer's telepath client connects to AHA after fork and registers as leader/mirror within 60s of startup
2. **Mirror replication**: Nexus log entries from leader reach mirror within 5s (same as stock Synapse)
3. **Leader election**: `aha:leader` service shows correct active node; failover completes within 30s
4. **Optic end-to-end**: Login, Storm queries, node creation all work through the web UI
5. **Pod restart resilience**: After `kubectl rollout restart`, both pods recover to healthy state without manual intervention
6. **No regression**: All v8j single-pod tests still pass (reads, writes, SSL, HTTP API, worker dispatch)

## Known Issues from v8j

1. **AHA outbound telepath broken**: The writer's `_initAhaRegistry()` creates a new Client but `_teleLinkLoop` never successfully connects. TCP+SSL to AHA works (verified), but the telepath protocol handshake response is never read. Background `schedCoro(_reinit_aha())` is a workaround but doesn't reliably connect.

2. **Mirror pod (1b) crashes**: When 1b starts as mirror, it needs to connect to 1a's nexus log for replication. This outbound telepath connection has the same issue as AHA — the writer's event loop doesn't process incoming data on outbound sockets.

3. **Root cause hypothesis**: After fork, outbound telepath connections created by `s_telepath.Client` have their `_rxLoop` (link message reader) task dead. The Client creates a new link, but the link's reader coroutine isn't scheduled on the new event loop. This affects ALL outbound telepath from the writer (AHA, mirrors, axon, jsonstor).

## Constraints

- The fix must work for ALL outbound telepath clients (not just AHA)
- Must not break the inbound path (router → workers → serve queries)
- Must handle the case where AHA isn't available at startup (retry)
- Must handle pod restarts gracefully (no permanent state corruption)
- The writer's event loop must be able to both serve (inbound) and connect (outbound)

## Architecture Context

```
                    ┌─────────────────────────────────────────┐
                    │              AHA Service                  │
                    │  (service discovery, leader election)     │
                    └──────────┬────────────────┬──────────────┘
                               │                │
                    outbound    │                │    outbound
                    telepath    │                │    telepath
                               ▼                ▼
              ┌────────────────────┐  ┌────────────────────┐
              │  cortex-1a (leader) │  │  cortex-1b (mirror) │
              │  ┌──────────────┐  │  │  ┌──────────────┐  │
              │  │ Writer       │  │  │  │ Writer       │  │
              │  │ (nexus push) │──┼──┼──│ (nexus pull) │  │
              │  └──────────────┘  │  │  └──────────────┘  │
              │  ┌──────────────┐  │  │  ┌──────────────┐  │
              │  │ Router       │  │  │  │ Router       │  │
              │  │ Workers ×4   │  │  │  │ Workers ×4   │  │
              │  └──────────────┘  │  │  └──────────────┘  │
              └────────────────────┘  └────────────────────┘
                        ▲                        ▲
                        │                        │
                    ┌───┴────────────────────────┴───┐
                    │           Optic (UI)            │
                    │  connects via aha://cortex...   │
                    └────────────────────────────────┘
```

## Research Needed

1. How does `s_telepath.Client` create and manage its link? Where is `_rxLoop` scheduled?
2. After `asyncio.run()` destroys the init loop and `asyncio.run(_writer_serve)` creates a new one, why don't freshly-created Client objects work? (They should — they're created on the new loop)
3. Is there global state in `synapse.telepath` or `synapse.glob` that references the dead loop?
4. Does `synapse.glob._glob_loop` need to be set before Client.anit() is called? (We set it in _writer_serve — check ordering)

## Plan (Phases)

### Phase 1: Fix outbound telepath from writer
- Debug why `Client.anit()` in the writer's new event loop doesn't connect
- Likely: `synapse.glob` loop reference, or certdir state, or link._rxLoop scheduling
- Test: writer connects to AHA within 60s of startup

### Phase 2: Mirror replication
- Ensure mirror (1b) can pull nexus log from leader (1a) after fork
- Test: write on 1a, verify it appears on 1b within 5s

### Phase 3: Leader failover
- Kill 1a, verify 1b promotes to leader via AHA
- Optic reconnects to 1b
- Test: Optic query works after failover

### Phase 4: Full cluster validation
- Both pods running with fork mode
- Optic connected and functional
- Run load test through Optic (HTTP API)
- Verify workers handle read load on both pods

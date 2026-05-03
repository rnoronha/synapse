# Pool Testing Design: Multiple Cortex Instances Behind a Load Balancer

## Context

Our test infrastructure runs a single Cortex instance on one EC2 host. Production Vertex
runs multiple Cortex instances across availability zones behind Kubernetes Services and an
NLB. This document designs how to test against a Cortex pool to catch failure modes that
only appear with multiple independent instances.

## 1. How Vertex Routes Queries in Production

Three routing layers, each serving a different purpose:

### Layer 1: AHA Service Discovery (Instance Registration)

Each Cortex pod registers with AHA (the Synapse service discovery cell) using per-AZ
AHA registries:

```
SYN_CORTEX_AHA_REGISTRY=["ssl://root@aha-us-east-1a.network", "ssl://root@aha-us-east-1b.network"]
```

Clients resolve `aha://cortex...` to discover available instances. The `?mirror=true`
parameter tells AHA to return a mirror (read-only follower) rather than the leader.

### Layer 2: Kubernetes Services (In-Cluster Routing)

From `cortex.ts`, three service tiers:

| Service | Selector | Purpose |
|---------|----------|---------|
| `cortex-{az}` | `component=cortex, availabilityZone={az}` | AZ-pinned access (per-AZ Ingress) |
| `cortex` (global) | `component=cortex` | Cross-AZ HTTPS via ALB Ingress |
| `cortex-internal-nlb` | `component=cortex, availabilityZone=us-east-1b` | **NLB on Telepath port 27492, pinned to one AZ** |

Key observation: the NLB is **not** a pool load balancer. Its selector pins to a single AZ
(`us-east-1b`). It provides a stable TCP endpoint for Telepath clients outside the cluster,
not round-robin across instances.

All Kubernetes Services use `sessionAffinity: ClientIP`, so a given client sticks to one
backend pod for the duration of its connection.

### Layer 3: Storm Pool (Application-Level Read Offloading)

The Cortex leader uses `cortex.storm.pool.set` to configure a pool URL (typically
`aha://pool00...`). When a read query arrives:

1. `_getMirrorProxy()` picks a pool member via `self.stormpool.proxy()`
2. Checks nexus offset delta: `curoffs - miroffs <= MAX_NEXUS_DELTA` (3,600 ops)
3. If in sync, offloads the query with `mirror=False` and `nexsoffs` for read-after-write
4. On timeout or sync lag, falls back to local execution

This is the **only** layer that does read/write splitting across independent Cortex
instances. The Kubernetes Services just route TCP connections; they don't understand
Storm query semantics.

### Layer 4: Local Query Router (Intra-Process Read Offloading)

Our `phase2-multi-process` branch adds `QueryRouter` — a local read/write splitter that
routes read queries to subprocess readers on the same host. This is orthogonal to the
Storm Pool (which routes across hosts).

**Summary of routing:**

```
Client → NLB/K8s Service → Cortex Leader
                              ├── write → execute locally
                              └── read  → Storm Pool → Mirror instance (cross-host)
                                          └── fallback: local QueryRouter → subprocess readers
```

## 2. Current Test Assumptions

Every test script connects to a **single Telepath URL** on one EC2 host:

| Script | Connection Pattern | Pool-Safe? |
|--------|-------------------|------------|
| `test_correctness.py` | `--writer URL --reader URL` (same host, different ports) | ❌ Assumes writer/reader are colocated |
| `test_read_after_write.py` | Single URL | ⚠️ Works but measures intra-host staleness only |
| `test_throughput.py` | Single URL | ⚠️ Measures single-instance throughput |
| `test_soak.py` | Single URL | ⚠️ No cross-instance failure injection |
| `test_recovery.py` | Single URL + `--reader-ports` | ❌ Kills local subprocess PIDs |
| `test_mixed_load.py` | Single URL | ⚠️ Works but no cross-instance routing |
| `test_pathological.py` | Single URL | ✅ Query correctness, instance-agnostic |
| `test_parallel_reads.py` | Single URL | ⚠️ Parallelism is intra-host only |
| `test_local_multiprocess.py` | Boots its own Cortex | ❌ Single-host by design |

**Core gap:** No test exercises the Storm Pool path (`_getMirrorProxy` → nexus offset
check → remote execution → fallback). No test verifies behavior when a client's connection
is load-balanced across independent Cortex instances with separate LMDB stores.

## 3. New Failure Modes With a Pool

### 3.1 Split Brain / Inconsistent Reads

Each Cortex instance has its own LMDB. Mirrors replicate via nexus log, not shared storage.
A client writing to the leader and immediately reading from a mirror sees stale data until
the mirror catches up. The `nexsoffs` mechanism in `_getMirrorProxy` bounds this, but:

- **Unbounded staleness if nexus replication stalls** — mirror falls behind > 3,600 ops,
  gets skipped, but if ALL mirrors are behind, queries run locally (no error, just degraded)
- **Write-then-read across connections** — if a client opens two connections (one hits leader,
  one hits mirror), the read connection has no `nexsoffs` coordination

### 3.2 Failover Gaps

- **NLB health check is TCP on 27492** — a Cortex that accepts TCP but has a corrupted LMDB
  or is stuck in startup (the 36-hour startup probe!) still passes NLB health checks
- **K8s readiness probe runs `synapse.tools.healthcheck`** — this checks cell health, not
  data freshness. A mirror 10,000 ops behind is "healthy"
- **Connection draining on pod restart** — `sessionAffinity: ClientIP` means existing
  connections stick, but new connections may route to a restarting pod

### 3.3 Admission Control Under Pool Routing

The local `QueryRouter` has `_max_concurrent=10` and `_queue_depth=100`. But the Storm Pool
has no equivalent admission control — `_getMirrorProxy` just grabs a proxy and fires. Under
load, a mirror can be overwhelmed by offloaded queries from multiple leaders.

### 3.4 Nexus Offset Race

`_getMirrorProxy` reads `self.getNexsIndx()` and the mirror's `getNexsIndx()` non-atomically.
If the leader is actively writing, the delta check can pass but the mirror hasn't replicated
the specific write the client cares about. The `nexsoffs` parameter in `_getMirrorOpts`
mitigates this (mirror waits for that offset), but the timeout path falls back silently.

## 4. EC2 Pool Test Architecture

### Option A: Multiple EC2 Instances (Recommended)

Closest to production. Each instance runs one Cortex with its own EBS volume.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  EC2: Leader     │     │  EC2: Mirror-1   │     │  EC2: Mirror-2   │
│  Cortex :27492   │────▶│  Cortex :27492   │     │  Cortex :27492   │
│  EBS /mnt/data   │     │  EBS /mnt/data   │     │  EBS /mnt/data   │
│  (writer)        │     │  (follower)      │     │  (follower)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                        │                        │
        └────────────────────────┼────────────────────────┘
                                 │
                          NLB :27492 (TCP)
                                 │
                          ┌──────┴──────┐
                          │ Test Client  │
                          └─────────────┘
```

**Setup:**
1. Provision 3 EC2 instances (reuse `deploy-and-test.sh` provisioning)
2. Leader: standard Cortex with `storm:pool` configured to `tcp://mirror-1:27492/cortex,tcp://mirror-2:27492/cortex`
3. Mirrors: Cortex started with `--mirror tcp://leader:27492/cortex` (nexus follower mode)
4. NLB: create via AWS CLI, target group with all 3 instances on port 27492

**Why not shared EBS?** EBS volumes are AZ-locked and single-attach (except io2 multi-attach,
which is expensive and not how production works). Production uses independent LMDB stores
per instance with nexus replication. Shared storage would test a different architecture.

### Option B: Single EC2, Multiple Cortex Processes

Cheaper, faster to set up, but doesn't test network partitions or NLB behavior.

```
┌──────────────────────────────────────────┐
│  EC2 (c5.4xlarge)                        │
│                                          │
│  Cortex-Leader  :27492  /mnt/data-leader │
│  Cortex-Mirror1 :27502  /mnt/data-m1     │
│  Cortex-Mirror2 :27512  /mnt/data-m2     │
│                                          │
│  Test client connects to each directly   │
└──────────────────────────────────────────┘
```

**Tradeoff:** Catches data consistency and routing bugs but not network-level failures.
Good for CI; Option A for release validation.

### Recommendation

Start with Option B (fast iteration, no NLB setup). Graduate to Option A for pre-release
validation. The test scripts should accept a list of URLs so the same tests work for both.

## 5. Test Changes Required

### 5.1 New: `test_pool_consistency.py`

The highest-value new test. Exercises the Storm Pool read path that no current test covers.

**What it tests:**
- Write to leader, read from each mirror individually — verify convergence within
  `MAX_NEXUS_DELTA` (3,600 ops) and within a time bound
- Write burst to leader, immediately read from pool endpoint — measure staleness distribution
  (like `test_read_after_write.py` but cross-instance)
- Verify `nexsoffs` wait mechanism: write, capture nexus offset, read with `nexsoffs` param,
  verify the mirror blocks until caught up

**Interface:**
```
python3.11 scripts/test_pool_consistency.py \
    --leader tcp://leader:27492/cortex \
    --mirrors tcp://mirror1:27492/cortex,tcp://mirror2:27492/cortex \
    --output /tmp/pool-results.json
```

### 5.2 New: `test_pool_failover.py`

**What it tests:**
- Kill a mirror process, verify queries still succeed (routed to remaining mirror or local)
- Kill the leader, verify mirrors reject writes with appropriate error
- Restart a mirror, verify it catches up and rejoins the pool
- Simulate slow mirror (inject latency), verify `_getMirrorProxy` timeout and local fallback

**Interface:**
```
python3.11 scripts/test_pool_failover.py \
    --leader tcp://leader:27492/cortex \
    --mirrors tcp://mirror1:27492/cortex,tcp://mirror2:27492/cortex \
    --leader-host i-xxx --mirror-hosts i-yyy,i-zzz \
    --output /tmp/failover-results.json
```

### 5.3 Modify: `test_read_after_write.py`

Add `--mirrors` flag. When provided, each write-then-read cycle reads from a specific mirror
(round-robin) instead of the same connection. This measures cross-instance staleness vs
the current intra-host staleness.

### 5.4 Modify: `test_correctness.py`

Add `--mirrors` flag (comma-separated URLs). Run the 26 read queries against each mirror
independently and compare results. Currently compares writer vs one reader on the same host.

### 5.5 Modify: `test_soak.py`

Add `--pool-urls` flag. Distribute read connections across pool members round-robin. Track
per-mirror latency and error rates separately. Detect if one mirror falls behind.

### 5.6 Modify: `deploy-and-test.sh`

Add `--pool` mode that:
1. Provisions N instances (default 3)
2. Starts leader on instance 1, mirrors on instances 2..N
3. Configures `storm:pool` on the leader
4. Runs pool-aware tests
5. Cleans up all instances

## 6. Implementation Priority

| Priority | Item | Effort | Value |
|----------|------|--------|-------|
| P0 | `test_pool_consistency.py` | 1 day | Catches the #1 production risk (stale reads) |
| P0 | Option B setup (single EC2, 3 processes) | 0.5 day | Unblocks all pool testing |
| P1 | `test_pool_failover.py` | 1 day | Catches failover regressions |
| P1 | `--mirrors` flag on existing tests | 1 day | Reuses proven test logic |
| P2 | Option A setup (multi-EC2 + NLB) | 1 day | Production-fidelity validation |
| P2 | `deploy-and-test.sh --pool` mode | 0.5 day | Automation for pool tests |

## 7. Open Questions

1. **Do we need AHA for pool testing?** Production uses AHA for service discovery, but our
   tests use direct `tcp://` URLs. We could skip AHA and configure the Storm Pool with
   explicit URLs. This misses AHA-related bugs but dramatically simplifies the test setup.
   **Recommendation:** Skip AHA for now. The Storm Pool accepts direct URLs.

2. **Mirror startup time.** A fresh mirror must replay the full nexus log to catch up. With
   310K pre-seeded nodes, this could take minutes. Should we snapshot mirror volumes too, or
   accept the startup cost? **Recommendation:** For Option B (same host), start leader first,
   seed data, then start mirrors. For Option A, use EBS snapshots.

3. **How many mirrors?** Production runs one Cortex per AZ (typically 3 AZs). Two mirrors
   is the minimum to test round-robin and failover-with-remaining-capacity.
   **Recommendation:** 2 mirrors (3 total instances).

# Synapse Cortex Performance — Roadmap

## Completed Goals

### 1. Single-Process Optimizations (Tier 1-2)
- noatime mount, HugePages reclaim, vm.dirty_ratio tuning
- BUID_CACHE_SIZE 10k → 200k
- Structural deepcopy, negative cache
- **Result:** ~32% warm-cache improvement on beta

### 2. Multi-Process Fork Infrastructure (v3 Phase 1)
- Arbiter + ReadOnlyWorker modules
- Pre-fork LMDB cleanup, worker readonly re-open
- Write forwarding via Telepath-over-UDS
- EPOLLEXCLUSIVE connection distribution
- **Result:** 4.2x throughput on c5.4xlarge

### 3. Legacy Reader Removal (v3 Phase 2)
- QueryRouter, ReaderManager removed
- Fork mode is the only multi-process mode
- classify() consolidated in worker.py

### 4. AST-Based Query Classification (readonly:true try-forward)
- Regex fast-path for obvious writes, readonly=True execution with IsReadOnly fallback
- Full AST walk prototype validated (scripts/prototype_ast_classify.py)
- Eliminates false positives — Synapse runtime is the authoritative classifier
- **Result:** All 8 criteria MET

### 5. Thin Router — FD Passing (current goal, nearly complete)
- Dedicated router process accepts all TCP connections
- sendmsg/SCM_RIGHTS passes fds to workers
- Eliminates EPOLLEXCLUSIVE contention
- Phase C: socketpair RPC write channel (replaces telepath proxy forwarding)
- **Result:** 16.6x read QPS vs baseline (283 → 4,705), p50 207ms → 8.1ms
- **Status:** EC2 write channel fix landed, final validation running
- **DONE:** Renamed `multi:process:readers` config to `multi:process:core_pct` (percent of cores for the multi-process system, accounting for router + writer + workers). Old key accepted with deprecation warning.

## Upcoming Goals

### 6. Thick Router — Per-Query Dispatch
- Router owns all client telepath connections
- Demuxes telepath stream into individual queries
- Classifies each query and dispatches to worker or writer
- Workers become pure readers — zero write handling code
- Architecture supports scaling to multiple router processes (tunable router count) for horizontal dispatch scaling
- **Depends on:** Goal 5 complete

### 7. Port to Synapse 3.x
- Port fork infrastructure (arbiter, worker, router) to 3.x branch
- Handle Drive subsystem (spawned process — close in workers, not needed for reads)
- Adapt to 3.x cell startup sequence (no startmain exists)
- Fewer slabs, no lockmemory thread — simpler than 2.x
- **Estimated effort:** ~2-3 weeks
- **Depends on:** Goal 5 complete (port stable 2.x code)

### 8. Production Deployment
- Deploy to gamma with monitoring
- Validate at 97.8M nodes / 4,091 layers / 96 cores
- Tune worker count for r7a.24xlarge
- Connection TTL for rebalancing
- Worker-level metrics and observability
- **Depends on:** Goal 7 (3.x port)

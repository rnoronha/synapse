# Goal: v3 Fork Architecture — Production-Ready Multi-Process Cortex

## End State
The Synapse Cortex uses a post-init fork architecture where N worker processes inherit the listening socket and serve read queries directly, bypassing the writer's event loop. Writes are forwarded to the writer via UDS. The architecture passes all 8 test scripts with <1% error rate under sustained load.

## Acceptance Criteria
1. Parallel-reads: >1400 QPS on c5.4xlarge (16 cores, 8 workers) — MET (1465 QPS)
2. Throughput speedup: >4x concurrent vs sequential — MET (4.68x)
3. Mixed-load: >3000 ops/s with 10% writes, 0 errors — MET (3370 ops/s)
4. Correctness: 26/26 read queries identical to single-process — MET
5. Soak: <1% error rate under 100 read + 30 write TPS for 10 minutes — NOT MET (50.8% → fix deployed, verifying)
6. Write forwarding: 100% RAW consistency at ≤10 ops/s, degrades at 50 ops/s (writer saturation) — PARTIALLY MET
7. Recovery: kill worker → failover → respawn, kill all → writer fallback → respawn all — MET (10/10)
8. Write handling: writes forwarded to writer (not executed on workers) — MET (v3 forwards writes correctly)

## Constraints
- Python 3.11 (no EPOLLEXCLUSIVE — use fallback)
- Single telepath URL (clients don't change)
- Must pass local test_v3_fork.py before EC2 deployment

## Out of Scope
- v2 QueryRouter/ReaderManager removal (Phase 2, after v3 validated)
- AST-based query classification (separate bead)
- Production CDK deployment changes

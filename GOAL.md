# Goal: Thick Router — Per-Query Dispatch to Workers/Writer

## End State
A thick router process owns all client telepath connections, handles handshakes and sessions, demuxes the telepath stream into individual queries, classifies each query, and dispatches reads to workers and writes to the writer via socketpair RPC. Workers are stateless pure readers — they receive query tasks, execute against readonly LMDB, and stream results back. Zero write-handling code in workers.

## Acceptance Criteria
1. Router handles telepath handshake and maintains sessions
2. All queries dispatched via socketpair RPC (reads to workers, writes to writer)
3. Workers have zero write-handling code
4. Streaming results relay correctly (no buffering, backpressure works)
5. Client cancellation propagates to workers (cooperative, best-effort)
6. No throughput regression: >1400 QPS parallel reads
7. Read p50 increase <10% vs thin router baseline (27ms)
8. Write latency no regression vs current (p50 <200ms)
9. All existing tests pass (parallel-reads, soak, mixed-load, read-after-write, pathological)
10. IsReadOnly re-dispatch works: misclassified writes transparently re-routed to writer
11. Share-returning methods routed to writer with explicit error if attempted on worker
12. Router respawn completes in <500ms; queued connections served without client error
13. Router memory stable under 1000 idle connections

## Constraints
- Python 3.11
- Single client-facing URL (port 27492)
- Reuse Daemon class for telepath protocol handling
- Socketpair + msgpack RPC for dispatch (proven pattern from write channel)
- Single router process (multi-router via SO_REUSEPORT only if needed)

## Design
Full design doc: .kiro/analysis/thick-router-design.md

## Revision History
- 2026-05-05 — Initial goal from design doc (reviewed by gpu-devils-advocate)

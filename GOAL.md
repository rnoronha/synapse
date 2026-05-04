# Goal: Thin Routing Layer — Separate Read/Write Paths

## End State
A dedicated router process accepts all client connections, classifies queries, and routes reads to fork workers and writes directly to the writer. Workers become pure readers with no write forwarding code. The writer is freed from read traffic and can be maxed out on writes.

## Acceptance Criteria
(To be defined after research — open questions below)

## Open Questions (must answer before finalizing criteria)
1. Separate process vs thread for the router?
2. FD passing (sendmsg/SCM_RIGHTS) vs stream proxy for routing connections?
3. How does this interact with Storm Pool mirror offloading (cortex.py stormpool)?
4. Can the router unify local fork routing with cross-host Storm Pool?
5. What's the write throughput ceiling with a dedicated writer (no read contention)?

## Constraints
- Python 3.11
- Single client-facing URL preserved
- Must not regress read throughput (<1400 QPS)
- Must not regress soak error rate (<1%)

## Out of Scope
- Changes to the telepath protocol
- External proxy software (HAProxy, nginx)
- Client-side routing

## Revision History
- 2026-05-04 — Initial goal (pre-research, acceptance criteria TBD)

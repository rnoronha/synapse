# Goal: Thin Router Process — FD Passing to Workers

## End State
A dedicated router process accepts all client TCP connections, then passes the file descriptor to the appropriate target via sendmsg/SCM_RIGHTS over per-worker UDS control channels. Workers receive the fd, complete the telepath handshake, and serve the full session. The writer process receives write-connection fds directly. No stream proxying — zero-copy connection handoff.

## Acceptance Criteria
1. Router process accepts connections on the public port (27492)
2. Router passes connection fds to workers via sendmsg/SCM_RIGHTS over UDS
3. Workers receive fds, complete telepath handshake, serve read queries locally
4. Write connections are passed to the writer process (not workers)
5. No throughput regression: >1400 QPS parallel reads on c5.4xlarge
6. No soak regression: <1% error rate under sustained load
7. Write throughput improvement: >50 write ops/s (currently ~10 via UDS proxy)
8. Workers have NO write forwarding code (pure readers)
9. Local test_v3_fork.py passes with router architecture

## Constraints
- Python 3.11
- Single client-facing URL (port 27492)
- sendmsg/SCM_RIGHTS requires Unix domain sockets (Linux only — fine for our deployment)
- Storm Pool remains independent (not merged into router)

## Out of Scope
- Cross-host routing (Storm Pool handles that)
- Client-side routing
- External proxy software

## Revision History
- 2026-05-04 — Initial goal after research (thin-router-research.md)

# Goal: Replace regex classify() with readonly:true try-forward

## End State
Workers execute ALL Storm queries with `readonly:true`. If the query succeeds, stream results to client. If it raises `IsReadOnly`, forward to the writer via UDS. The regex-based `classify()` function is removed from the critical path — Synapse's own readonly enforcement handles all edge cases.

## Acceptance Criteria
1. Workers set `readonly:true` in Storm opts for all incoming queries
2. Read queries execute locally on workers without hitting the writer
3. Write queries raise IsReadOnly on the worker, get forwarded to writer, succeed
4. Mixed queries (reads with write side-effects) are correctly caught and forwarded
5. No regression in read throughput (>1400 QPS on c5.4xlarge)
6. No regression in soak error rate (<1%)
7. The regex classify() is demoted to optional fast-path hint (not required for correctness)
8. Local test_v3_fork.py passes with readonly:true enforcement

## Constraints
- Must work with Python 3.11
- Must not change the telepath protocol (single URL preserved)
- Must handle all Storm syntax including lib functions, package commands, subqueries
- The regex fast-path is optional — if removed entirely, the system must still work

## Out of Scope
- AST walking/parsing (the whole point is to NOT reimplement write detection)
- Changes to Synapse's readonly enforcement itself
- v2 QueryRouter changes (v3 fork only)

## Revision History
- 2026-05-04 — Initial goal

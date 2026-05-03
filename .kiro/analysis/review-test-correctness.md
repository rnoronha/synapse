# Devil's Advocate Review: `scripts/test_correctness.py`

**Reviewer:** Kiro (automated)
**Date:** 2026-05-03
**Branch:** phase2-multi-process
**Verdict:** Test has structural flaws that allow false passes. Several "passing" tests assert nothing meaningful.

---

## 1. Does It Test What It Claims?

The docstring claims:
> Seeds test data through the writer, then runs 26 read queries against both writer and reader to verify result equivalence. Also verifies that 4 write operations are rejected by the reader with IsReadOnly.

### Claim-to-code mapping

| Claim | Backed by code? | Rating |
|---|---|---|
| 26 read queries run against both endpoints | Yes — `READ_QUERIES` has 26 entries, enforced by `assert len(READ_QUERIES) == 26` | OK |
| Result equivalence verified | Partially — `_compare()` checks `==` on sorted node lists, but see §3 | **HIGH** |
| 4 write operations rejected with IsReadOnly | 3 of 4 — `del_node` actually gets `CantDelNode` (confirmed in results JSON). The test correctly reports FAIL, but the docstring claim of "4 write operations" is aspirational, not achieved. | **MEDIUM** |

### Unbacked claims / silent vacuous passes

- **`prefix_host` and `prefix_host1`** — Both return 0 results on writer AND reader. The query `inet:fqdn:fqdn~=host` references a `:fqdn` secondary property that **does not exist** on `inet:fqdn`. The form's secondary properties are `:host`, `:domain`, `:zone`, `:iszone`, `:issuffix`. This query silently returns nothing. The test passes because `[] == []`. **These two tests assert nothing.** — **CRITICAL**

- **`zone_com` and `filter_zone_com`** — Both return 0 on both sides. In Synapse's data model, `host0.test.com` has `zone=test.com`, not `zone=com`. So `inet:fqdn:zone=com` correctly returns 0. But comparing 0 == 0 doesn't test read equivalence — it tests that both endpoints agree on "no results." This is a vacuous pass that would succeed even if the reader had no data at all. — **HIGH**

---

## 2. Edge Cases Missed

| Missing edge case | Risk | Rating |
|---|---|---|
| **Empty/no-data reader** — If the reader has zero data (replication broken), every query that returns 0 on the writer also returns 0 on the reader → PASS. There's no positive assertion that the reader actually has data. | A completely broken reader with no replicated data could pass 4+ of 26 tests. | **CRITICAL** |
| **Replication lag beyond 6s** — The hardcoded `await asyncio.sleep(6)` after seeding is the only sync mechanism. If replication takes 7s, all 26 comparisons run against stale data. No verification that replication is complete. | Flaky failures or, worse, false passes if partial replication happens to match on the tested queries. | **HIGH** |
| **Unicode / IDN domain names** — All test FQDNs are pure ASCII (`host0.test.com`). IDNA encoding is a known normalization path in `Fqdn._normPyStr`. No test covers whether the reader correctly handles IDNA-normalized FQDNs. | Misses encoding bugs in the read path. | **MEDIUM** |
| **Large result sets** — The broadest query returns 51 nodes. No test exercises pagination, streaming backpressure, or result sets in the hundreds/thousands. | Won't catch truncation or memory issues in the reader proxy. | **MEDIUM** |
| **Concurrent writes during reads** — All writes happen before reads. No test for read consistency when the writer is actively mutating data during reader queries. | Misses snapshot isolation bugs. | **MEDIUM** |
| **Connection failures / reconnection** — No test for what happens if the reader telepath connection drops mid-query. | Misses error propagation bugs. | **LOW** |
| **Special characters in values** — No URLs with query strings, fragments, encoded characters, or FQDNs with hyphens at boundaries. | Misses normalization edge cases. | **LOW** |

---

## 3. False Pass Scenarios

### 3a. Vacuous equality on empty results — CRITICAL

Four tests currently pass by comparing `[] == []` or `0 == 0`:
- `prefix_host` (0 == 0)
- `prefix_host1` (0 == 0)
- `zone_com` (0 == 0)
- `filter_zone_com` (0 == 0)

If the reader returned nothing for *every* query, these four would still pass. The test should assert a **minimum expected count** for each query to prevent vacuous passes.

### 3b. Count-only comparison for `print` mode queries — HIGH

For `count_fqdn`, `count_ipv4`, `count_url`, the test collects print messages and compares them. But `_collect_print` returns the raw message strings. The comparison `['Got 51 results.'] == ['Got 51 results.']` works, but:
- If the count format changes (e.g., `"51 nodes"` vs `"Got 51 results."`), both endpoints would produce the same new format → still passes. This is fine.
- But if both endpoints return an *error* print message instead of a count, the test would compare `['error: ...'] == ['error: ...']` → PASS. There's no validation that the print message actually contains a count.

### 3c. `lib_guid` shape-only check — MEDIUM

```python
if name == 'lib_guid':
    return len(writer_result) == 1 and len(reader_result) == 1
```

This only checks that both returned exactly one print message. If both return an error message, it passes. Should at minimum validate the returned string looks like a GUID (32 hex chars).

### 3d. Node identity comparison uses `mesg[1][0]` (ndef tuple) — OK but incomplete

`_collect_nodes` extracts `mesg[1][0]` which is the node's ndef (form, value) tuple. This is a reasonable identity check. However, it does **not** compare:
- Node properties (secondary properties like `:host`, `:domain`, `:zone`)
- Node tags
- Node data

A reader that returns the right nodes but with wrong/missing properties would pass all tests. — **HIGH**

### 3e. `_shutdown` event can silently skip tests — MEDIUM

If a signal arrives mid-run, the loop breaks early. The summary still computes `read_passed / read_total` based on whatever ran. If only 5 of 26 queries ran and all 5 passed, the report says `5/26 passed` and overall FAIL — but only because of the count mismatch, not because it detected a real bug. If the signal arrives after all read queries but before write rejection tests, `reject_total` is 0, so `0 == 0` → that section "passes."

---

## 4. Methodology

### 4a. No replication completion check — HIGH

The 6-second sleep is a guess. Sound methodology would poll the reader until it reports the same nexus offset as the writer, or at minimum retry queries with backoff until counts stabilize.

### 4b. Sequential execution masks timing bugs — MEDIUM

All 26 queries run sequentially. In production, concurrent queries are the norm. Sequential execution won't surface race conditions in the reader's query handling.

### 4c. No idempotency / cleanup — MEDIUM

The test seeds data but never cleans up. Running the test twice against the same Cortex doubles the dataset, changing expected counts. The `broad_fqdn` query uses `| limit 50` which masks this, but `zone_test_com` (which returns 51) would return 102 on a second run → FAIL. This makes the test non-idempotent.

### 4d. Single-process mode is a no-op — LOW

In single-process mode, every query is `[RECORDED]` with `'pass': True` hardcoded. The overall result is always PASS. This mode doesn't test anything — it's a data collection tool masquerading as a test.

---

## 5. Missing Tests

| What's missing | Why it matters | Rating |
|---|---|---|
| **Property equivalence** — Compare secondary properties (`:host`, `:domain`, `:zone`, `:iszone`) between writer and reader nodes | A reader that returns correct ndefs but wrong properties is a real bug this test can't catch | **HIGH** |
| **Tag equivalence** — Verify that tags on nodes match between writer and reader | Tags are a core Synapse feature; replication bugs could drop them | **HIGH** |
| **Write-then-read consistency** — Seed data, write more data, wait, verify the new data appears on reader | Only tests initial bulk seed, not incremental replication | **HIGH** |
| **Node deletion propagation** — Delete a node on writer, verify it disappears from reader | Only tests that reader rejects writes; never tests that writer deletes propagate | **HIGH** |
| **Edge/lightweight edge queries** — No `-(edge)>` or `<(edge)-` traversals | Edges are a distinct storage path that could have different replication behavior | **MEDIUM** |
| **`$node.data.*` reads** — No test reads node data from the reader | Node data is a separate storage subsystem | **MEDIUM** |
| **Error propagation** — What happens when a query errors on the reader? Does it return the same error as the writer? | Only happy-path queries tested | **MEDIUM** |
| **Auth/permissions** — No test that reader enforces the same permissions as writer | Could be a security gap if reader bypasses auth | **MEDIUM** |
| **Layer/view queries** — No multi-layer or view-specific queries | Multi-process split could have layer-routing bugs | **LOW** |

---

## Summary of Findings by Severity

| Rating | Count | Key issues |
|---|---|---|
| **CRITICAL** | 2 | Vacuous `[] == []` passes on broken queries (prefix_host, prefix_host1); empty reader would pass multiple tests |
| **HIGH** | 7 | No property/tag comparison; no replication completion check; no positive result count assertions; no write-then-read or delete propagation tests |
| **MEDIUM** | 8 | GUID shape-only check; non-idempotent; sequential-only; missing edge/data/auth coverage |
| **LOW** | 3 | No unicode FQDNs; no connection failure tests; single-process mode is a no-op |

### Top 3 Recommendations

1. **Fix the broken queries.** `inet:fqdn:fqdn~=host` is not a valid property path. Change to `inet:fqdn:host~=host` or `inet:fqdn~="host"`. Then add minimum expected count assertions to every query to prevent vacuous passes.

2. **Replace the 6s sleep with a replication sync check.** Poll the reader's nexus offset or use a sentinel node: write a unique node on the writer, then poll the reader until it appears.

3. **Compare full node representations, not just ndefs.** Extend `_collect_nodes` to capture properties and tags, then compare those too. A reader that returns the right node identities but wrong data is a real production bug.

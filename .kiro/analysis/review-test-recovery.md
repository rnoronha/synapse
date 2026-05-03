# Devil's Advocate Review: `scripts/test_recovery.py`

**Reviewer:** Kiro (automated)
**Date:** 2026-05-03
**Branch:** phase2-multi-process

---

## 1. DOES IT TEST WHAT IT CLAIMS?

The docstring claims: *"kills reader processes and verifies failover and respawn behavior."*

| Claimed Behavior | Step | Actually Asserted? | Verdict |
|---|---|---|---|
| Health check readers | Step 1 | Calls `getCellInfo()` — confirms connectivity, but **does not assert `active` is True** despite printing it. | ⚠️ Partial |
| Seed data | Step 2 | Runs 100 `[inet:fqdn=...]` mutations. **No assertion that 100 nodes actually exist** — just that the loop completed without exception. | ⚠️ Partial |
| Kill reader 1 | Step 3 | Sends SIGKILL and sleeps 0.5s. **Never verifies the process is actually dead** (no `_pid_from_port` recheck returning None). | ⚠️ Partial |
| Failover after kill | Step 4 | Checks `successes > 0` out of 10 reads. **Passes if even 1/10 succeeds** — a 90% failure rate is a PASS. | ⚠️ Weak |
| Reader respawn | Step 5 | Polls `getCellInfo()` — confirms the port is listening again. Sound. | ✅ |
| Respawned reader serves data | Step 6 | Runs `inet:fqdn | limit 10` and reports count. **No assertion on the count value** — returns 0 nodes and still passes. | ❌ No |
| Kill ALL readers | Step 7 | Same issue as step 3 — no death verification. | ⚠️ Partial |
| Writer fallback | Step 8 | Same weak threshold as step 4 — 1/10 is a pass. | ⚠️ Weak |
| All readers respawn | Step 9 | Polls all ports. Sound. | ✅ |
| Final health check | Step 10 | Same gap as step 1 — no `active` assertion. | ⚠️ Partial |

**Rating: HIGH** — Steps 4, 6, and 8 have assertions so weak they can pass when the system is broken.

---

## 2. EDGE CASES MISSED

### 2a. Single-process mode silently reports all-pass — CRITICAL

When `--reader-ports` is omitted, the test fabricates a report with **all 10 steps marked PASS** and `detail: 'N/A'`. This is not "skipped" — it's a synthetic green report. Any CI pipeline consuming the JSON output will see 10/10 PASS for a test that ran zero assertions. This actively hides the fact that recovery was never tested.

### 2b. Race between SIGKILL and port reuse — MEDIUM

After `os.kill(pid, SIGKILL)`, the test sleeps 0.5s then proceeds. There's no verification the process is dead. On a loaded system, the kernel may not have reaped the process yet, or worse, the `ReaderManager._health_loop` (10s interval) may have already respawned a new process on the same port before step 4 runs. The test would then be reading from a fresh reader, not testing failover at all.

### 2c. `_pid_from_port` is fragile — MEDIUM

- Relies on `ss` being available (not present on macOS, some minimal containers).
- Parses `ss` output with a regex that assumes `pid=(\d+)` appears on `LISTEN` lines. Different `ss` versions or kernel configurations may format this differently.
- If `ss` fails silently (returns 0 but no matching lines), `_pid_from_port` returns `None`, and step 3 raises `RuntimeError` — but this is conflated with "reader not running" rather than "tooling broken."

### 2d. No test with >2 readers — LOW

The round-robin logic in `QueryRouter.getReaderProxy()` has index arithmetic (`self._rr_index % len(self._healthy)`) that could have off-by-one bugs when the healthy list shrinks. The test only exercises whatever reader count the operator passes in, with no minimum enforced.

### 2e. No test of concurrent kill + query — HIGH

The test kills a reader, then issues reads sequentially. A real failure mode is a query *in flight* when the reader dies — the router must handle a mid-stream connection reset. This is never tested.

### 2f. No test of SIGTERM (graceful shutdown) — MEDIUM

The test only uses SIGKILL. The `ReaderManager._kill_proc` uses SIGTERM first, then escalates to SIGKILL. Graceful shutdown has different failure modes (half-closed connections, drain timeouts) that are untested.

---

## 3. FALSE PASS SCENARIOS

### 3a. Step 6 — respawned reader returns 0 nodes → PASS — CRITICAL

```python
async def verify_respawned():
    async with await s_telepath.openurl(killed_url) as prox:
        count = await _storm_count(prox, 'inet:fqdn | limit 10')
    return f'Respawned reader returned {count} nodes'
```

If the respawned reader has an empty slab (data corruption, wrong datadir, stale snapshot), `count == 0` and the step still passes. The return value is a string used as `step.detail`, not checked against any threshold. **This is the single most dangerous false-pass in the file.**

### 3b. Steps 4 & 8 — 1/10 success threshold — HIGH

```python
if successes == 0:
    raise RuntimeError('All 10 reads failed')
```

A system where failover works 10% of the time passes. For a recovery test, the threshold should be 10/10 (or at minimum 9/10 with jitter tolerance). As written, a router that fails to remove a dead reader from its healthy list — causing 9/10 requests to hit the dead reader — would pass.

### 3c. Single-process synthetic report — CRITICAL

Already covered in 2a. A CI system that checks `"passed" == "total"` in the JSON will see this as a clean run.

### 3d. Step 2 — seeding may partially fail — HIGH

The seed loop runs 100 individual Storm queries. If some fail silently (e.g., the router drops them, or a transient connection issue), fewer than 100 nodes exist. Since step 6 only checks `limit 10` with no minimum, and no step ever asserts `count == 100`, data loss during seeding is invisible.

### 3e. `run_step` catches all exceptions as FAIL — masks assertion errors — MEDIUM

```python
except Exception as exc:
    step.detail = str(exc)
```

If a step function has a programming bug (e.g., `TypeError`, `KeyError`), it's reported as a test FAIL with the exception message, not as a test *error*. The distinction matters: a FAIL means the system is broken, an ERROR means the test is broken. Conflating them means a broken test looks like a system failure, and operators may "fix" the wrong thing.

---

## 4. METHODOLOGY

### 4a. No isolation — HIGH

The test connects to a live, externally-managed Cortex. It seeds `inet:fqdn=recovery-*.test.com` nodes but never cleans them up. If run twice, the second run's step 2 is a no-op (nodes already exist), and step 6's count check (if it had one) would pass from stale data. There's no before/after diff to prove the respawned reader has *current* data.

### 4b. Timing assumptions are hardcoded — MEDIUM

- 0.5s sleep after SIGKILL (may not be enough under load)
- 30s respawn deadline (the `ReaderManager._health_loop` runs every 10s, so respawn can take 10s + spawn time; 30s is reasonable but not derived from the system's actual SLA)
- 5s telepath timeout (adequate for local, may fail on remote)

None of these are configurable or documented as to why they were chosen.

### 4c. No statistical rigor — LOW

The 10-read loops in steps 4 and 8 are too small to distinguish "flaky" from "broken." With 10 samples, a system that fails 20% of the time has a 10.7% chance of showing 10/10 success. Not a benchmark, but the sample size is too small for confidence even as a smoke test.

### 4d. Exit code semantics — LOW

`main()` returns `0` for all-pass, `1` for any failure, but also returns `None` (implicitly) on early `_shutdown` exits. `sys.exit(None)` is equivalent to `sys.exit(0)`, so a SIGINT during the test looks like success.

---

## 5. MISSING TESTS

| Missing Test | Why It Matters | Rating |
|---|---|---|
| **Data correctness after recovery** | Verify the respawned reader returns the *same* data as before the kill, not just *some* data. Query a specific node by value and check properties. | CRITICAL |
| **In-flight query during kill** | Start a long-running Storm query, kill the reader mid-stream, verify the router retries or returns a clean error. | HIGH |
| **Write routing during reader outage** | Verify that writes still go to the writer (not queued/rejected) when all readers are dead. The `QueryRouter.route()` falls back to local for writes, but this is never tested during a failure state. | HIGH |
| **Admission control under failure** | When readers die, the `QueryRouter._pending` counter and semaphores may leak (if `release()` is never called for in-flight queries). Test that `get_stats()` returns sane values after recovery. | HIGH |
| **Reader with stale data** | Kill a reader, write new data through the router, respawn the reader, verify the reader picks up the new data. This tests the readonly Cortex's ability to read from a shared slab that was written to while it was down. | HIGH |
| **Rapid kill-respawn cycling** | Kill and respawn a reader 5+ times in succession. Tests for resource leaks (file descriptors, slab locks, telepath connections) in the `ReaderManager`. | MEDIUM |
| **Partial reader fleet failure** | With 3+ readers, kill 1, verify load shifts to survivors (not just "some reads work"). Check that the round-robin skips the dead reader. | MEDIUM |
| **Graceful shutdown (SIGTERM)** | Test that a SIGTERM'd reader drains in-flight queries before exiting. | MEDIUM |
| **Health loop timing interaction** | Verify that the `ReaderManager._health_loop` (10s interval) actually detects and respawns a dead reader, rather than the test's polling creating a race. | MEDIUM |

---

## Summary

| Severity | Count | Key Issues |
|---|---|---|
| CRITICAL | 3 | Single-process synthetic pass; step 6 no data assertion; no data correctness test |
| HIGH | 6 | 1/10 success threshold; no in-flight kill test; seed count unverified; no isolation; admission control leaks; stale data |
| MEDIUM | 6 | Kill verification race; SIGTERM untested; hardcoded timings; `ss` fragility; error vs fail conflation; rapid cycling |
| LOW | 3 | Sample size; exit code on SIGINT; reader count minimum |

**Bottom line:** This test verifies that *something* responds on a port after a kill. It does not verify that the recovered system returns correct data, handles in-flight failures, or maintains admission control invariants. The single-process mode producing a synthetic all-pass report is a CI landmine. Steps 4, 6, and 8 have assertion thresholds so low they'd pass a system that's 90% broken.

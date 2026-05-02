# Design Review: Readonly Slab Transaction Refresh Mechanism

**Date:** 2026-05-02
**Branch:** phase2-multi-process
**Reviewer:** Kiro (AI)
**Files:** `synapse/lib/lmdbslab.py`

---

## 1. IN-FLIGHT QUERY SAFETY

**Can `_refresh_ro_xact` fire between two `cursor.iternext()` calls? What happens to in-flight scans? Is the bumped scan consistent?**

**Finding: Yes it can fire, and the mechanism is safe — but only because Synapse is single-threaded (asyncio).**

`_refresh_ro_xact` (L1036–1047) is called from `_ro_refresh_loop` (L1049–1054), which is an async coroutine scheduled via `schedCoro` (L975). It runs on the asyncio event loop thread. The scan iteration methods (`scanByPref`, `scanByRange`, etc.) are synchronous generators that do not `await` — so between two `cursor.iternext()` calls within a single generator step, the event loop cannot interleave `_refresh_ro_xact`.

However, **any `await` in the caller** (e.g., `await asyncio.sleep(0)` in `scanKeysByHierPref` at L1487, `countByPref` at L1524, or any async consumer of a scan generator) yields control back to the event loop. At that point, `_ro_refresh_loop` can fire and call `_refresh_ro_xact`.

When this happens:

1. `_refresh_ro_xact` calls `scan.bump()` (L1042) on every active scan, which closes the cursor and sets `self.bumped = True` (L1976–1979).
2. The transaction is aborted and a new one is begun (L1044–1047).
3. When the scan's `iternext()` resumes (L1955–1973), it detects `self.bumped`, opens a new cursor on the new transaction, and calls `resume()` (L1985–2001) to reposition using the saved `self.atitem`.

The bumped scan sees the **new MVCC snapshot**, not the old one. This means:
- New keys inserted by the writer between the old and new snapshot will appear in the scan.
- Keys deleted by the writer may be skipped (handled by `resume()` — see Q4).
- The scan is **not** a point-in-time consistent snapshot across the refresh boundary.

For a readonly reader process in a writer+readers architecture, this is acceptable: the reader is already seeing a delayed view, and refreshing to a newer snapshot is strictly better than holding a stale one that blocks page reclamation.

**Risk: LOW** — The mechanism is correct for the single-threaded asyncio model. The consistency trade-off (seeing a newer snapshot mid-scan) is inherent to the design and acceptable for the use case. The `assert s_glob.iAmLoop()` guard in `_finiCoXact` (L1108) provides a runtime check that this code only runs on the loop thread.

---

## 2. PERFORMANCE IMPACT

**N readers × M slabs × txn cycle every 5s. Measurable? Should period be longer?**

**Finding: Negligible overhead. 5s is a reasonable default.**

Each refresh cycle in `_refresh_ro_xact` (L1036–1047) performs:
1. `scan.bump()` for each active scan — closes cursors (cheap, no I/O).
2. `self.xact.abort()` — releases the read transaction's MVCC snapshot. This is a metadata-only operation in LMDB: it clears the reader slot in the shared lock file (a single cache-line-aligned write).
3. `self.lenv.begin(readonly=True)` — acquires a new reader slot and reads the current meta page. Again metadata-only, no data copying.

Cost estimate: each cycle is ~2 mutex operations + 1 meta page read ≈ **sub-microsecond** on modern hardware. For 4 readers × 10 slabs × 1 cycle/5s = 8 cycles/second total — completely unmeasurable against any real workload.

The 5-second period (L784, `SYN_SLAB_READONLY_REFRESH`) is configurable via environment variable. The trade-off:
- **Shorter period** → writer reclaims freed pages faster, less DB bloat, but more frequent scan bumps (each bump forces a cursor reposition on resume).
- **Longer period** → fewer scan interruptions, but the reader holds old snapshots longer, blocking page reclamation.

5 seconds is a good balance. The research doc confirms that long-lived read transactions are the #1 operational hazard for multi-process LMDB (Q3 in the research doc). A 5s ceiling on snapshot age is conservative and safe.

**One concern:** when no scans are active and `txnrefcount == 0`, the readonly slab has `self.xact = None` (L932–933). `_refresh_ro_xact` correctly short-circuits on `self.xact is None` (L1038). But when `txnrefcount > 0` (active scans), the refresh fires and bumps all scans. There is **no check of `txnrefcount`** in `_refresh_ro_xact` — it will abort and re-begin the transaction even if scans are active. This is by design (the bump/resume mechanism handles it), but it means every active scan pays the reposition cost every 5 seconds.

**Risk: NONE** — The overhead is negligible. The 5s default is well-chosen.

---

## 3. MAP_SIZE RESIZE

**Is there a window between abort and begin where the mmap is invalid?**

**Finding: There is a window, but it is safe due to single-threaded execution and `_initCoXact`'s `MapResizedError` handling.**

In `_refresh_ro_xact` (L1044–1047):
```python
self.xact.abort()
del self.xact
self.xact = None
self._initCoXact()
```

Between `abort()` and `_initCoXact()`, `self.xact` is `None`. If the writer process grew the map between the old and new transaction, the mmap region referenced by the old transaction is potentially remapped. However:

1. All scan cursors were already closed by `bump()` (L1042) before `abort()`. No code holds references to cursor data across this boundary.
2. `_initCoXact` (L1658–1667) handles `MapResizedError` by calling `self.lenv.set_mapsize(0)` to re-read the writer's current map size, then retries `begin()`. This is the correct pattern per LMDB documentation and the research doc (Q2 table: "Readers call `set_mapsize(0)` to re-read current size").
3. The `self.xact = None` assignment (L1046) ensures that if any code somehow ran between abort and begin (impossible in single-threaded asyncio, but defensive), it would see `None` rather than a stale transaction object.

The mmap itself is managed by the LMDB C library. When `set_mapsize(0)` is called, py-lmdb/LMDB remaps the file with the new size. The old mmap region is unmapped. Since all cursors and transaction references were released before this point, there are no dangling pointers.

**Risk: NONE** — The single-threaded model eliminates the window, and `_initCoXact` handles the resize case correctly.

---

## 4. SCAN BUMP CORRECTNESS

**What if the saved key was deleted? What about dupsort databases?**

**Finding: Handled correctly for both cases, with one subtle behavioral note for dupsort.**

When a scan is bumped and resumes, `Scan.resume()` (L1985–2001) repositions the cursor using `self.atitem` (the last yielded key/value pair):

**Non-dupsort case (L1988–1989):**
```python
if not self.dupsort:
    return self.curs.set_range(item[0])
```
`set_range` positions at the first key >= the saved key. If the saved key was deleted, the cursor lands on the next key. The scan continues from there — no data is lost, no duplicate yields. This is correct: the scan skips over the gap left by the deletion and continues forward.

**Dupsort case (L1991–2001):**
```python
if self.curs.set_range_dup(*item):
    return True

if not self.curs.set_range(item[0]):
    return False

if self.curs.key() == item[0]:
    return self.curs.next_nodup()

return True
```

Three sub-cases:
1. **Exact (key, value) still exists or a later dup exists:** `set_range_dup` succeeds, cursor is positioned correctly.
2. **Key still exists but the specific dup value was deleted and no later dup exists:** `set_range_dup` fails, `set_range` finds the same key, `curs.key() == item[0]` is true, so `next_nodup()` advances to the next key. Correct — we've exhausted dups for this key.
3. **Key was entirely deleted:** `set_range_dup` fails, `set_range` lands on the next key (different from `item[0]`), returns True. Correct — scan continues from the next key.

After `resume()`, `iternext()` calls `isatitem()` (L1969) to check if the cursor landed exactly on the previously-yielded item. If so, it calls `next(self.genr)` to skip it (avoiding a duplicate yield). For non-dupsort, `isatitem` compares keys only (L2005–2006); for dupsort, it compares both key and value (L2007). This is correct.

**Subtle behavioral note:** In the dupsort case, if the saved key's dup was deleted but *earlier* dups for the same key still exist, `set_range_dup` will land on a dup *after* the deleted one (or fail). It will never go backwards to yield earlier dups that were already yielded. However, if new dups were *inserted* between the old and new snapshot with values less than the saved value, those new dups will be skipped. This is acceptable — the scan is moving forward through the new snapshot.

**Risk: LOW** — The resume logic is well-designed and handles deletion correctly for both dupsort and non-dupsort databases. The only edge case (skipping newly-inserted earlier dups in dupsort) is inherent to forward-only scan semantics and not a bug.

---

## 5. INTERACTION WITH reader_check()

**Can `reader_check()` clear a slot during refresh?**

**Finding: No race condition exists. `reader_check()` runs in the writer process; `_refresh_ro_xact` runs in the reader process.**

`reader_check()` is called in the writer Cortex process via `_lmdbReaderCheck` (cortex.py L1734–1741), running every 60 seconds in `_lmdbReaderCheckLoop` (cortex.py L1743–1747). It scans the LMDB reader table (in the shared lock file) and clears slots whose owning PIDs no longer exist.

`_refresh_ro_xact` runs in a reader process. The sequence is:
1. Reader calls `self.xact.abort()` — this releases the reader's slot in the shared lock file.
2. Reader calls `self._initCoXact()` → `self.lenv.begin(readonly=True)` — this acquires a new slot.

Between steps 1 and 2, the reader has **no active slot**. If `reader_check()` runs at this exact moment, it has nothing to clear for this reader — the slot is already released. This is safe.

The dangerous scenario would be: `reader_check()` clearing a slot that a reader is actively using. This can only happen if the reader process has **crashed** (PID no longer exists). A live reader process's PID will always be found by `reader_check()`, so its slots are never cleared.

There is also no risk of `reader_check()` interfering with the `begin()` call. LMDB's reader table uses per-slot atomic operations. `reader_check()` only clears slots owned by dead PIDs. The new `begin()` acquires a fresh slot atomically.

**Risk: NONE** — The writer's `reader_check()` and the reader's `_refresh_ro_xact` operate on different processes' slots with no interference.

---

## 6. txn_reset/txn_renew vs abort/begin

**Performance difference? Correctness difference?**

**Finding: `abort/begin` is used. `txn_reset/txn_renew` would be marginally faster but the difference is negligible. The current approach is correct.**

The current implementation uses `self.xact.abort()` + `self.lenv.begin()` (L1044–1047). The alternative would be `self.xact.reset()` + `self.xact.renew()`.

**Performance difference:**
- `abort/begin`: Frees the transaction handle, then allocates a new one. Each `begin()` allocates a small C struct (~few hundred bytes) and acquires a reader slot.
- `reset/renew`: Keeps the transaction handle allocated but releases the MVCC snapshot (`reset`), then re-acquires a fresh snapshot (`renew`). Avoids the malloc/free cycle.

Per the LMDB documentation and the research doc (Q3 table): `reset/renew` is the "pooled txn" pattern, recommended for high-frequency reads. The savings are one malloc + one free per cycle. At 1 cycle per 5 seconds, this saves ~nanoseconds — completely irrelevant.

**Correctness difference:**
Both approaches release the MVCC snapshot equally. The reader slot behavior differs slightly:
- `abort`: Releases the reader slot entirely. A new slot is acquired on `begin`.
- `reset`: Keeps the reader slot allocated but marks it as inactive (txnid = 0). `renew` reactivates it.

With `reset/renew`, the reader slot is never fully released, which means `reader_check()` would see the slot as belonging to a live PID even between `reset` and `renew`. With `abort/begin`, the slot is released and re-acquired. Both are correct.

**One practical consideration:** `abort/begin` is simpler and matches the pattern used in `_handle_mapfull` (L1687–1689) and `_finiCoXact` (L1103–1122). Using the same pattern everywhere reduces cognitive load and the chance of bugs. The `_initCoXact` method also handles `MapResizedError` (L1660–1665), which would need to be replicated in a `renew`-based approach.

**Risk: NONE** — `abort/begin` is correct and the performance difference is immeasurable at the 5s refresh interval. Switching to `reset/renew` would add complexity for no practical benefit.

---

## Summary Table

| # | Question | Risk | Key Finding |
|---|----------|------|-------------|
| 1 | In-flight query safety | **Low** | Safe under single-threaded asyncio. Scans see a newer snapshot after bump — acceptable trade-off. Any `await` in a scan consumer can trigger a refresh. |
| 2 | Performance impact | **None** | Sub-microsecond per cycle. 5s default is well-chosen. Configurable via `SYN_SLAB_READONLY_REFRESH`. |
| 3 | MAP_SIZE resize window | **None** | Single-threaded execution eliminates the window. `_initCoXact` handles `MapResizedError` correctly. |
| 4 | Scan bump correctness | **Low** | `resume()` handles deleted keys correctly for both dupsort and non-dupsort. Newly-inserted earlier dups in dupsort may be skipped (acceptable). |
| 5 | reader_check() interaction | **None** | No race. Writer clears dead-PID slots only. Reader's abort/begin is atomic from the writer's perspective. |
| 6 | txn_reset/renew vs abort/begin | **None** | abort/begin is correct, simpler, and consistent with the rest of the codebase. Performance difference is negligible at 5s intervals. |

## Recommendations

1. **No changes required.** The mechanism is well-designed and correct for the single-threaded asyncio execution model.

2. **Document the consistency model.** Add a docstring to `_refresh_ro_xact` noting that bumped scans see a newer MVCC snapshot, not the original one. This is a design choice that future maintainers should understand.

3. **Consider a guard against multi-threaded misuse.** If Synapse ever introduces thread-based parallelism for reads, the bump/resume mechanism would have race conditions (e.g., `self.bumped` read/write without locks, `self.xact` replacement while another thread uses it). An `assert s_glob.iAmLoop()` in `_refresh_ro_xact` (matching the one in `_finiCoXact` at L1108) would make this invariant explicit.

4. **Monitor scan bump frequency in production.** If scans are frequently bumped (visible via logging or metrics), it could indicate that the 5s period is too aggressive for workloads with many long-running scans. The `SYN_SLAB_READONLY_REFRESH` env var provides an escape hatch.

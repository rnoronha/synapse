# Phase 2 Removal Plan — QueryRouter & ReaderManager

> Generated 2026-05-03. Branch: `phase2-multi-process`.

## Summary

v3 Phase 2 replaces the old `QueryRouter` + `ReaderManager` subprocess infrastructure
with fork-mode (`_initForkMode`). The old path (`_initQueryRouter`) spawns standalone
reader cortex processes via `subprocess.Popen` and routes queries over Telepath. Fork-mode
replaces all of this with `os.fork()` workers that share the parent's LMDB mappings.

The `classify()` function (read vs write classification) was already copied to `worker.py`
and is the sole copy going forward.

---

## File-by-file classification

### REMOVE — dead code after v3

| File | Lines | What |
|------|-------|------|
| `synapse/lib/queryrouter.py` | entire file | Old routing + admission control. `classify()` already lives in `worker.py`. Delete the whole module. |
| `synapse/lib/readermanager.py` | entire file | Subprocess-based reader spawning/health-check. Replaced by fork-mode workers. Delete the whole module. |
| `synapse/cortex.py:53` | `import synapse.lib.queryrouter as s_queryrouter` | Dead import. |
| `synapse/cortex.py:54` | `import synapse.lib.readermanager as s_readermanager` | Dead import. |
| `synapse/cortex.py:988` | `self.queryrouter = None` | Dead attribute init. |
| `synapse/cortex.py:989` | `self.readermgr = None` | Dead attribute init. |
| `synapse/cortex.py:1773` | `await self._initQueryRouter()` (else branch) | Dead call — see UPDATE for the `if/else` block. |
| `synapse/cortex.py:1851-1875` | `async def _initQueryRouter(self):` | Entire method: spawns `ReaderManager`, creates `QueryRouter`, registers fini callbacks. Remove. |
| `synapse/cortex.py:6444-6454` | `if self.queryrouter is not None:` block in `storm()` | Old reader-proxy routing in `storm()`. Remove the entire `if` block (lines 6444–6454). |
| `synapse/cortex.py:6490-6498` | `if self.queryrouter is not None:` block in `callStorm()` | Same pattern in `callStorm()`. Remove the entire `if` block (lines 6490–6498). |
| `scripts/test_reader_manager.py` | entire file | Test script for `ReaderManager`. No longer applicable. |
| `scripts/test_query_router.py` | entire file | Tests for `queryrouter.classify()` and routing. Classification tests should be migrated to test `worker.classify()` (see UPDATE). |
| `scripts/test_query_queue.py` | entire file | Tests for `QueryRouter` admission control. No equivalent in fork-mode. |

### KEEP — still needed

| File | Lines | What |
|------|-------|------|
| `synapse/lib/worker.py:26-27` | Comment: `# Query classification (copied from queryrouter.py …)` | The `classify()` function and all supporting constants (`_write_commands`, `_re_edit_bracket`, `_re_write_cmd`, `_re_write_patterns`, `_strip_comments`) in `worker.py` are the canonical copy going forward. **Keep.** |

### UPDATE — needs modification

| File | Lines | What to change |
|------|-------|----------------|
| `synapse/cortex.py:1769-1773` | `if pct and not self.readonly: … else: await self._initQueryRouter()` | Remove the `else` branch entirely. When `pct` is 0 or `readonly`, no multi-process init is needed — just skip. The block becomes: `if pct and not self.readonly: await self._initForkMode(pct)` |
| `synapse/cortex.py:914-921` | Config schema `'multi:process:readers'` | **Rename to `multi:process:workers`** (or similar) to reflect fork-mode semantics. The key currently says "readers" which implies the old subprocess model. Coordinate with `cell.py:4661`, deploy scripts, and `test_local_multiprocess.py`. |
| `synapse/lib/cell.py:4661` | Comment: `Only Cortex with multi:process:readers > 0 uses it.` | Update comment to match renamed config key. |
| `synapse/lib/worker.py:26-27` | Comment: `# copied from queryrouter.py — will be sole copy after Phase 2 removes queryrouter.py` | Update comment — Phase 2 is done, this *is* the sole copy. Remove the forward-looking language. |
| `scripts/test_local_multiprocess.py:44` | `'multi:process:readers': 50,` | Update config key if renamed. |
| `scripts/test_local_multiprocess.py:95` | `print('  SKIP — no ReaderManager …')` | Update message to reference fork-mode workers, not `ReaderManager`. |
| `scripts/start-mp-cortex.sh:6,64,87` | References to `multi:process:readers` | Update config key if renamed. |
| `scripts/deploy-and-test.sh:337` | `multi:process:readers` in cell.yaml generation | Update config key if renamed. |
| `scripts/run-prod-equiv-matrix.sh:185` | `multi:process:readers` in cell.yaml generation | Update config key if renamed. |
| `scripts/test_query_router.py` | entire file | Migrate `classify()` test cases to target `synapse.lib.worker.classify` before deleting. |

---

## Execution order

1. **Migrate tests** — port `test_query_router.py` classify tests to `worker.classify`.
2. **Remove modules** — delete `queryrouter.py` and `readermanager.py`.
3. **Clean cortex.py** — remove imports, attribute inits, `_initQueryRouter`, routing blocks in `storm()`/`callStorm()`, and the `else` branch.
4. **Rename config key** (optional, can defer) — `multi:process:readers` → `multi:process:workers` across all files.
5. **Update comments/messages** — `worker.py` comment, `cell.py` comment, `test_local_multiprocess.py` skip message.
6. **Delete dead test scripts** — `test_reader_manager.py`, `test_query_router.py`, `test_query_queue.py`.

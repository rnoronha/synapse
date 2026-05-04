# AST-Based Storm Query Classifier — Design Analysis

## Summary

Replace the regex-based `classify()` in `worker.py` and `queryrouter.py` with an
AST-based classifier that parses Storm text via `synapse.lib.parser.parseQuery()`
and walks the tree for write-operation nodes.

**Verdict:** The AST approach is strictly more correct than regex for detecting edit
operations, but still requires a static command list for `CmdOper` nodes. The hybrid
approach (AST for edits + command list for commands) eliminates all regex edge-case
risks while keeping the `IsReadOnly` fallback as a safety net.

---

## 1. Complete List of Write AST Node Types

### Edit nodes (subclasses of `ast.Edit`)

Every `Edit` subclass calls `self.reqNotReadOnly(runt)` at the start of its `run()` method.

| AST Class | Storm Syntax | Line in ast.py |
|---|---|---|
| `EditParens` | `[ <nodeadd> <edits...> ]` (edit bracket wrapper) | 4047 |
| `EditNodeAdd` | `[inet:ipv4=1.2.3.4]` | 4096 |
| `EditCondPropSet` | `[:prop?=value]` (conditional set) | 4232 |
| `EditPropSet` | `[:prop=value]` | 4277 |
| `EditPropSetMulti` | `[:prop=val :prop2=val2]` (multi-prop) | 4370 |
| `EditPropDel` | `[-:prop]` | 4447 |
| `EditUnivDel` | `[-.univprop]` | 4473 |
| `EditEdgeAdd` | `[+(verb)> { ... }]` | 4647 |
| `EditEdgeDel` | `[-(verb)> { ... }]` | 4726 |
| `EditTagAdd` | `[+#tag]` | 4805 |
| `EditTagDel` | `[-#tag]` | 4866 |
| `EditTagPropSet` | `[+#tag:prop=val]` | 4888 |
| `EditTagPropDel` | `[-#tag:prop]` | 4922 |

### Other write enforcement points (runtime-only)

These cannot be detected by pure AST walking — they check `runt.readonly` at execution time:

| Check Location | What It Guards | Line |
|---|---|---|
| `CmdOper.run()` | Commands not marked `readonly = True` | 1300 |
| `FuncCall` (unnamed class) | Lib functions without `_storm_readonly` | 3616 |
| `SetItemOper.run()` | `$obj.prop = val` without `_storm_readonly` | 1390, 1412 |
| `Lookup.run()` | `autoadd=True` mode (lookup with auto-create) | 254 |

### Write commands (for CmdOper static classification)

These 34 commands are not marked `readonly = True` in their class definitions:

```
auth.user.add, auth.user.del, auth.role.add, auth.role.del,
auth.user.grant, auth.user.revoke, auth.user.addrule, auth.user.delrule,
auth.role.addrule, auth.role.delrule,
cron.add, cron.del, cron.mod, cron.move, cron.enable, cron.disable, cron.cleanup,
delnode, dmon.add, dmon.del, feed.ingest,
graph.add, graph.del,
layer.add, layer.del, layer.set, layer.pull.add, layer.push.add,
macro.set, macro.del, merge,
model.edge.set, model.edge.del, model.depr.lock, model.depr.unlock,
movetag, pkg.load, pkg.del,
queue.add, queue.del,
service.add, service.del,
trigger.add, trigger.del, trigger.mod, trigger.enable, trigger.disable,
view.add, view.del, view.set, view.merge
```

---

## 2. Performance

Benchmark: 1000 parses (10 representative queries × 100 iterations), warm cache.

| Metric | Regex | AST Parse + Walk |
|---|---|---|
| avg per query | ~1 µs | ~100 µs |
| ratio | 1× | ~100× |

**Assessment:** 100µs is negligible. A Storm query that touches LMDB takes milliseconds
to seconds. The classify step is <0.01% of total query time. Parse cost is not a concern.

### Can we reuse the execution parse?

Yes. The Cortex already parses every query via `cortex.getStormQuery()` → `querycache`
→ `parseQuery()`. In the worker, we could:

1. Parse once via `parseQuery(text)` for classification
2. Pass the parsed AST to the execution path (avoiding double-parse)

However, the Cortex's `querycache` already caches parsed ASTs (size=10,000), so
repeated queries hit cache. The 100µs cost only applies to cache misses.

---

## 3. Accuracy Comparison

50 test queries (31 original + 19 edge cases): **both classifiers agree on all 50**.

The regex classifier is currently correct on all tested queries. However, the AST
approach is structurally more robust against:

| Edge Case | Regex Risk | AST Handling |
|---|---|---|
| `[` inside string literals | Could false-positive | Parser handles string escaping |
| Nested subqueries with edits | Regex sees brackets | AST walks into SubQuery children |
| Comments containing `[` | Regex strips `//` comments only | Parser ignores all comments |
| Multi-line queries with mixed syntax | Regex is line-unaware | AST is structural |
| New edit syntax in future Synapse | Requires regex update | New `Edit` subclass auto-detected |
| `$x[0]` variable indexing | Regex has negative lookbehind | Not valid Storm (uses `$x.index(0)`) |

**Key advantage:** When Synapse adds new `Edit*` subclasses, the AST classifier
automatically detects them. The regex classifier requires manual updates.

### What AST cannot catch statically

- **Non-readonly lib functions:** `$lib.queue.myq.put(...)` — the `_storm_readonly`
  attribute is on the Python function object, not in the AST.
- **Non-readonly commands from packages:** Custom Storm packages can register commands.
  The AST sees `CmdOper` but doesn't know if the command is readonly without the registry.
- **SetItemOper writes:** `$node.data.set(...)` — depends on the runtime type's
  `_storm_readonly` attribute.

These are all caught by the `IsReadOnly` exception fallback in `worker.storm()`.

---

## 4. Recommended Classifier Design

```python
from synapse.lib.parser import parseQuery
from synapse.lib.ast import Edit, CmdOper

_write_commands = frozenset({...})  # same 34 commands

def classify(text):
    try:
        query = parseQuery(text)
    except Exception:
        return 'write'  # parse failure → assume write (safe default)
    return _walk(query)

def _walk(node):
    if isinstance(node, Edit):
        return 'write'
    if isinstance(node, CmdOper):
        if node.kids[0].value() in _write_commands:
            return 'write'
    for kid in node.kids:
        if _walk(kid) == 'write':
            return 'write'
    return 'read'
```

**Design decisions:**
- Parse failure → classify as `write`. This is the safe default: writes go to the
  writer process, which can handle anything. A false-positive write classification
  costs one IPC round-trip; a false-negative causes an `IsReadOnly` retry.
- The `IsReadOnly` catch in `worker.storm()` remains as a safety net for the cases
  AST cannot detect statically (lib functions, package commands, SetItemOper).
- The `_write_commands` set is still needed for `CmdOper` classification, but the
  regex for edit brackets (`_re_edit_bracket`) is eliminated entirely.

---

## 5. Integration Plan

### Where to hook in `worker.py`

Replace the current `classify()` function (line 67) in `worker.py`:

```python
# Before (regex):
def classify(text):
    stripped = _re_comment.sub('', text)
    if _re_edit_bracket.search(stripped): return 'write'
    if _re_write_cmd.search(stripped): return 'write'
    if _re_write_patterns.search(stripped): return 'write'
    return 'read'

# After (AST):
def classify(text):
    try:
        query = parseQuery(text)
    except Exception:
        return 'write'
    return _walk(query)
```

### What to remove
- `_re_edit_bracket` regex
- `_re_write_patterns` regex (the `$node.data.set` / `->>` patterns)
- `_re_comment` regex
- `_re_write_cmd` regex (replaced by CmdOper check in `_walk`)
- The duplicate `classify()` in `queryrouter.py` (import from worker or shared module)

### What to keep
- `_write_commands` frozenset (still needed for CmdOper classification)
- `IsReadOnly` catch in `worker.storm()` (safety net for runtime-only checks)

### Migration path
1. Add `classify_ast()` alongside existing `classify()` in worker.py
2. Log mismatches between regex and AST in production for 1 release cycle
3. Switch to AST-only after confirming zero mismatches
4. Remove regex code and `queryrouter.py` duplicate

---

## 6. Prototype

See `scripts/prototype_ast_classify.py` — 50 test queries, timing, mismatch detection.

Run: `PYTHONPATH=. python3 scripts/prototype_ast_classify.py`

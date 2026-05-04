# Cycle Log — readonly:true Try-Forward Classifier

## Cycle 1 — 2026-05-04T05:41Z

### Orient
- Goal: Replace regex classify() with readonly:true try-forward in v3 fork workers
- Design: .kiro/analysis/ast-classify-design.md + bead 9xiqo.11
- Current state: worker.py has classify() using regex patterns + _install_write_forwarding() that monkey-patches cell.storm/callStorm
- The change: instead of classify() deciding read vs write, ALL queries run with readonly:true. IsReadOnly exception triggers write forwarding.
- This is simpler than the current approach and handles all edge cases automatically.

### Plan
1. Read current worker.py to understand the classify → route flow
2. Modify _patched_storm and _patched_callStorm to always try readonly:true first
3. On IsReadOnly, forward to writer (existing UDS path)
4. Keep regex classify() as optional fast-path: if classify says 'write', skip the try and forward directly
5. Test locally, then EC2

### Dispatched

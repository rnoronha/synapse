# Session State — Thin Router Productionization

## Decision
Going with thin router only. Thick router shelved.

## What's Done
- Thin router fully validated: 6-10x QPS, 4-5x write latency, 0 soak errors
- PR at https://github.com/rnoronha/synapse/pull/1 (branch phase2-clean on fork)
- PR commit: 5d25e0df0 (phase2-clean-v3 branch locally)
- All 28 review comments addressed + linux-expert criticals fixed
- Single-process regression test: PASS (no perf impact when disabled)
- Results: /tmp/thin-router-matrix.md, /tmp/thin-router-matrix-raw.json

## Key Artifacts
- Validated thin artifact: s3://rodrigon-testing-artifacts/matrix-thin.tar.gz (from phase2-clean-v3)
- Working branch: phase2-multi-process (full history + scripts)
- Clean PR branch: phase2-clean-v3 (squashed, synapse/ only)
- Thick router worktree: /local/home/rodrigon/projects/synapse-thick-router (shelved)
- Test scripts: scripts/test_*.py on phase2-multi-process
- Deploy script: scripts/deploy-and-test.sh (uses multi:process:readers key)
- Frozen deploy: /tmp/deploy-and-test-frozen.sh

## Next Steps (Productionization)
1. **Patch minimization** — See GOAL below. Reduce existing-file diff from ~600 lines to ~80 lines.
2. **Config key**: Keep as `multi:process:readers` (matches deployed ECR image). Rename to `core_pct` is a separate follow-up.
3. **Dead code removal**: queryrouter.py and readermanager.py are legacy fallback — remove them and make fork mode the only multi-process mode.
4. **Test harness**: Design at .kiro/analysis/test-harness-design.md — CortexSubprocess context manager for local testing without EC2.
5. **Duplication**: classify() logic duplicated in worker.py and queryrouter.py. Extract to shared module.
6. **3.x port**: Feasibility report done. ~2-3 weeks. Drive subsystem is low risk. Main work is cell startup integration.

## GOAL: Minimize Existing-File Diff for Patch Maintainability

### End State
The thin router PR modifies existing Synapse files by ~80 lines total (down from ~600). All fork-mode logic lives in new files that don't conflict on Synapse upgrades.

### Principle
A) Minimize changes in existing files (these conflict on every rebase)
B) Maximize changes in new files (these never conflict)

### Refactorings

#### 1. cell.py → new synapse/lib/forkmode.py (~350 lines moved out)
Move from cell.py:
- `startmain()` classmethod body
- `_initForFork()` / `_mayFork()` / `prepareFork()`
- `_writer_serve()` async function (the gc.get_objects loop, Event rebinding, slab sync restart)
- `_restoreAfterInitLoop()`

Keep in cell.py (~10 lines):
- `startmain` classmethod that calls `forkmode.run(cls, argv)`
- Any minimal hooks needed (e.g., `getForkInfo()` property)

#### 2. cortex.py → forkmode.py or arbiter.py (~150 lines moved out)
Move from cortex.py:
- `_initForkMode()` method body
- `prepareFork()` method body  
- `_lmdbReaderCheck()` / `_lmdbReaderCheckLoop()`

Keep in cortex.py (~15 lines):
- Config schema entry for `multi:process:readers`
- 2-line branch in `initServiceRuntime`: `if pct: await forkmode.init_fork_mode(self, pct)`

#### 3. daemon.py → worker.py (~36 lines moved out)
Move the `_ensureLinkSess` / session-on-the-fly logic to the worker's connection handler. Daemon.py stays untouched.

#### 4. lmdbslab.py — keep as-is (~30 lines)
`_refresh_ro_xact()` accesses private slab state (self.xact, self.scans) — can't easily externalize. Accept these as stable, minimal changes.

#### 5. Small readonly guards — keep as-is (~20 lines across 5 files)
nexus.py, multislabseqn.py, node.py, auth.py, view.py — 1-5 lines each. These rarely change upstream and are simple `if not self.readonly:` guards.

### Target Diff After Refactoring

| File | Lines changed | Notes |
|------|--------------|-------|
| **New files** | | |
| synapse/lib/forkmode.py | ~500 (new) | All fork lifecycle logic |
| synapse/lib/arbiter.py | ~550 (new) | Process management (already exists) |
| synapse/lib/router.py | ~212 (new) | Thin router (already exists) |
| synapse/lib/worker.py | ~450 (new) | Worker + session handling (already exists) |
| synapse/lib/writechannel.py | ~214 (new) | Write RPC (already exists) |
| **Modified files** | | |
| synapse/cortex.py | ~15 | Config schema + initServiceRuntime branch |
| synapse/lib/cell.py | ~10 | startmain entry point |
| synapse/servers/cortex.py | ~1 | execmain → startmain |
| synapse/lib/lmdbslab.py | ~30 | _refresh_ro_xact, max_readers, NotFoundError |
| synapse/lib/nexus.py | ~3 | readonly guard |
| synapse/lib/multislabseqn.py | ~5 | readonly guard |
| synapse/lib/node.py | ~2 | readonly guard |
| synapse/lib/auth.py | ~3 | readonly guard |
| synapse/lib/view.py | ~3 | readonly guard |
| **Total existing-file changes** | **~72** | Down from ~600 |

### Acceptance Criteria
1. Existing-file diff < 100 lines total
2. All tests still pass (parallel-reads, mixed-load, soak on c5)
3. `python3.11 -c "import synapse.cortex"` passes
4. No behavioral change — same performance numbers

## Key Technical Details
- Config key everywhere (PR, deployed ECR image): `multi:process:readers` 
- ECR: 900555465481.dkr.ecr.us-east-1.amazonaws.com/synapse-cortex-prototype:thin-router (verified: uses `readers`)
- Rename to `core_pct` exists only on working branch (phase2-multi-process) — NOT deployed, NOT in PR
- AWS profiles: rodrigon-testing (EC2 testing), ppa-personal-admin (ECR/PPA stack)
- SSH key for GitHub: ~/.ssh/id_ed25519_personal
- Beads DB: BEADS_DIR=/local/home/rodrigon/projects/synapse/.beads

## Performance Numbers (validated, c5.4xlarge)
- Baseline: 287 QPS parallel reads, 480 ops/s mixed
- Thin Router (8 workers): 1,733 QPS parallel reads, 2,060 ops/s mixed
- r7a.24xlarge (48 workers): 4,371 QPS parallel reads, 3,276 ops/s mixed
- Soak: 0 errors on all configurations (30 min)
- Single-process regression: PASS (no impact when disabled)

# EC2 Test Provisioning Investigation

**Date:** 2026-05-03  
**Investigator:** Kiro (Python expert agent)  
**Scope:** All failure modes in EC2 provisioning scripts causing agent time waste

---

## Executive Summary

Agents spend 80%+ of their time fighting provisioning because of **five compounding failures** that cascade into each other. The #1 killer is the SSH hang in `start-mp-cortex.sh`, which triggers 30-minute watchdog timeouts, retries, and then agents abandon the script and try inline SSH — which has the same bug. The pip version conflict is the #2 killer: it silently runs the wrong synapse version, producing confusing "unknown config key" errors that send agents on wild goose chases.

---

## A. SSH HANG

### Where It Hangs

The hang occurs specifically when starting the Cortex process via SSH. The pattern is:

```
ssh ... ec2-user@IP 'cd / && nohup python3.11 -m synapse.servers.cortex ... > /tmp/cortex-writer.log 2>&1 & echo started-pid=$!'
```

This is in `start-mp-cortex.sh` line 73 (the `run_and_wait` call for Step 4). Other SSH commands (rsync, pip install, pkill, ss) do NOT hang — only the Cortex start command.

### Root Cause

`start-mp-cortex.sh` uses `nohup ... &` but is **missing all three** detachment primitives:

| Primitive | Present? | Purpose |
|-----------|----------|---------|
| `setsid` | ❌ NO | Creates new session, detaches from SSH's controlling terminal |
| `disown` | ❌ NO | Removes job from shell's job table so SIGHUP isn't sent |
| `</dev/null` | ❌ NO | Detaches stdin so SSH doesn't wait for the child to close its stdin fd |

Without these, SSH keeps the connection open because:
1. The child process inherits SSH's stdin file descriptor
2. SSH waits for all file descriptors to close before exiting
3. `nohup` only redirects stdout/stderr — it does NOT close stdin
4. The Cortex process runs forever → SSH session hangs forever

The SSM path (`run_and_wait`) has a different variant of this bug: `aws ssm wait command-executed` blocks until the SSM command completes, but the `nohup ... &` background process keeps the SSM command "running" because the child's stdout/stderr are still connected to the SSM session.

### Evidence from Logs

**g9-throughput-v3.log:**
```
[watchdog] tool 'Running: bash scripts/start-mp-cortex.sh 44.210.116.24 synapse-perf-test.pem /tmp/cortex-mp 2 2>&1' hung for 1803s (limit: 1800s)
Attempt 1 failed: hung process ... Retrying...
[watchdog] tool 'Running: bash scripts/start-mp-cortex.sh ...' hung for 1803s (limit: 1800s)
Error: All 2 attempt(s) failed.
```

Both attempts hung for exactly 1800s (the watchdog limit). The agent then tried inline SSH with the same missing primitives and also hung.

**g9-recovery-v3.log:** Same pattern — agent tried the wrapper script, it hung, agent fell back to inline SSH, that also hung.

**verify-throughput-v2.log:** Agent bypassed the script entirely and did manual SSM commands. When it ran `nohup python3.11 -m synapse.servers.cortex ... &` via SSM, the SSM command also hung because the child process kept the session alive.

### Does start-mp-cortex.sh Fix It?

**No.** The current script has the exact same bug. Line 73:
```bash
run_and_wait "cd / && nohup python3.11 -m synapse.servers.cortex $DATADIR \
  --telepath tcp://0.0.0.0:27492/ \
  --https 0 \
  --auth-anon root \
  > /tmp/cortex-writer.log 2>&1 & echo started-pid=\$!"
```

Missing: `setsid`, `disown`, `</dev/null`.

### Fix

The Cortex start command must be:
```bash
setsid python3.11 -m synapse.servers.cortex $DATADIR \
  --telepath tcp://0.0.0.0:27492/ --https 0 --auth-anon root \
  </dev/null >/tmp/cortex-writer.log 2>&1 &
disown
```

For SSM, the pattern is different — SSM runs commands as a script, not an interactive shell. The fix is:
```bash
nohup setsid python3.11 -m synapse.servers.cortex ... </dev/null >/tmp/cortex-writer.log 2>&1 &
sleep 1  # let the process detach before SSM script exits
```

---

## B. PIP VERSION CONFLICT

### The Conflict

`start-mp-cortex.sh` Step 2 runs:
```bash
run_and_wait "python3.11 -m pip install synapse 2>&1 | tail -5"
```

This installs **upstream synapse from PyPI** (v2.241.0) **system-wide** (as root via SSM).

Then agents do:
```bash
pip3.11 install --user -e .
```

This installs the **local patched version** (with `multi:process:readers` support) into `~/.local/lib/python3.11/site-packages/`.

### Which Takes Precedence?

**The system-wide PyPI version wins.** Python's `sys.path` ordering is:

1. Script directory
2. `PYTHONPATH`
3. **System site-packages** (`/usr/lib/python3.11/site-packages/`)
4. User site-packages (`~/.local/lib/python3.11/site-packages/`)

The `--user` install goes to position 4. The system-wide `pip install synapse` goes to position 3. **System-wide wins.**

### How Agents End Up Running the Wrong Version

1. `start-mp-cortex.sh` Step 2 installs PyPI synapse system-wide as root
2. Agent rsyncs code and does `pip install --user -e .`
3. Agent starts Cortex → Python imports from system site-packages (PyPI version)
4. PyPI synapse doesn't know about `multi:process:readers` → config key ignored or error
5. Agent sees "no readers spawned" → confused → starts debugging the wrong thing

**Evidence from verify-throughput-v2.log:**
```
The config key `multi:process:readers` isn't valid for the upstream synapse package installed via SSM (pip).
...
The SSM command ran as root and installed synapse system-wide from PyPI, overriding our editable install.
...
The system-wide install (from SSM's root pip install) takes precedence.
```

The agent had to `sudo pip3.11 uninstall -y synapse` to fix it.

### Fix

**Remove Step 2 entirely.** The script should never install synapse from PyPI. Instead:
1. Install only the dependencies: `pip3.11 install --user regex lmdb msgpack xxhash PyYAML aiohttp cryptography pyOpenSSL fastjsonschema`
2. Rsync the local code
3. `pip3.11 install --user -e .` (editable install of local code)

Or better: use a `requirements.txt` that lists only dependencies, not synapse itself.

---

## C. PEM KEY

### Where Is It Created?

`provision-test-infra.sh` references `KEY_NAME="synapse-perf-test"` (line 8) and passes it to `aws ec2 run-instances --key-name`. This tells EC2 to inject the public key into the instance.

**But the script never creates or downloads the private key.** The `synapse-perf-test` key pair must already exist in the AWS account, and the `.pem` file must already exist locally.

### Current State

The PEM file exists at `/local/home/rodrigon/projects/synapse/synapse-perf-test.pem` (created 2026-05-03 01:34, 1679 bytes, mode 0400). It was likely created manually or by a prior agent run.

### Is It in .gitignore?

**No.** The `.gitignore` has no `*.pem` entry. This means:
- If someone commits it, the private key leaks into git history
- If someone doesn't have it, they can't SSH to instances
- Agents that start fresh won't find it

### Why Some Agents Find It and Others Don't

The PEM file is an ephemeral local artifact. It exists only if:
1. A human manually created it and placed it in the project root, OR
2. A prior agent session created it (e.g., via `aws ec2 create-key-pair`)

Agents that run on a fresh workspace or after a `git clean` won't have it. The `run-throughput.log` shows an agent that couldn't find it and had to fall back to SSM entirely, using S3 presigned URLs for file transfer.

### Fix

1. Add `*.pem` to `.gitignore` immediately
2. `provision-test-infra.sh` should either:
   - Create the key pair if it doesn't exist: `aws ec2 create-key-pair --key-name synapse-perf-test --query KeyMaterial --output text > synapse-perf-test.pem && chmod 400 synapse-perf-test.pem`
   - Or switch to SSM-only (no SSH needed, no PEM needed) — **recommended**

---

## D. START-MP-CORTEX.SH Line-by-Line Analysis

### Current Script Structure

```
Lines 1-8:   Shebang, set -euo pipefail, argument parsing
Lines 10-15: Variable setup (TARGET, PEM, DATADIR, READER_PCT, REGION, PROFILE)
Lines 17-44: run_and_wait() function — sends SSM command, waits, gets output
Lines 46-47: Banner
Lines 49-50: Step 1 — Install Python 3.11 (dnf install)
Lines 52-53: Step 2 — Install synapse from PyPI ← BUG: installs wrong version
Lines 55-62: Step 3 — Write cell.yaml via multi-line Python ← BUG: JSON escaping
Lines 64-65: Step 4 — Kill existing cortex
Lines 67-75: Step 4 cont — Start cortex via nohup ← BUG: missing setsid/disown/</dev/null
Lines 77-79: Step 5 — Wait 10s, check ports
Lines 81-84: Banner with endpoint info
```

### Failure Modes

| # | Bug | Impact | Severity |
|---|-----|--------|----------|
| 1 | Step 2 installs PyPI synapse system-wide | Shadows local editable install, wrong version runs | **Critical** |
| 2 | Step 3 multi-line Python in JSON string | SSM JSON parameter escaping breaks on newlines/quotes | **Critical** |
| 3 | Step 4 `nohup` without `setsid`/`disown`/`</dev/null` | SSM command never completes → `run_and_wait` hangs forever | **Critical** |
| 4 | No cleanup of prior Cortex data directory | Stale LMDB locks from prior runs can prevent startup | Medium |
| 5 | Step 5 only waits 10s | Cortex with many readers can take 30-60s to fully start | Medium |
| 6 | No health check | Script reports "complete" without verifying Cortex is actually serving | Medium |
| 7 | No file transfer mechanism | Script assumes code is already on the instance | Low |
| 8 | Expects instance ID, not IP | Agents pass IPs (from provision script output), script needs instance IDs | Low |

### Does It Handle a Running Cortex?

Partially. Step 4 does `pkill -f 'synapse.servers.cortex'` which kills existing processes. But it does NOT:
- Wait for the process to actually die (no `sleep` after pkill)
- Clean up stale LMDB lock files in the data directory
- Handle the case where pkill fails (the `|| true` swallows errors)

---

## E. AGENT BEHAVIOR — Why Agents Ignore the Wrapper Script

### The Script Is Broken, So Agents Fall Back

The causal chain is:

1. Agent reads instructions saying "use `start-mp-cortex.sh`"
2. Agent runs the script
3. Script hangs for 1800s (SSH/SSM hang bug)
4. Watchdog kills it
5. Agent retries → hangs again
6. After 2 failed attempts (3600s wasted), agent gives up on the script
7. Agent tries inline SSH commands — reproducing the same bugs manually
8. Inline SSH also hangs (same missing `setsid`/`disown`/`</dev/null`)
9. Agent tries yet another approach (manual SSM, different command structure)
10. Eventually succeeds after 30-60 minutes of fighting

### Is the Instruction Unclear?

The instruction is clear enough ("use the wrapper script"). The problem is that **the script doesn't work**, so agents are forced to improvise. And when they improvise, they reproduce the same bugs because they don't know about the `setsid`/`disown`/`</dev/null` requirement.

### Evidence

**g9-throughput-v3.log:** Agent tried the script twice (1800s each), then fell back to inline SSH with `setsid` (learned from prior context) but still missed `</dev/null` and `disown`.

**g9-recovery-v3.log:** Same pattern. Agent tried script → hung → inline SSH → hung → eventually used SSM with `nohup ... &` → also hung.

**verify-throughput-v2.log:** Agent skipped the script entirely (learned from prior failures) and went straight to manual SSM commands. Still hit the pip version conflict.

**run-throughput.log:** Agent couldn't find PEM file, fell back to SSM-only, had to use S3 presigned URLs for file transfer, fought pip version conflict, eventually succeeded after ~8 minutes of provisioning overhead.

---

## Proposed: `deploy-and-test.sh` — Single End-to-End Script

### Design

A single script that handles everything from instance provisioning to test execution to cleanup. SSM-only, no SSH/PEM required.

```
Usage: bash scripts/deploy-and-test.sh --test throughput [--readers 50] [--instance-type c5.4xlarge]
```

### Phases

```
Phase 1: PROVISION (idempotent)
  - Check for existing running instance with matching tag
  - If none: launch via provision-test-infra.sh
  - Wait for SSM agent to come online (aws ssm describe-instance-information)
  - Output: INSTANCE_ID

Phase 2: SETUP (idempotent)
  - Install Python 3.11 + system deps via SSM
  - Install ONLY dependencies (not synapse itself) via SSM
  - Transfer code via S3 presigned URL (tar.gz → presign → curl on instance)
  - pip install --user -e . (editable install of local code)
  - Verify: python3.11 -c 'import synapse; print(synapse.__file__)' points to /home/ec2-user/synapse/

Phase 3: START CORTEX (with proper detachment)
  - Kill any existing cortex: pkill -f synapse.servers.cortex; sleep 3
  - Clean data dir: rm -rf $DATADIR; mkdir -p $DATADIR
  - Write cell.yaml using printf (no Python/YAML dependency):
      printf 'auth:anon: root\nmulti:process:readers: %d\n' $READER_PCT > $DATADIR/cell.yaml
  - Start with full detachment:
      nohup setsid python3.11 -m synapse.servers.cortex $DATADIR \
        --telepath tcp://0.0.0.0:27492/ --https 0 \
        </dev/null >/tmp/cortex-writer.log 2>&1 &
      sleep 1
  - Health check loop (max 120s, 5s interval):
      ss -tlnp | grep -c '2749[0-9]' >= expected_ports

Phase 4: RUN TEST
  - Execute the requested test script via SSM
  - Stream results back
  - Copy results JSON to local machine

Phase 5: CLEANUP (optional, --no-terminate to skip)
  - Terminate instance
  - Clean up S3 artifacts
```

### Key Design Decisions

1. **SSM-only** — No SSH, no PEM files, no key management
2. **S3 presigned URLs for file transfer** — Works without instance profile
3. **printf for config** — No Python/YAML dependency for writing cell.yaml
4. **Full detachment** — `nohup setsid ... </dev/null ... &; sleep 1`
5. **Health check with timeout** — Don't report success until ports are actually listening
6. **Idempotent phases** — Can re-run after partial failure without starting over
7. **No PyPI synapse install** — Only install dependencies, never the synapse package itself

---

## Should We Switch to SSM-Only?

**Yes. Strongly recommended.**

### Arguments For SSM-Only

| Factor | SSH | SSM |
|--------|-----|-----|
| Key management | Requires PEM file, not in .gitignore, ephemeral | No keys needed |
| Security group | Needs port 22 open | No inbound ports needed |
| Agent compatibility | Agents can't find PEM, fall back to SSM anyway | Works out of the box |
| File transfer | rsync (needs PEM) | S3 presigned URL (no instance profile needed) |
| Process detachment | Needs setsid/disown/</dev/null | Same issue, but SSM RunShellScript has cleaner semantics |
| Audit trail | None | CloudTrail logs every command |
| Network | Needs public IP or bastion | Works via VPC endpoints, no public IP needed |

### Arguments Against

1. **SSM latency** — Each `send-command` + `get-command-invocation` round-trip adds 3-5s overhead. For a script with 10 commands, that's 30-50s of overhead vs near-zero for SSH.
2. **Output size limit** — SSM `StandardOutputContent` is limited to 24KB. Large outputs need S3 output bucket.
3. **No interactive debugging** — Can't SSH in to poke around (but `ssm start-session` provides interactive shell).

### Recommendation

Switch to SSM-only. The PEM file management alone has caused more agent time waste than all the SSM overhead combined. The `run-throughput.log` shows an agent that successfully completed the entire flow via SSM-only in ~8 minutes, while SSH-based attempts in `g9-throughput-v3.log` burned 60+ minutes on hangs.

---

## Summary of All Fixes

| # | Failure Mode | Root Cause | Fix | Priority |
|---|-------------|-----------|-----|----------|
| 1 | SSH/SSM hang on Cortex start | Missing `setsid`/`disown`/`</dev/null` | Add all three detachment primitives | **P0** |
| 2 | Wrong synapse version runs | Step 2 installs PyPI synapse system-wide, shadows editable install | Remove Step 2; install only deps | **P0** |
| 3 | PEM file missing for some agents | Not created by provision script, not in git, not in .gitignore | Switch to SSM-only; add `*.pem` to .gitignore | **P1** |
| 4 | Step 3 JSON escaping breaks SSM | Multi-line Python in JSON string parameter | Use `printf` for cell.yaml instead of Python | **P1** |
| 5 | Script expects instance ID, agents pass IP | provision-test-infra.sh outputs IP, start-mp-cortex.sh expects instance ID | Output both; accept either; or use SSM-only | **P2** |
| 6 | No health check after Cortex start | Script reports success without verifying ports | Add polling loop with timeout | **P2** |
| 7 | Stale LMDB locks from prior runs | No cleanup of data directory before start | `rm -rf $DATADIR` before creating fresh | **P2** |
| 8 | Agents fall back to inline SSH | Script is broken, agents improvise with same bugs | Fix the script so agents don't need to improvise | **P0** |

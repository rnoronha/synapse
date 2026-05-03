# EBS Snapshot-Based Test Data Architecture

## Problem

Seeding 310K+ nodes (100K fqdn, 100K fqdn-with-intervals, 100K geo:place, 10K ipv4, 50 url)
takes minutes per test run. Every `deploy-and-test.sh` invocation pays this cost, even though
the seed data is identical across runs.

## Solution

Seed once → snapshot the EBS data volume → restore from snapshot for each test run.

```
seed-test-dataset.sh          snapshot-test-data.sh         deploy-and-test.sh --data-snapshot
┌──────────────────┐          ┌──────────────────┐          ┌──────────────────────────┐
│ Provision EC2    │          │ Detach volume     │          │ Create volume from snap  │
│ Attach EBS vol   │          │ Snapshot volume   │          │ Attach to instance       │
│ Deploy synapse   │          │ Tag snapshot      │          │ Mount at /mnt/cortex     │
│ Start Cortex on  │──────▶   │ Write manifest    │──────▶   │ Start Cortex on it       │
│   /mnt/cortex    │          │ Clean up          │          │ Skip seeding entirely    │
│ Run ALL seeds    │          └──────────────────┘          └──────────────────────────┘
│ Stop Cortex      │
└──────────────────┘
```

## Design Decisions

### Volume Size: 2 GiB

LMDB for 310K nodes with properties (latlong, .seen intervals, URLs) is ~200-400 MB.
LMDB pre-allocates mmap space, so actual disk usage is higher. 2 GiB gives 5x headroom
for LMDB growth, WAL files, and cell.yaml. gp3 at 2 GiB costs ~$0.16/month.

### One Snapshot for All Branches

The seed data is form-level data (inet:fqdn, geo:place, etc.) — it exercises the storage
layer, not branch-specific code paths. The LMDB format is stable across master/g3/phase2.
One snapshot works for all branches. Recreate only when:
- Seed data schema changes (new forms, new properties)
- LMDB storage format changes (Synapse major version bump)
- Node counts need to increase

### Mount Path and Filesystem

- Device: `/dev/xvdf` (first available non-root device on AL2023)
- Mount: `/mnt/cortex-data`
- Filesystem: ext4 (default for EBS, no special requirements)
- The Cortex `--dirn` points to `/mnt/cortex-data` instead of `/tmp/cortex-data`

### Snapshot Lifecycle

Tag each snapshot with a version string: `v1`, `v2`, etc. The manifest records the
current snapshot ID. Recreate by running seed + snapshot scripts. Old snapshots are
not auto-deleted (pennies/month for storage).

### What Gets Seeded

All test data from all test scripts, in one Cortex:

| Source | Forms | Count | Purpose |
|--------|-------|-------|---------|
| test_pathological.py | inet:fqdn | 100,000 | Broad lift |
| test_pathological.py | inet:fqdn (with .seen) | 100,000 | Interval lift |
| test_pathological.py | geo:place (with latlong) | 100,000 | Geospatial |
| test_pathological.py | inet:ipv4 | 10,000 | Filter regex |
| test_pathological.py | inet:url | 50 | URL normalization |
| test_correctness.py | inet:fqdn, ipv4, url, dns:a, tags | ~85 | Read correctness |
| test_throughput.py | inet:fqdn, ipv4, inet:url | 1,000 | Throughput bench |
| test_parallel_reads.py | inet:fqdn, ipv4, inet:url | 1,000 | Parallel read bench |
| test_mixed_load.py | inet:fqdn | 500 | Mixed load bench |
| test_recovery.py | inet:fqdn | 100 | Recovery test |

No conflicts: each test uses distinct domain suffixes (pathbench.com, test.com,
bench.throughput.com, bench.parallel.com, bench.mixed.com, recovery-N.test.com).

### Snapshot Tags

```
Name:           synapse-test-data-v1
dataset-version: v1
node-count:     311685
branch:         phase2-multi-process
created:        2026-05-03T12:00:00Z
```

## Scripts

### 1. `scripts/seed-test-dataset.sh`

Provisions an EC2 instance, attaches a dedicated EBS volume, deploys synapse,
starts a Cortex on the EBS volume, runs a seed script via telepath that calls
all seed functions, then stops the Cortex cleanly.

Key details:
- Uses the same infra constants as deploy-and-test.sh (AMI, SG, subnet, IAM profile)
- Creates a 2 GiB gp3 volume in the instance's AZ
- Formats as ext4, mounts at /mnt/cortex-data
- Runs `scripts/seed_all_test_data.py` which imports and calls each test's seed function
- Stops Cortex with SIGTERM, waits for clean shutdown
- Leaves instance + volume running for snapshot-test-data.sh

### 2. `scripts/snapshot-test-data.sh`

Takes the instance ID and volume ID from seed-test-dataset.sh output:
- Stops the Cortex (if still running) for crash-consistent snapshot
- Unmounts and detaches the volume
- Creates an EBS snapshot with tags
- Waits for snapshot completion
- Writes snapshot ID to artifacts/snapshot-manifest.json
- Terminates the instance, deletes the volume

### 3. `deploy-and-test.sh` Integration

New flag: `--data-snapshot <snap-id>`

In Phase 1 (provision):
- After instance is running, create a 2 GiB volume from the snapshot
- Attach as /dev/xvdf
- Mount at /mnt/cortex-data (no mkfs — data already on it)

In Phase 4 (start cortex):
- Use DATADIR=/mnt/cortex-data instead of /tmp/cortex-data
- Write cell.yaml into the existing data dir
- Skip seeding entirely (data already present)

The `--data-snapshot` flag is optional. Without it, behavior is unchanged.

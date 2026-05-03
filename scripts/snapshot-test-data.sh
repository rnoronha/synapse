#!/bin/bash
set -euo pipefail

# snapshot-test-data.sh — Snapshot the seeded EBS volume and record in manifest.
# Run after seed-test-dataset.sh completes.
#
# Usage:
#   bash scripts/snapshot-test-data.sh <instance-id> <volume-id> [--version <v1>]

INSTANCE_ID="${1:?Usage: $0 <instance-id> <volume-id> [--version <vN>]}"
VOLUME_ID="${2:?Usage: $0 <instance-id> <volume-id> [--version <vN>]}"
shift 2

REGION="us-east-1"
DATASET_VERSION="v1"
MOUNT_POINT="/mnt/cortex-data"
DEVICE="/dev/xvdf"

export AWS_PROFILE=rodrigon-testing

while [[ $# -gt 0 ]]; do
    case $1 in
        --version) DATASET_VERSION="$2"; shift 2;;
        --region)  REGION="$2"; shift 2;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

BRANCH=$(git rev-parse --abbrev-ref HEAD)
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "═══════════════════════════════════════════════════════════"
echo "  SNAPSHOT TEST DATA"
echo "  Instance: $INSTANCE_ID"
echo "  Volume:   $VOLUME_ID"
echo "  Version:  $DATASET_VERSION"
echo "═══════════════════════════════════════════════════════════"

# ── helper: run command via SSM ───────────────────────────────────────
run_ssm() {
    local cmd="$1"
    local timeout="${2:-120}"
    local cmd_id
    cmd_id=$(aws ssm send-command \
        --region "$REGION" \
        --instance-ids "$INSTANCE_ID" \
        --document-name AWS-RunShellScript \
        --timeout-seconds "$timeout" \
        --parameters "{\"commands\":[$(printf '%s' "$cmd" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')]}" \
        --query Command.CommandId --output text)

    local status=""
    while true; do
        sleep 5
        status=$(aws ssm get-command-invocation \
            --region "$REGION" \
            --command-id "$cmd_id" \
            --instance-id "$INSTANCE_ID" \
            --query Status --output text 2>/dev/null || echo "Pending")
        case $status in
            Success)  break;;
            Failed|TimedOut|Cancelled)
                aws ssm get-command-invocation \
                    --region "$REGION" \
                    --command-id "$cmd_id" \
                    --instance-id "$INSTANCE_ID" \
                    --query StandardErrorContent --output text >&2
                return 1;;
            *) ;;
        esac
    done

    aws ssm get-command-invocation \
        --region "$REGION" \
        --command-id "$cmd_id" \
        --instance-id "$INSTANCE_ID" \
        --query StandardOutputContent --output text
}

# ── STEP 1: ENSURE CORTEX IS STOPPED ─────────────────────────────────
echo ""
echo "── STEP 1: STOP CORTEX ────────────────────────────────────"
run_ssm "pkill -TERM -f 'synapse.servers.cortex' 2>/dev/null || true; sleep 5; sync"

# ── STEP 2: UNMOUNT AND DETACH ────────────────────────────────────────
echo ""
echo "── STEP 2: UNMOUNT AND DETACH ─────────────────────────────"
run_ssm "sudo umount $MOUNT_POINT 2>/dev/null || true"

aws ec2 detach-volume \
    --region "$REGION" \
    --volume-id "$VOLUME_ID" \
    --force --output text >/dev/null 2>&1 || true

echo "Waiting for volume to detach..."
aws ec2 wait volume-available --region "$REGION" --volume-ids "$VOLUME_ID"
echo "Volume detached."

# ── STEP 3: CREATE SNAPSHOT ───────────────────────────────────────────
echo ""
echo "── STEP 3: CREATE SNAPSHOT ────────────────────────────────"

SNAPSHOT_ID=$(aws ec2 create-snapshot \
    --region "$REGION" \
    --volume-id "$VOLUME_ID" \
    --description "Synapse test data $DATASET_VERSION - 310K+ nodes" \
    --tag-specifications "ResourceType=snapshot,Tags=[
        {Key=Name,Value=synapse-test-data-$DATASET_VERSION},
        {Key=dataset-version,Value=$DATASET_VERSION},
        {Key=node-count,Value=311685},
        {Key=branch,Value=$BRANCH},
        {Key=created,Value=$DATE}
    ]" \
    --query SnapshotId --output text)

echo "Snapshot: $SNAPSHOT_ID"
echo "Waiting for snapshot to complete (this may take a few minutes)..."
aws ec2 wait snapshot-completed --region "$REGION" --snapshot-ids "$SNAPSHOT_ID"
echo "Snapshot complete."

# ── STEP 4: WRITE MANIFEST ───────────────────────────────────────────
echo ""
echo "── STEP 4: WRITE MANIFEST ─────────────────────────────────"

VOLUME_SIZE=2  # match seed script

mkdir -p artifacts
python3 -c "
import json, pathlib

manifest_path = pathlib.Path('artifacts/snapshot-manifest.json')

# Load existing or start fresh
if manifest_path.exists():
    manifest = json.loads(manifest_path.read_text())
else:
    manifest = {'snapshots': {}}

manifest['current'] = '$DATASET_VERSION'
manifest['snapshots']['$DATASET_VERSION'] = {
    'snapshot_id': '$SNAPSHOT_ID',
    'volume_size_gib': $VOLUME_SIZE,
    'node_count': 311685,
    'branch': '$BRANCH',
    'created': '$DATE',
    'region': '$REGION',
}

manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
print(f'Written: {manifest_path}')
"

cat artifacts/snapshot-manifest.json

# ── STEP 5: CLEANUP ──────────────────────────────────────────────────
echo ""
echo "── STEP 5: CLEANUP ────────────────────────────────────────"

echo "Terminating instance $INSTANCE_ID..."
aws ec2 terminate-instances --region "$REGION" --instance-ids "$INSTANCE_ID" --output text >/dev/null

echo "Deleting volume $VOLUME_ID..."
aws ec2 delete-volume --region "$REGION" --volume-id "$VOLUME_ID" 2>/dev/null || true

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  SNAPSHOT COMPLETE"
echo "  Snapshot: $SNAPSHOT_ID"
echo "  Version:  $DATASET_VERSION"
echo ""
echo "  Use with: bash scripts/deploy-and-test.sh --test <test> --data-snapshot $SNAPSHOT_ID"
echo "═══════════════════════════════════════════════════════════"

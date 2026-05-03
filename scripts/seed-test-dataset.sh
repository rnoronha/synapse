#!/bin/bash
set -euo pipefail

# seed-test-dataset.sh — Provision EC2, attach EBS, deploy synapse, seed all test data.
# Output: instance ID and volume ID for snapshot-test-data.sh.
#
# Usage:
#   bash scripts/seed-test-dataset.sh [--artifact <s3-url>]

ARTIFACT=""
REGION="us-east-1"
S3_BUCKET="rodrigon-testing-artifacts"
INSTANCE_TYPE="c5.4xlarge"
VOLUME_SIZE=2  # GiB — 5x headroom for 310K nodes in LMDB
MOUNT_POINT="/mnt/cortex-data"
DEVICE="/dev/xvdf"

# Hardcoded infra (from provision-test-infra.sh)
AMI="ami-0e1e769742d1cfb49"
SG_ID="sg-0a29a79adae40d6d9"
SUBNET="subnet-0ba72e38d87dc5061"
IAM_PROFILE="SSMInstanceProfile"

export AWS_PROFILE=rodrigon-testing
INSTANCE_ID=""
VOLUME_ID=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --artifact)      ARTIFACT="$2"; shift 2;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2;;
        --region)        REGION="$2"; shift 2;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

BRANCH=$(git rev-parse --abbrev-ref HEAD)

echo "═══════════════════════════════════════════════════════════"
echo "  SEED TEST DATASET"
echo "  Branch:   $BRANCH"
echo "  Instance: $INSTANCE_TYPE"
echo "  Volume:   ${VOLUME_SIZE} GiB gp3"
echo "═══════════════════════════════════════════════════════════"

# ── helper: run command via SSM ───────────────────────────────────────
run_ssm() {
    local cmd="$1"
    local timeout="${2:-600}"
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
                echo "ERROR: SSM command failed (status=$status)" >&2
                aws ssm get-command-invocation \
                    --region "$REGION" \
                    --command-id "$cmd_id" \
                    --instance-id "$INSTANCE_ID" \
                    --query StandardErrorContent --output text >&2
                aws ssm get-command-invocation \
                    --region "$REGION" \
                    --command-id "$cmd_id" \
                    --instance-id "$INSTANCE_ID" \
                    --query StandardOutputContent --output text
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

# ── PHASE 1: PROVISION INSTANCE ──────────────────────────────────────
echo ""
echo "── PHASE 1: PROVISION ─────────────────────────────────────"

INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET" \
    --iam-instance-profile "Name=$IAM_PROFILE" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=synapse-seed-$$}]" \
    --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

# Get AZ for volume creation
AZ=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' --output text)

echo "Waiting for SSM agent..."
for i in $(seq 1 60); do
    if aws ssm describe-instance-information \
        --region "$REGION" \
        --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
        --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null | grep -q Online; then
        echo "SSM agent online."
        break
    fi
    [[ $i -eq 60 ]] && { echo "ERROR: SSM agent not online after 300s"; exit 1; }
    sleep 5
done

# ── PHASE 2: CREATE AND ATTACH EBS VOLUME ────────────────────────────
echo ""
echo "── PHASE 2: EBS VOLUME ────────────────────────────────────"

VOLUME_ID=$(aws ec2 create-volume \
    --region "$REGION" \
    --availability-zone "$AZ" \
    --size "$VOLUME_SIZE" \
    --volume-type gp3 \
    --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=synapse-seed-data-$$}]" \
    --query VolumeId --output text)

echo "Volume: $VOLUME_ID ($AZ)"
aws ec2 wait volume-available --region "$REGION" --volume-ids "$VOLUME_ID"

aws ec2 attach-volume \
    --region "$REGION" \
    --volume-id "$VOLUME_ID" \
    --instance-id "$INSTANCE_ID" \
    --device "$DEVICE" --output text >/dev/null

echo "Waiting for attachment..."
sleep 10

echo "Formatting and mounting..."
run_ssm "sudo mkfs.ext4 -q $DEVICE && sudo mkdir -p $MOUNT_POINT && sudo mount $DEVICE $MOUNT_POINT && sudo chown ec2-user:ec2-user $MOUNT_POINT"

# ── PHASE 3: DEPLOY SYNAPSE ──────────────────────────────────────────
echo ""
echo "── PHASE 3: DEPLOY SYNAPSE ────────────────────────────────"

run_ssm "sudo dnf install -y python3.11 python3.11-pip python3.11-devel gcc && pip3.11 install --user regex lmdb msgpack xxhash PyYAML aiohttp cryptography pyOpenSSL fastjsonschema"

if [[ -n "$ARTIFACT" ]]; then
    echo "Using artifact: $ARTIFACT"
    run_ssm "mkdir -p /home/ec2-user/synapse && aws s3 cp $ARTIFACT /tmp/synapse-deploy.tar.gz --region $REGION && tar xzf /tmp/synapse-deploy.tar.gz -C /home/ec2-user/synapse"
else
    S3_KEY="synapse-seed-$$.tar.gz"
    TARBALL=/tmp/synapse-deploy.tar.gz
    tar czf "$TARBALL" --exclude=.git --exclude=__pycache__ --exclude=.kiro --exclude='*.pem' -C "$(pwd)" .
    aws s3 cp "$TARBALL" "s3://$S3_BUCKET/$S3_KEY" --region "$REGION" --quiet
    rm -f "$TARBALL"
    run_ssm "mkdir -p /home/ec2-user/synapse && aws s3 cp s3://$S3_BUCKET/$S3_KEY /tmp/synapse-deploy.tar.gz --region $REGION && tar xzf /tmp/synapse-deploy.tar.gz -C /home/ec2-user/synapse"
    aws s3 rm "s3://$S3_BUCKET/$S3_KEY" --quiet 2>/dev/null || true
fi

run_ssm "cd /home/ec2-user/synapse && pip3.11 install --user -e . 2>&1 | tail -3"

# ── PHASE 4: START CORTEX ON EBS VOLUME ──────────────────────────────
echo ""
echo "── PHASE 4: START CORTEX ──────────────────────────────────"

run_ssm "printf 'auth:anon: root\n' > $MOUNT_POINT/cell.yaml"
run_ssm "nohup setsid python3.11 -m synapse.servers.cortex $MOUNT_POINT --telepath tcp://0.0.0.0:27492/ --https 0 </dev/null >/tmp/cortex.log 2>&1 & sleep 2"

echo "Waiting for Cortex to be ready..."
run_ssm "for i in \$(seq 1 30); do python3.11 -c \"
import asyncio, sys
async def check():
    import synapse.telepath as t
    async with await t.openurl('tcp://127.0.0.1:27492/cortex') as prox:
        await prox.getCellInfo()
        print('READY')
asyncio.run(check())
\" 2>/dev/null && break || sleep 2; done"

# ── PHASE 5: SEED ALL TEST DATA ──────────────────────────────────────
echo ""
echo "── PHASE 5: SEED ALL TEST DATA ───────────────────────────"

# Inline seed script — calls each test's seed function via telepath
run_ssm "cd /home/ec2-user/synapse && python3.11 -c \"
import asyncio, sys, time
sys.path.insert(0, 'scripts')

async def main():
    import synapse.telepath as t
    url = 'tcp://127.0.0.1:27492/cortex'
    async with await t.openurl(url) as prox:
        t0 = time.monotonic()

        # 1. Pathological seed (310K nodes — the big one)
        print('Seeding pathological data (310K nodes)...')
        from test_pathological import _seed_if_empty
        await _seed_if_empty(prox, timeout=600)

        # 2. Correctness seed (~85 nodes)
        print('Seeding correctness data...')
        from test_correctness import _build_seed_queries
        queries = _build_seed_queries()
        for start in range(0, len(queries), 20):
            chunk = ' '.join(queries[start:start + 20])
            async for _ in prox.storm(chunk):
                pass

        # 3. Throughput seed (1000 nodes)
        print('Seeding throughput data...')
        from test_throughput import _seed_nodes as seed_tp
        await seed_tp(prox)

        # 4. Parallel reads seed (1000 nodes)
        print('Seeding parallel-reads data...')
        from test_parallel_reads import _seed_nodes as seed_pr
        await seed_pr(prox)

        # 5. Mixed load seed (500 nodes)
        print('Seeding mixed-load data...')
        from test_mixed_load import _seed_nodes as seed_ml
        await seed_ml(prox)

        # 6. Recovery seed (100 nodes)
        print('Seeding recovery data...')
        for i in range(100):
            async for _ in prox.storm(f'[inet:fqdn=recovery-{i}.test.com]'):
                pass

        elapsed = time.monotonic() - t0
        print(f'All seeding complete in {elapsed:.1f}s')

asyncio.run(main())
\" " 1800

# ── PHASE 6: STOP CORTEX CLEANLY ─────────────────────────────────────
echo ""
echo "── PHASE 6: STOP CORTEX ──────────────────────────────────"

run_ssm "pkill -TERM -f 'synapse.servers.cortex' || true; sleep 5; pkill -0 -f 'synapse.servers.cortex' 2>/dev/null && sleep 10 || echo 'Cortex stopped'"
run_ssm "sync"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  SEED COMPLETE"
echo "  Instance: $INSTANCE_ID"
echo "  Volume:   $VOLUME_ID"
echo "  Mount:    $MOUNT_POINT"
echo ""
echo "  Next: bash scripts/snapshot-test-data.sh $INSTANCE_ID $VOLUME_ID"
echo "═══════════════════════════════════════════════════════════"

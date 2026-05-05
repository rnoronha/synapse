#!/bin/bash
set -euo pipefail

# run-prod-equiv-matrix.sh — Run 4 tests on a single r7a.24xlarge instance
# Provisions once, runs all tests sequentially, terminates at end.
#
# Usage: bash scripts/run-prod-equiv-matrix.sh

INSTANCE_TYPE="r7a.24xlarge"
BRANCH="phase2-multi-process"
READERS=50
DATA_SNAPSHOT="snap-0572436bb7e5d6d78"
REGION="us-east-1"
S3_BUCKET="rodrigon-testing-artifacts"
DATADIR="/mnt/cortex-data"

# Hardcoded infra
AMI="ami-0e1e769742d1cfb49"
SG_ID="sg-0a29a79adae40d6d9"
SUBNET="subnet-0ba72e38d87dc5061"
IAM_PROFILE="SSMInstanceProfile"

export AWS_PROFILE=rodrigon-testing
INSTANCE_ID=""
SNAP_VOL_ID=""

echo "═══════════════════════════════════════════════════════════"
echo "  PRODUCTION-EQUIVALENT TEST MATRIX"
echo "  Instance: $INSTANCE_TYPE (96 vCPU, 768 GiB)"
echo "  Branch:   $BRANCH"
echo "  Readers:  $READERS (= 48 reader processes)"
echo "  Snapshot: $DATA_SNAPSHOT (311K nodes)"
echo "═══════════════════════════════════════════════════════════"

cleanup() {
    echo ""
    echo "── CLEANUP ────────────────────────────────────────────────"
    if [[ -n "$INSTANCE_ID" ]]; then
        echo "Terminating instance $INSTANCE_ID..."
        aws ec2 terminate-instances --region "$REGION" --instance-ids "$INSTANCE_ID" --output text >/dev/null 2>&1 || true
    fi
    if [[ -n "$SNAP_VOL_ID" ]]; then
        echo "Waiting for volume detach..."
        sleep 30
        aws ec2 delete-volume --region "$REGION" --volume-id "$SNAP_VOL_ID" 2>/dev/null || true
    fi
    echo "Done."
}
trap cleanup EXIT

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

# ── PROVISION ─────────────────────────────────────────────────────────
echo ""
echo "── PROVISIONING r7a.24xlarge ──────────────────────────────"

INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET" \
    --iam-instance-profile "Name=$IAM_PROFILE" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=synapse-prod-equiv},{Key=Project,Value=synapse-perf-test}]" \
    --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Waiting for running state..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

echo "Waiting for SSM agent..."
for i in $(seq 1 60); do
    if aws ssm describe-instance-information \
        --region "$REGION" \
        --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
        --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null | grep -q Online; then
        echo "SSM agent online."
        break
    fi
    if [[ $i -eq 60 ]]; then echo "ERROR: SSM agent not online after 300s"; exit 1; fi
    sleep 5
done

# ── ATTACH SNAPSHOT ───────────────────────────────────────────────────
echo ""
echo "── ATTACHING SNAPSHOT VOLUME ──────────────────────────────"

AZ=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' --output text)

SNAP_VOL_ID=$(aws ec2 create-volume \
    --region "$REGION" \
    --availability-zone "$AZ" \
    --snapshot-id "$DATA_SNAPSHOT" \
    --volume-type gp3 \
    --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=synapse-prod-equiv-snap}]" \
    --query VolumeId --output text)

echo "Volume $SNAP_VOL_ID from snapshot $DATA_SNAPSHOT"
aws ec2 wait volume-available --region "$REGION" --volume-ids "$SNAP_VOL_ID"

aws ec2 attach-volume \
    --region "$REGION" \
    --volume-id "$SNAP_VOL_ID" \
    --instance-id "$INSTANCE_ID" \
    --device /dev/xvdf --output text >/dev/null

sleep 10
run_ssm "sudo mkdir -p $DATADIR && sudo mount /dev/xvdf $DATADIR && sudo chown ec2-user:ec2-user $DATADIR"
echo "Snapshot data mounted at $DATADIR"

# ── SETUP ─────────────────────────────────────────────────────────────
echo ""
echo "── INSTALLING DEPENDENCIES ────────────────────────────────"
run_ssm "sudo dnf install -y python3.11 python3.11-pip python3.11-devel gcc && pip3.11 install --user regex lmdb msgpack xxhash PyYAML aiohttp cryptography pyOpenSSL fastjsonschema"

# ── TRANSFER CODE ─────────────────────────────────────────────────────
echo ""
echo "── TRANSFERRING CODE ──────────────────────────────────────"

# Find latest artifact
ARTIFACT=$(aws s3 ls "s3://$S3_BUCKET/" --region "$REGION" 2>/dev/null \
    | grep "synapse-phase2" | sort -k1,2 | tail -1 | awk "{print \$4}")
if [[ -n "$ARTIFACT" ]]; then
    ARTIFACT="s3://$S3_BUCKET/$ARTIFACT"
    echo "Using artifact: $ARTIFACT"
    run_ssm "mkdir -p /home/ec2-user/synapse && aws s3 cp $ARTIFACT /tmp/synapse-deploy.tar.gz --region $REGION && tar xzf /tmp/synapse-deploy.tar.gz -C /home/ec2-user/synapse"
    run_ssm "cd /home/ec2-user/synapse && pip3.11 install --user -e . 2>&1 | tail -3"
else
    echo "ERROR: No artifact found"; exit 1
fi

# ── FUNCTION: START CORTEX ────────────────────────────────────────────
start_cortex() {
    echo "Stopping any existing cortex..."
    run_ssm "pkill -f 'synapse.servers.cortex' || true; sleep 3"

    echo "Writing cell.yaml (readers=$READERS)..."
    run_ssm "printf 'auth:anon: root\nmulti:process:core_pct: $READERS\n' > $DATADIR/cell.yaml"

    echo "Starting cortex..."
    run_ssm "nohup setsid python3.11 -m synapse.servers.cortex $DATADIR --telepath tcp://0.0.0.0:27492/ --https 0 </dev/null >/tmp/cortex.log 2>&1 & sleep 2"

    # 48 readers take ~3-5 min to fully start on r7a.24xlarge
    echo "Waiting for writer port 27492 (max 300s)..."
    for i in $(seq 1 60); do
        PORT_COUNT=$(run_ssm "ss -tlnp | grep '27492' | wc -l" | tr -d '[:space:]')
        if [[ "$PORT_COUNT" -ge 1 ]]; then
            echo "Writer port listening."
            break
        fi
        if [[ $i -eq 60 ]]; then
            echo "ERROR: Writer port not up after 300s"
            run_ssm "tail -50 /tmp/cortex.log" || true
            exit 1
        fi
        sleep 5
    done

    # Wait for all reader processes to spawn (check log for readiness)
    echo "Waiting for all reader processes to start (max 600s)..."
    for i in $(seq 1 120); do
        READY=$(run_ssm "grep -c 'is listening' /tmp/cortex.log 2>/dev/null || echo 0" | tr -d '[:space:]')
        # With 50% readers on 96 cores = 48 readers, expect 48 "is listening" lines
        if [[ "$READY" -ge 48 ]]; then
            echo "All $READY reader ports listening."
            break
        fi
        if [[ $i -eq 120 ]]; then
            echo "WARNING: Only $READY/48 readers after 600s, proceeding anyway"
            break
        fi
        sleep 5
    done

    echo "Verifying telepath connectivity (max 120s)..."
    run_ssm "for i in \$(seq 1 60); do python3.11 -c \"
import asyncio, sys
async def check():
    try:
        import synapse.telepath as t
        async with await t.openurl('tcp://127.0.0.1:27492/cortex') as prox:
            await prox.getCellInfo()
            print('READY')
    except Exception as e:
        print(f'WAITING: {e}')
        sys.exit(1)
asyncio.run(check())
\" && break || sleep 2; done"
}

# ── FUNCTION: RUN TEST ────────────────────────────────────────────────
run_test() {
    local test_name="$1"
    local test_cmd="$2"
    local timeout="${3:-600}"

    echo ""
    echo "══════════════════════════════════════════════════════════"
    echo "  TEST: $test_name"
    echo "══════════════════════════════════════════════════════════"

    set +e
    TEST_OUTPUT=$(run_ssm "$test_cmd" "$timeout")
    TEST_EXIT=$?
    set -e
    echo "$TEST_OUTPUT"

    # Fetch results JSON
    RESULTS_JSON=$(run_ssm "cat /tmp/test-results.json 2>/dev/null || echo '{}'" | tr -d '\n')
    if [[ "$RESULTS_JSON" != "{}" ]]; then
        mkdir -p results/prod-equiv
        echo "$RESULTS_JSON" > "results/prod-equiv/${test_name}-results.json"
        echo "→ Saved: results/prod-equiv/${test_name}-results.json"
    fi

    if [[ $TEST_EXIT -ne 0 ]]; then
        echo "WARNING: $test_name exited $TEST_EXIT"
        run_ssm "tail -20 /tmp/cortex.log" || true
    fi

    # Clear results file for next test
    run_ssm "rm -f /tmp/test-results.json"
    return $TEST_EXIT
}

# ── START CORTEX ──────────────────────────────────────────────────────
echo ""
echo "── STARTING CORTEX (48 reader processes) ──────────────────"
start_cortex

WRITER_URL="tcp://127.0.0.1:27492/cortex"

# ── TEST 1: parallel-reads ────────────────────────────────────────────
run_test "parallel-reads" \
    "cd /home/ec2-user/synapse && python3.11 scripts/test_parallel_reads.py $WRITER_URL --concurrency 64 --output /tmp/test-results.json" \
    600

# ── TEST 2: throughput ────────────────────────────────────────────────
run_test "throughput" \
    "cd /home/ec2-user/synapse && python3.11 scripts/test_throughput.py $WRITER_URL --output /tmp/test-results.json" \
    600

# ── TEST 3: mixed-load (120s) ─────────────────────────────────────────
run_test "mixed-load" \
    "cd /home/ec2-user/synapse && python3.11 scripts/test_mixed_load.py $WRITER_URL --write-pct 10 --duration 120 --output /tmp/test-results.json" \
    600

# ── TEST 4: read-after-write (60s per condition) ──────────────────────
run_test "read-after-write" \
    "cd /home/ec2-user/synapse && python3.11 scripts/test_read_after_write.py $WRITER_URL --duration 60 --output /tmp/test-results.json" \
    600

# ── SUMMARY ───────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ALL 4 TESTS COMPLETE"
echo "  Results in: results/prod-equiv/"
echo "  Instance $INSTANCE_ID will be terminated."
echo "═══════════════════════════════════════════════════════════"

#!/bin/bash
set -euo pipefail

# deploy-and-test.sh — SSM-only EC2 test runner for Synapse Cortex.
# No SSH, no PEM files. Provisions, deploys, runs test, cleans up.
#
# Usage:
#   bash scripts/deploy-and-test.sh --branch phase2-multi-process --test throughput --readers 50
#   bash scripts/deploy-and-test.sh --test soak --duration 600
#   bash scripts/deploy-and-test.sh --test correctness

# ── defaults ──────────────────────────────────────────────────────────
BRANCH=""
TEST=""
READERS=""
ARTIFACT=""
INSTANCE_TYPE="c5.4xlarge"
DURATION=600
REGION="us-east-1"
S3_BUCKET="rodrigon-testing-artifacts"
S3_KEY="synapse-deploy-$$.tar.gz"
DATADIR="/tmp/cortex-data"
DATA_SNAPSHOT=""

# ── hardcoded infra (from provision-test-infra.sh) ────────────────────
AMI="ami-0e1e769742d1cfb49"
SG_ID="sg-0a29a79adae40d6d9"
SUBNET="subnet-0ba72e38d87dc5061"
IAM_PROFILE="SSMInstanceProfile"

export AWS_PROFILE=rodrigon-testing
INSTANCE_ID=""

# ── arg parsing ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --branch)        BRANCH="$2";        shift 2;;
        --test)          TEST="$2";          shift 2;;
        --readers)       READERS="$2";       shift 2;;
        --artifact)      ARTIFACT="$2";      shift 2;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2;;
        --duration)      DURATION="$2";      shift 2;;
        --region)        REGION="$2";        shift 2;;
        --data-snapshot) DATA_SNAPSHOT="$2"; shift 2;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

if [[ -z "$TEST" ]]; then
    echo "Usage: $0 --test <pathological|soak|correctness|throughput|recovery|read-after-write|parallel-reads> [options]"
    echo ""
    echo "Options:"
    echo "  --branch <name>         Branch to test (default: current branch)"
    echo "  --readers <0|25|50>     Reader percentage (default: 0 for master/g3, 50 for phase2)"
    echo "  --artifact <s3-url>      Use pre-built S3 artifact instead of building from git"
    echo "  --instance-type <type>  EC2 instance type (default: c5.4xlarge)"
    echo "  --duration <secs>       Soak test duration (default: 600)"
    echo "  --region <region>       AWS region (default: us-east-1)"
    echo "  --data-snapshot <id>    EBS snapshot with pre-seeded test data"
    exit 1
fi

# Validate test name
case $TEST in
    pathological|soak|correctness|throughput|recovery|read-after-write|parallel-reads|mixed-load) ;;
    *) echo "ERROR: Unknown test '$TEST'"; exit 1;;
esac

# Default branch = current branch
if [[ -z "$BRANCH" ]]; then
    BRANCH=$(git rev-parse --abbrev-ref HEAD)
fi

# Default readers: 0 for master/g3, 50 for phase2
if [[ -z "$READERS" ]]; then
    case $BRANCH in
        master|g3-piecemeal-fixes) READERS=0;;
        *)                         READERS=50;;
    esac
fi

echo "═══════════════════════════════════════════════════════════"
echo "  Branch:   $BRANCH"
echo "  Test:     $TEST"
echo "  Readers:  $READERS"
echo "  Instance: $INSTANCE_TYPE"
echo "  Region:   $REGION"
if [[ -n "$DATA_SNAPSHOT" ]]; then
echo "  Snapshot: $DATA_SNAPSHOT"
fi
echo "═══════════════════════════════════════════════════════════"

# ── cleanup trap ──────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "── CLEANUP ────────────────────────────────────────────────"
    if [[ -n "$INSTANCE_ID" ]]; then
        echo "Terminating instance $INSTANCE_ID..."
        aws ec2 terminate-instances --region "$REGION" --instance-ids "$INSTANCE_ID" --output text >/dev/null 2>&1 || true
    fi
    if [[ -n "${SNAP_VOL_ID:-}" ]]; then
        echo "Waiting for instance termination to release volume..."
        sleep 30
        aws ec2 delete-volume --region "$REGION" --volume-id "$SNAP_VOL_ID" 2>/dev/null || true
    fi
    aws s3 rm "s3://$S3_BUCKET/$S3_KEY" >/dev/null 2>&1 || true
    rm -f /tmp/synapse-deploy.tar.gz
    echo "Done."
}
trap cleanup EXIT

# ── helper: run command via SSM ───────────────────────────────────────
# Stdout  → captured StandardOutputContent (always, success or failure)
# Stderr  → printed to fd2 (visible in terminal, not captured by $())
# Returns → SSM exit status (0=Success, 1=Failed/TimedOut/Cancelled)
# NOTE: SSM StandardOutputContent is capped at 24KB. Tests write results
#       to /tmp/test-results.json and we fetch that file separately.
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

    # Poll until complete (aws ssm wait command-executed has a 5-min max)
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
                # Print stderr to terminal
                aws ssm get-command-invocation \
                    --region "$REGION" \
                    --command-id "$cmd_id" \
                    --instance-id "$INSTANCE_ID" \
                    --query StandardErrorContent --output text >&2
                # Still emit stdout so callers can capture it
                aws ssm get-command-invocation \
                    --region "$REGION" \
                    --command-id "$cmd_id" \
                    --instance-id "$INSTANCE_ID" \
                    --query StandardOutputContent --output text
                return 1;;
            *) ;;  # InProgress, Pending, etc — keep polling
        esac
    done

    aws ssm get-command-invocation \
        --region "$REGION" \
        --command-id "$cmd_id" \
        --instance-id "$INSTANCE_ID" \
        --query StandardOutputContent --output text
}


# ── AUTO-RESOLVE ARTIFACT ─────────────────────────────────────────────
if [[ -z "$ARTIFACT" ]]; then
    # Map branch name to artifact prefix
    case "$BRANCH" in
        master)                  ARTIFACT_PREFIX="synapse-master" ;;
        g3-piecemeal-fixes)      ARTIFACT_PREFIX="synapse-g3-g3" ;;
        phase2-multi-process)    ARTIFACT_PREFIX="synapse-phase2" ;;
        *)                       ARTIFACT_PREFIX="synapse-$(echo $BRANCH | tr / -)" ;;
    esac
    # Find the latest artifact matching the prefix
    ARTIFACT=$(aws s3 ls "s3://$S3_BUCKET/" --region "$REGION" 2>/dev/null \
        | grep "$ARTIFACT_PREFIX" | sort -k1,2 | tail -1 | awk "{print \$4}")
    if [[ -n "$ARTIFACT" ]]; then
        ARTIFACT="s3://$S3_BUCKET/$ARTIFACT"
        echo "Auto-resolved artifact: $ARTIFACT"
    else
        echo "WARNING: No artifact found for prefix $ARTIFACT_PREFIX in s3://$S3_BUCKET/"
        echo "Building from git instead..."
    fi
fi
# ── PHASE 1: PROVISION ────────────────────────────────────────────────
echo ""
echo "── PHASE 1: PROVISION ─────────────────────────────────────"

INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET" \
    --iam-instance-profile "Name=$IAM_PROFILE" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=synapse-test-$$}]" \
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
    if [[ $i -eq 60 ]]; then
        echo "ERROR: SSM agent not online after 300s"
        exit 1
    fi
    sleep 5
done

# ── PHASE 1b: ATTACH SNAPSHOT VOLUME (if --data-snapshot) ─────────────
if [[ -n "$DATA_SNAPSHOT" ]]; then
    echo ""
    echo "── PHASE 1b: SNAPSHOT VOLUME ──────────────────────────────"
    DATADIR="/mnt/cortex-data"

    AZ=$(aws ec2 describe-instances \
        --region "$REGION" \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' --output text)

    SNAP_VOL_ID=$(aws ec2 create-volume \
        --region "$REGION" \
        --availability-zone "$AZ" \
        --snapshot-id "$DATA_SNAPSHOT" \
        --volume-type gp3 \
        --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=synapse-test-snap-$$}]" \
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
fi

# ── PHASE 2: SETUP ────────────────────────────────────────────────────
echo ""
echo "── PHASE 2: SETUP ─────────────────────────────────────────"

echo "Installing Python 3.11 and dependencies..."
run_ssm "sudo dnf install -y python3.11 python3.11-pip python3.11-devel gcc && pip3.11 install --user regex lmdb msgpack xxhash PyYAML aiohttp cryptography pyOpenSSL fastjsonschema"

# ── PHASE 3: TRANSFER CODE ────────────────────────────────────────────
echo ""
echo "── PHASE 3: TRANSFER CODE ─────────────────────────────────"

if [[ -n "$ARTIFACT" ]]; then
    echo "Using pinned artifact: $ARTIFACT"
    run_ssm "mkdir -p /home/ec2-user/synapse && aws s3 cp $ARTIFACT /tmp/synapse-deploy.tar.gz --region $REGION && tar xzf /tmp/synapse-deploy.tar.gz -C /home/ec2-user/synapse"
    run_ssm "cd /home/ec2-user/synapse && pip3.11 install --user -e . 2>\&1 | tail -3"
    run_ssm "python3.11 -c 'import synapse; print(synapse.__file__)'"
else

ORIG_BRANCH=$(git rev-parse --abbrev-ref HEAD)
NEED_CHECKOUT=false

if [[ "$BRANCH" != "$ORIG_BRANCH" ]]; then
    echo "Checking out $BRANCH..."
    git checkout "$BRANCH" --quiet
    NEED_CHECKOUT=true
fi

# Build tarball: synapse code from target branch + scripts/ always from phase2
TARBALL=/tmp/synapse-deploy.tar.gz
STAGE=$(mktemp -d)

# Copy working tree (target branch is already checked out)
rsync -a --exclude=.git --exclude=__pycache__ --exclude=.kiro --exclude='*.pem' . "$STAGE/"

# Overlay test scripts from phase2-multi-process (they only exist on that branch)
if [[ "$BRANCH" != "phase2-multi-process" ]]; then
    mkdir -p "$STAGE/scripts"
    for f in test_pathological.py test_soak.py test_correctness.py \
             test_throughput.py test_recovery.py test_read_after_write.py \
             test_parallel_reads.py test_mixed_load.py; do
        git show "phase2-multi-process:scripts/$f" > "$STAGE/scripts/$f"
    done
fi

tar czf "$TARBALL" -C "$STAGE" .
rm -rf "$STAGE"

if [[ "$NEED_CHECKOUT" == "true" ]]; then
    git checkout "$ORIG_BRANCH" --quiet
fi

echo "Uploading to S3..."
aws s3 cp "$TARBALL" "s3://$S3_BUCKET/$S3_KEY" --region "$REGION" --quiet

echo "Downloading and extracting on instance..."
run_ssm "mkdir -p /home/ec2-user/synapse && aws s3 cp s3://$S3_BUCKET/$S3_KEY /tmp/synapse-deploy.tar.gz --region $REGION && tar xzf /tmp/synapse-deploy.tar.gz -C /home/ec2-user/synapse"

echo "Installing synapse (editable)..."
run_ssm "cd /home/ec2-user/synapse && pip3.11 install --user -e ."

echo "Verifying import path..."
IMPORT_PATH=$(run_ssm "python3.11 -c \"import synapse; print(synapse.__file__)\"")
echo "  synapse.__file__ = $IMPORT_PATH"
if ! echo "$IMPORT_PATH" | grep -q "/home/ec2-user/synapse/"; then
    echo "ERROR: synapse imported from wrong location!"
    exit 1
fi

fi
# ── PHASE 4: START CORTEX ─────────────────────────────────────────────
echo ""
echo "── PHASE 4: START CORTEX ──────────────────────────────────"

echo "Killing any existing cortex..."
if [[ -n "$DATA_SNAPSHOT" ]]; then
    run_ssm "pkill -f 'synapse.servers.cortex' || true; sleep 2"
else
    run_ssm "pkill -f 'synapse.servers.cortex' || true; sleep 2; rm -rf $DATADIR; mkdir -p $DATADIR"
fi

echo "Writing cell.yaml (readers=$READERS)..."
if [[ "$READERS" -gt 0 ]]; then
    run_ssm "printf 'auth:anon: root\nmulti:process:readers: $READERS\n' > $DATADIR/cell.yaml"
else
    run_ssm "printf 'auth:anon: root\n' > $DATADIR/cell.yaml"
fi

echo "Starting cortex..."
run_ssm "nohup setsid python3.11 -m synapse.servers.cortex $DATADIR --telepath tcp://0.0.0.0:27492/ --https 0 </dev/null >/tmp/cortex.log 2>&1 & sleep 2"

# Health check: wait for expected ports
if [[ "$READERS" -eq 0 ]]; then
    EXPECTED_PORTS=1
else
    EXPECTED_PORTS=3
fi

echo "Waiting for $EXPECTED_PORTS port(s) (max 120s)..."
for i in $(seq 1 24); do
    PORT_COUNT=$(run_ssm "ss -tlnp | grep -c '2749[0-9]' || echo 0" | tr -d '[:space:]')
    if [[ "$PORT_COUNT" -ge "$EXPECTED_PORTS" ]]; then
        echo "Cortex ready ($PORT_COUNT ports listening)."
        break
    fi
    if [[ $i -eq 24 ]]; then
        echo "ERROR: Only $PORT_COUNT/$EXPECTED_PORTS ports after 120s"
        echo "Cortex log:"
        run_ssm "tail -50 /tmp/cortex.log" || true
        exit 1
    fi
    sleep 5
done

# Telepath readiness probe: port LISTEN != accepting connections
echo "Verifying telepath connectivity (max 60s)..."
run_ssm "for i in \$(seq 1 30); do python3.11 -c \"
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

# ── PHASE 5: RUN TEST ─────────────────────────────────────────────────
echo ""
echo "── PHASE 5: RUN TEST ($TEST) ──────────────────────────────"

WRITER_URL="tcp://127.0.0.1:27492/cortex"
READER_URL="tcp://127.0.0.1:27493/cortex"
OUTPUT_FLAG="--output /tmp/test-results.json"

case $TEST in
    pathological)
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_pathological.py $WRITER_URL $OUTPUT_FLAG";;
    soak)
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_soak.py $WRITER_URL --duration $DURATION --read-tps 100 --write-tps 30 $OUTPUT_FLAG";;
    correctness)
        READER_FLAG=""
        if [[ "$READERS" -gt 0 ]]; then
            READER_FLAG="--reader $READER_URL"
        fi
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_correctness.py --writer $WRITER_URL $READER_FLAG $OUTPUT_FLAG";;
    throughput)
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_throughput.py $WRITER_URL $OUTPUT_FLAG";;
    recovery)
        READER_PORTS_FLAG=""
        if [[ "$READERS" -gt 0 ]]; then
            READER_PORTS_FLAG="--reader-ports 27493,27494"
        fi
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_recovery.py $WRITER_URL $READER_PORTS_FLAG $OUTPUT_FLAG";;
    read-after-write)
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_read_after_write.py $WRITER_URL $OUTPUT_FLAG";;
    parallel-reads)
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_parallel_reads.py $WRITER_URL --concurrency 64 $OUTPUT_FLAG";;
    mixed-load)
        TEST_CMD="cd /home/ec2-user/synapse && python3.11 scripts/test_mixed_load.py $WRITER_URL --write-pct 10 --duration $DURATION $OUTPUT_FLAG";;
esac

# Soak tests need a longer SSM timeout
SSM_TIMEOUT=$((DURATION + 300))
if [[ $SSM_TIMEOUT -lt 600 ]]; then
    SSM_TIMEOUT=600
fi

echo "Running: $TEST_CMD"
echo "─────────────────────────────────────────────────────────"
set +e
TEST_OUTPUT=$(run_ssm "$TEST_CMD" "$SSM_TIMEOUT")
TEST_EXIT=$?
set -e
echo "$TEST_OUTPUT"
if [[ $TEST_EXIT -ne 0 ]]; then
    echo "WARNING: test exited with status $TEST_EXIT"
fi
echo "─────────────────────────────────────────────────────────"

# Always fetch JSON results (written to file, immune to 24KB SSM stdout cap)
echo ""
RESULTS_JSON=$(run_ssm "cat /tmp/test-results.json 2>/dev/null || echo '{}'" | tr -d '\n')
if [[ "$RESULTS_JSON" != "{}" ]]; then
    mkdir -p results
    echo "$RESULTS_JSON" > "results/${TEST}-results.json"
    echo "Results saved to results/${TEST}-results.json"
else
    echo "WARNING: No results JSON produced"
fi

# Dump cortex log tail for diagnostics on failure
if [[ $TEST_EXIT -ne 0 ]]; then
    echo ""
    echo "-- CORTEX LOG (last 30 lines) -----------------------------"
    run_ssm "tail -30 /tmp/cortex.log" || true
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
if [[ $TEST_EXIT -eq 0 ]]; then
    echo "  PASS — $TEST test finished"
else
    echo "  FAIL — $TEST test exited $TEST_EXIT"
fi
echo "═══════════════════════════════════════════════════════════"
exit $TEST_EXIT

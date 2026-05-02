#!/bin/bash
set -euo pipefail

# Deploy and start multi-process Cortex on a remote EC2 instance via SSM.
# The cortex spawns read-only reader subprocesses internally when
# multi:process:readers is set in cell.yaml.
#
# Usage: bash scripts/start-mp-cortex.sh <instance-id> [ssm|pem-path] [datadir] [reader-pct]

TARGET="$1"
PEM="${2:-ssm}"
DATADIR="${3:-/tmp/cortex-mp}"
READER_PCT="${4:-50}"

REGION="${AWS_REGION:-us-east-1}"
PROFILE="${AWS_PROFILE:-rodrigon-testing}"

run_and_wait() {
    echo "  > $1"
    local cmd_id
    cmd_id=$(aws ssm send-command \
        --region "$REGION" \
        --profile "$PROFILE" \
        --instance-ids "$TARGET" \
        --document-name "AWS-RunShellScript" \
        --parameters "{\"commands\":[\"$1\"],\"executionTimeout\":[\"600\"]}" \
        --query 'Command.CommandId' \
        --output text)
    sleep 2
    aws ssm wait command-executed \
        --region "$REGION" \
        --profile "$PROFILE" \
        --command-id "$cmd_id" \
        --instance-id "$TARGET" 2>/dev/null || true
    local result
    result=$(aws ssm get-command-invocation \
        --region "$REGION" \
        --profile "$PROFILE" \
        --command-id "$cmd_id" \
        --instance-id "$TARGET" \
        --query '[Status,StandardOutputContent,StandardErrorContent]' \
        --output text 2>&1)
    echo "$result"
    if echo "$result" | grep -q "^Failed"; then
        echo "ERROR: Command failed"
        return 1
    fi
}

echo "=== Deploying multi-process Cortex to $TARGET ==="

echo ""
echo "Step 1: Install Python 3.11"
run_and_wait "dnf install -y python3.11 python3.11-pip 2>&1 | tail -3"

echo ""
echo "Step 2: Install synapse"
run_and_wait "python3.11 -m pip install synapse 2>&1 | tail -5"

echo ""
echo "Step 3: Prepare data directory and config"
run_and_wait "mkdir -p $DATADIR && python3.11 -c \"
import json, pathlib
conf = {'auth:anon': 'root', 'multi:process:readers': $READER_PCT}
pathlib.Path('$DATADIR/cell.yaml').write_text(json.dumps(conf))
print('Config written:', conf)
\""

echo ""
echo "Step 4: Kill any existing cortex and start writer"
run_and_wait "pkill -f 'synapse.servers.cortex' 2>/dev/null || true"
sleep 2
run_and_wait "cd / && nohup python3.11 -m synapse.servers.cortex $DATADIR \
  --telepath tcp://0.0.0.0:27492/ \
  --https 0 \
  --auth-anon root \
  > /tmp/cortex-writer.log 2>&1 & echo started-pid=\$!"

echo ""
echo "Step 5: Wait for ports"
sleep 10
run_and_wait "ss -tlnp | grep -E '2749[0-9]' || echo 'ports not yet listening'"

echo ""
echo "=== Deployment complete ==="
echo "Writer: tcp://$TARGET:27492/cortex"
echo "Readers are spawned internally by the cortex (multi:process:readers=$READER_PCT%)"

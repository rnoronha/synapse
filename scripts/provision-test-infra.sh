#!/bin/bash
set -euo pipefail

# Provision an EC2 instance for synapse multi-process testing.
# Usage: AWS_PROFILE=rodrigon-testing bash scripts/provision-test-infra.sh --name synapse-g9b

NAME=""
INSTANCE_TYPE="c5.4xlarge"
REGION="us-east-1"
KEY_NAME="synapse-perf-test"
SG_ID="sg-0a29a79adae40d6d9"
SUBNET="subnet-0ba72e38d87dc5061"
AMI="ami-0e1e769742d1cfb49"

while [[ $# -gt 0 ]]; do
    case $1 in
        --name) NAME="$2"; shift 2;;
        --type) INSTANCE_TYPE="$2"; shift 2;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

if [[ -z "$NAME" ]]; then
    echo "Usage: $0 --name <instance-name>"
    exit 1
fi

echo "Launching $INSTANCE_TYPE instance: $NAME"

INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --image-id "$AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
    --query 'Instances[0].InstanceId' \
    --output text)

echo "Instance: $INSTANCE_ID"
echo "Waiting for running state..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

PUBLIC_IP=$(aws ec2 describe-instances \
    --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text)

echo "Public IP: $PUBLIC_IP"
echo "Instance ID: $INSTANCE_ID"
echo ""
echo "Connect via SSM: aws ssm start-session --target $INSTANCE_ID --region $REGION --profile ${AWS_PROFILE:-default}"
echo "IP=$PUBLIC_IP"

#!/usr/bin/env bash
# deploy/deploy_aws.sh
#
# End-to-end idempotent deployment of the SEC EDGAR Bronze ingest pipeline on AWS.
#
# Prerequisites:
#   1. Run deploy/create_deployer.sh once as an AWS admin to create the
#      sec-loader-deployer IAM user and get its access keys.
#   2. Configure AWS CLI:  aws configure --profile sec-edgar
#   3. Fill in the Config block below (VPC_ID and VPC_SUBNET_IDS at minimum).
#   4. Docker must be running locally (for the image build + push step).
#
# Usage:
#   AWS_PROFILE=sec-edgar bash deploy/deploy_aws.sh
#
# Idempotent: safe to run multiple times. Re-running after a failed step will
# pick up from where it left off (most AWS create-* calls use --no-fail-if-exists
# or check for existing resources).
#
# After deployment, trigger a smoke test:
#   aws stepfunctions start-execution \
#     --state-machine-arn <ARN printed at end> \
#     --input '{"ingestDate":"YYYY-MM-DD","fullRefresh":false}'
#
# Weekly full refresh (all ~5k NYSE/Nasdaq CIKs — run each Sunday):
#   aws stepfunctions start-execution \
#     --state-machine-arn <ARN> \
#     --input '{"ingestDate":"YYYY-MM-DD","fullRefresh":true}'

set -euo pipefail

# ┌──────────────────────────────────────────────────────────────────────────┐
# │  CONFIG — edit these values before running                               │
# └──────────────────────────────────────────────────────────────────────────┘
AWS_BUCKET="my-sec-edgar-bucket"        # S3 bucket name (created if absent)
AWS_REGION="us-east-1"
ECR_REPO="sec-edgar-ingest"
ECS_CLUSTER="sec-edgar-cluster"
STEP_FUNCTIONS_NAME="sec-edgar-bronze-ingest"
TASK_ROLE_NAME="sec-edgar-ecs-task-role"
EXECUTION_ROLE_NAME="sec-edgar-ecs-execution-role"
SF_ROLE_NAME="sec-edgar-stepfunctions-role"
SCHEDULER_ROLE_NAME="sec-edgar-scheduler-role"

VPC_ID="vpc-xxxxxxxx"                    # ← YOUR default VPC ID
VPC_SUBNET_IDS="subnet-xx,subnet-yy"   # ← Two public subnets in different AZs

SNS_ALERT_EMAIL="paul.ananth@yahoo.com"
SEC_USER_AGENT="SEC EDGAR Pipeline paul.ananth@yahoo.com"
# ┌──────────────────────────────────────────────────────────────────────────┘

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
POLICY_DIR="$SCRIPT_DIR/iam_policies"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
fail() { echo "[$(date '+%H:%M:%S')] ✗ $*"; exit 1; }

# ── Step 1: Auth check ────────────────────────────────────────────────────────
log "=== Step 1/14: Verifying AWS credentials ==="
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CALLER_ARN=$(aws sts get-caller-identity --query Arn --output text)
ok "Account: $ACCOUNT_ID  |  Caller: $CALLER_ARN"

# ── Step 2: Local deps for pre-flight ─────────────────────────────────────────
log "=== Step 2/14: Installing local Python deps (s3fs) ==="
cd "$REPO_ROOT"
uv sync --group aws --quiet
ok "Local deps ready"

# ── Step 3: S3 bucket ─────────────────────────────────────────────────────────
log "=== Step 3/14: Creating S3 bucket: s3://$AWS_BUCKET ==="
if aws s3api head-bucket --bucket "$AWS_BUCKET" 2>/dev/null; then
    ok "Bucket already exists"
else
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$AWS_BUCKET" --region "$AWS_REGION"
    else
        aws s3api create-bucket --bucket "$AWS_BUCKET" --region "$AWS_REGION" \
            --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
    ok "Bucket created"
fi
aws s3api put-bucket-versioning --bucket "$AWS_BUCKET" \
    --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket "$AWS_BUCKET" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
aws s3api put-bucket-encryption --bucket "$AWS_BUCKET" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
ok "Bucket versioning + encryption + public-access-block configured"

# ── Step 4: S3 VPC Gateway Endpoint ──────────────────────────────────────────
log "=== Step 4/14: Creating S3 VPC Gateway Endpoint (free) ==="
EXISTING_EP=$(aws ec2 describe-vpc-endpoints \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=service-name,Values=com.amazonaws.$AWS_REGION.s3" \
              "Name=vpc-endpoint-type,Values=Gateway" \
    --query "VpcEndpoints[0].VpcEndpointId" --output text 2>/dev/null || echo "None")
if [ "$EXISTING_EP" = "None" ] || [ -z "$EXISTING_EP" ]; then
    # Get all route tables in the VPC
    RT_IDS=$(aws ec2 describe-route-tables \
        --filters "Name=vpc-id,Values=$VPC_ID" \
        --query "RouteTables[*].RouteTableId" --output text | tr '\t' ',')
    aws ec2 create-vpc-endpoint \
        --vpc-id "$VPC_ID" \
        --service-name "com.amazonaws.$AWS_REGION.s3" \
        --vpc-endpoint-type Gateway \
        --route-table-ids $RT_IDS \
        --query "VpcEndpoint.VpcEndpointId" --output text
    ok "S3 VPC Gateway Endpoint created"
else
    ok "S3 VPC Gateway Endpoint already exists: $EXISTING_EP"
fi

# ── Step 5: Security group ────────────────────────────────────────────────────
log "=== Step 5/14: Creating ECS security group ==="
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=sec-edgar-ecs-sg" \
    --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || echo "None")
if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
    SG_ID=$(aws ec2 create-security-group \
        --group-name "sec-edgar-ecs-sg" \
        --description "SEC EDGAR ECS tasks — egress 443 only, no ingress" \
        --vpc-id "$VPC_ID" \
        --query "GroupId" --output text)
    # Remove default allow-all egress, add HTTPS-only egress
    aws ec2 revoke-security-group-egress --group-id "$SG_ID" \
        --ip-permissions '[{"IpProtocol":"-1","IpRanges":[{"CidrIp":"0.0.0.0/0"}]}]' 2>/dev/null || true
    aws ec2 authorize-security-group-egress --group-id "$SG_ID" \
        --ip-permissions '[{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"IpRanges":[{"CidrIp":"0.0.0.0/0"}]}]'
    aws ec2 create-tags --resources "$SG_ID" \
        --tags "Key=Name,Value=sec-edgar-ecs-sg"
    ok "Security group created: $SG_ID"
else
    ok "Security group already exists: $SG_ID"
fi

# ── Step 6: ECR repository ────────────────────────────────────────────────────
log "=== Step 6/14: Creating ECR repository: $ECR_REPO ==="
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"
if aws ecr describe-repositories --repository-names "$ECR_REPO" \
        --region "$AWS_REGION" > /dev/null 2>&1; then
    ok "ECR repo already exists: $ECR_URI"
else
    aws ecr create-repository --repository-name "$ECR_REPO" --region "$AWS_REGION" \
        --image-scanning-configuration scanOnPush=true \
        --query "repository.repositoryUri" --output text
    ok "ECR repo created: $ECR_URI"
fi
aws ecr put-lifecycle-policy --repository-name "$ECR_REPO" --region "$AWS_REGION" \
    --lifecycle-policy-text \
    '{"rules":[{"rulePriority":1,"description":"Keep last 10 images","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}}]}' \
    > /dev/null
ok "ECR lifecycle policy set (keep last 10 images)"

# ── Step 7: IAM roles ─────────────────────────────────────────────────────────
log "=== Step 7/14: Creating IAM roles ==="

_create_role() {
    local role_name="$1"
    local trust_service="$2"
    local policy_file="$3"

    local trust_doc
    trust_doc=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "${trust_service}"},
    "Action": "sts:AssumeRole"
  }]
}
EOF
)
    if aws iam get-role --role-name "$role_name" > /dev/null 2>&1; then
        log "  Role already exists: $role_name — updating policy"
    else
        aws iam create-role --role-name "$role_name" \
            --assume-role-policy-document "$trust_doc" \
            --description "SEC EDGAR ingest pipeline role" > /dev/null
        log "  Created role: $role_name"
    fi

    # Substitute placeholders in policy file
    local policy
    policy=$(sed \
        -e "s|BUCKET|${AWS_BUCKET}|g" \
        -e "s|REGION|${AWS_REGION}|g" \
        -e "s|ACCOUNT|${ACCOUNT_ID}|g" \
        -e "s|CLUSTER|${ECS_CLUSTER}|g" \
        -e "s|STATE_MACHINE|${STEP_FUNCTIONS_NAME}|g" \
        "$policy_file")
    aws iam put-role-policy --role-name "$role_name" \
        --policy-name "sec-edgar-inline-policy" \
        --policy-document "$policy"
    ok "  Role ready: $role_name"
}

_create_role "$TASK_ROLE_NAME"      "ecs-tasks.amazonaws.com"        "$POLICY_DIR/task_role_policy.json"
_create_role "$EXECUTION_ROLE_NAME" "ecs-tasks.amazonaws.com"        "$POLICY_DIR/execution_role_policy.json"
_create_role "$SF_ROLE_NAME"        "states.amazonaws.com"           "$POLICY_DIR/stepfunctions_role_policy.json"
_create_role "$SCHEDULER_ROLE_NAME" "scheduler.amazonaws.com"        "$POLICY_DIR/scheduler_role_policy.json"

# ── Step 8: CloudWatch log groups ─────────────────────────────────────────────
log "=== Step 8/14: Creating CloudWatch log groups ==="
for task_num in 01 02 03 04; do
    LG="/ecs/sec-edgar-ingest-${task_num}"
    aws logs create-log-group --log-group-name "$LG" --region "$AWS_REGION" 2>/dev/null || true
    aws logs put-retention-policy --log-group-name "$LG" --retention-in-days 30 \
        --region "$AWS_REGION" 2>/dev/null || true
    log "  $LG (30-day retention)"
done
ok "Log groups ready"

# ── Step 9: SNS alert topic ───────────────────────────────────────────────────
log "=== Step 9/14: Creating SNS alert topic ==="
SNS_ARN=$(aws sns create-topic --name "sec-edgar-pipeline-alerts" \
    --region "$AWS_REGION" --query TopicArn --output text)
aws sns subscribe --topic-arn "$SNS_ARN" --protocol email \
    --notification-endpoint "$SNS_ALERT_EMAIL" --region "$AWS_REGION" > /dev/null
ok "SNS topic: $SNS_ARN  (check $SNS_ALERT_EMAIL for subscription confirmation)"

# CloudWatch alarm: Step Functions execution failures
aws cloudwatch put-metric-alarm \
    --alarm-name "sec-edgar-pipeline-failure" \
    --alarm-description "SEC EDGAR Bronze ingest failed" \
    --namespace "AWS/States" \
    --metric-name "ExecutionsFailed" \
    --dimensions "Name=StateMachineArn,Value=arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STEP_FUNCTIONS_NAME}" \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --evaluation-periods 1 \
    --alarm-actions "$SNS_ARN" \
    --treat-missing-data notBreaching \
    --region "$AWS_REGION"
ok "CloudWatch alarm: sec-edgar-pipeline-failure → $SNS_ARN"

# ── Step 10: Docker build + push ──────────────────────────────────────────────
log "=== Step 10/14: Building and pushing Docker image ==="
aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker build --platform linux/amd64 -t "${ECR_URI}:latest" "$REPO_ROOT"
docker push "${ECR_URI}:latest"
IMAGE_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "${ECR_URI}:latest" 2>/dev/null \
    || echo "${ECR_URI}:latest")
ok "Image pushed: $IMAGE_DIGEST"

# ── Step 11: Pre-flight validation ────────────────────────────────────────────
log "=== Step 11/14: Running pre-flight permission validation ==="
cd "$REPO_ROOT"
CLOUD_PROVIDER=aws AWS_BUCKET="$AWS_BUCKET" AWS_DEFAULT_REGION="$AWS_REGION" \
    SEC_USER_AGENT="$SEC_USER_AGENT" \
    uv run python scripts/validate_azure_permissions.py --cloud aws
ok "Pre-flight validation passed"

# ── Step 12: ECS cluster + task definitions ───────────────────────────────────
log "=== Step 12/14: Creating ECS cluster and task definitions ==="
aws ecs create-cluster --cluster-name "$ECS_CLUSTER" \
    --capacity-providers FARGATE \
    --region "$AWS_REGION" > /dev/null 2>&1 || true
ok "ECS cluster: $ECS_CLUSTER"

TASK_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${TASK_ROLE_NAME}"
EXEC_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${EXECUTION_ROLE_NAME}"

_register_task() {
    local task_name="$1"
    local script="$2"
    local cpu="$3"
    local memory="$4"
    local log_suffix="$5"

    # Environment common to all tasks
    local env_vars
    env_vars=$(cat <<ENVEOF
[
  {"name":"CLOUD_PROVIDER","value":"aws"},
  {"name":"AWS_BUCKET","value":"${AWS_BUCKET}"},
  {"name":"AWS_DEFAULT_REGION","value":"${AWS_REGION}"},
  {"name":"SEC_USER_AGENT","value":"${SEC_USER_AGENT}"}
]
ENVEOF
)

    local container_def
    container_def=$(cat <<CONTDEF
[{
  "name": "ingest",
  "image": "${ECR_URI}:latest",
  "command": ["python", "${script}"],
  "environment": ${env_vars},
  "logConfiguration": {
    "logDriver": "awslogs",
    "options": {
      "awslogs-group": "/ecs/sec-edgar-ingest-${log_suffix}",
      "awslogs-region": "${AWS_REGION}",
      "awslogs-stream-prefix": "ecs"
    }
  }
}]
CONTDEF
)

    aws ecs register-task-definition \
        --family "$task_name" \
        --network-mode awsvpc \
        --requires-compatibilities FARGATE \
        --cpu "$cpu" \
        --memory "$memory" \
        --task-role-arn "$TASK_ROLE_ARN" \
        --execution-role-arn "$EXEC_ROLE_ARN" \
        --container-definitions "$container_def" \
        --region "$AWS_REGION" \
        --query "taskDefinition.taskDefinitionArn" --output text
    ok "  Task def: $task_name (${cpu}vCPU / ${memory}MB)"
}

TASK01_ARN=$(_register_task "sec-edgar-ingest-01" "scripts/ingest/01_ingest_tickers_exchange.py" 1024 2048 "01")
TASK02_ARN=$(_register_task "sec-edgar-ingest-02" "scripts/ingest/02_ingest_daily_index.py"      1024 2048 "02")
TASK03_ARN=$(_register_task "sec-edgar-ingest-03" "scripts/ingest/03_ingest_submissions.py"      2048 4096 "03")
TASK04_ARN=$(_register_task "sec-edgar-ingest-04" "scripts/ingest/04_ingest_companyfacts.py"     2048 4096 "04")

# Extract task def families (Step Functions uses family:revision or just family)
TASK01_FAM="sec-edgar-ingest-01"
TASK02_FAM="sec-edgar-ingest-02"
TASK03_FAM="sec-edgar-ingest-03"
TASK04_FAM="sec-edgar-ingest-04"

# ── Step 13: Step Functions state machine ─────────────────────────────────────
log "=== Step 13/14: Creating Step Functions state machine ==="

# Substitute runtime values into the state machine template
SF_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SF_ROLE_NAME}"
CLUSTER_ARN="arn:aws:ecs:${AWS_REGION}:${ACCOUNT_ID}:cluster/${ECS_CLUSTER}"

SF_DEF=$(sed \
    -e "s|ACCOUNT_ID|${ACCOUNT_ID}|g" \
    -e "s|AWS_REGION|${AWS_REGION}|g" \
    -e "s|ECS_CLUSTER_ARN|${CLUSTER_ARN}|g" \
    -e "s|TASK01_FAM|${TASK01_FAM}|g" \
    -e "s|TASK02_FAM|${TASK02_FAM}|g" \
    -e "s|TASK03_FAM|${TASK03_FAM}|g" \
    -e "s|TASK04_FAM|${TASK04_FAM}|g" \
    -e "s|SG_ID|${SG_ID}|g" \
    -e "s|SUBNET_IDS|${VPC_SUBNET_IDS}|g" \
    -e "s|TASK_ROLE_ARN|${TASK_ROLE_ARN}|g" \
    -e "s|EXEC_ROLE_ARN|${EXEC_ROLE_ARN}|g" \
    -e "s|SEC_USER_AGENT_VALUE|${SEC_USER_AGENT}|g" \
    -e "s|AWS_BUCKET_VALUE|${AWS_BUCKET}|g" \
    "$REPO_ROOT/workflows/stepfunctions_state_machine.json")

EXISTING_SF=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?name=='${STEP_FUNCTIONS_NAME}'].stateMachineArn" \
    --output text --region "$AWS_REGION" 2>/dev/null || echo "")

if [ -n "$EXISTING_SF" ]; then
    aws stepfunctions update-state-machine \
        --state-machine-arn "$EXISTING_SF" \
        --definition "$SF_DEF" \
        --role-arn "$SF_ROLE_ARN" \
        --region "$AWS_REGION" > /dev/null
    SF_ARN="$EXISTING_SF"
    ok "State machine updated: $SF_ARN"
else
    SF_ARN=$(aws stepfunctions create-state-machine \
        --name "$STEP_FUNCTIONS_NAME" \
        --definition "$SF_DEF" \
        --role-arn "$SF_ROLE_ARN" \
        --type STANDARD \
        --region "$AWS_REGION" \
        --query stateMachineArn --output text)
    ok "State machine created: $SF_ARN"
fi

# ── Step 14: EventBridge Scheduler ───────────────────────────────────────────
log "=== Step 14/14: Creating EventBridge daily scheduler ==="
SCHEDULER_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SCHEDULER_ROLE_NAME}"

SCHED_TARGET=$(cat <<EOF
{
  "Arn": "${SF_ARN}",
  "RoleArn": "${SCHEDULER_ROLE_ARN}",
  "Input": "{\"ingestDate\": \"<aws.events.event.time | slice(0,10) >\",\"fullRefresh\": false}"
}
EOF
)

# EventBridge Scheduler (different from EventBridge Rules — uses aws scheduler CLI)
EXISTING_SCHED=$(aws scheduler get-schedule --name "sec-edgar-daily-ingest" \
    --region "$AWS_REGION" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('Arn',''))" 2>/dev/null || echo "")

if [ -n "$EXISTING_SCHED" ]; then
    aws scheduler update-schedule \
        --name "sec-edgar-daily-ingest" \
        --schedule-expression "cron(0 6 * * ? *)" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "{\"Arn\":\"${SF_ARN}\",\"RoleArn\":\"${SCHEDULER_ROLE_ARN}\",\"Input\":\"{\\\"ingestDate\\\":\\\"<aws.events.event.time>\\\",\\\"fullRefresh\\\":false}\"}" \
        --region "$AWS_REGION" > /dev/null
    ok "Scheduler updated: sec-edgar-daily-ingest (06:00 UTC daily)"
else
    aws scheduler create-schedule \
        --name "sec-edgar-daily-ingest" \
        --schedule-expression "cron(0 6 * * ? *)" \
        --flexible-time-window '{"Mode":"OFF"}' \
        --target "{\"Arn\":\"${SF_ARN}\",\"RoleArn\":\"${SCHEDULER_ROLE_ARN}\",\"Input\":\"{\\\"ingestDate\\\":\\\"<aws.events.event.time>\\\",\\\"fullRefresh\\\":false}\"}" \
        --region "$AWS_REGION" > /dev/null
    ok "Scheduler created: sec-edgar-daily-ingest (06:00 UTC daily)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Deployment complete!"
echo "  State machine ARN: $SF_ARN"
echo "  S3 bucket:         s3://$AWS_BUCKET/sec-edgar/bronze/"
echo "  ECR image:         $ECR_URI:latest"
echo ""
echo "  Smoke test (incremental, today):"
echo "    aws stepfunctions start-execution \\"
echo "      --state-machine-arn $SF_ARN \\"
echo "      --input '{\"ingestDate\":\"$(date +%Y-%m-%d)\",\"fullRefresh\":false}'"
echo ""
echo "  Weekly full refresh (Sundays — all ~5k CIKs):"
echo "    aws stepfunctions start-execution \\"
echo "      --state-machine-arn $SF_ARN \\"
echo "      --input '{\"ingestDate\":\"$(date +%Y-%m-%d)\",\"fullRefresh\":true}'"
echo ""
echo "  Check $SNS_ALERT_EMAIL — confirm SNS subscription to receive failure alerts."
echo "============================================================"

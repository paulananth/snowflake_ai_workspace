#!/usr/bin/env bash
# terraform/aws/deploy.sh
#
# Apply Terraform to create / update all SEC EDGAR Bronze AWS infrastructure.
#
# Prerequisites:
#   1. Terraform >= 1.5  (https://developer.hashicorp.com/terraform/install)
#   2. AWS credentials configured:
#        aws configure --profile sec-edgar
#      OR export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
#   3. Docker must be running locally — the script prints the push commands
#      after apply so you can build and push the container image.
#
# Usage:
#   cd terraform/aws
#   bash deploy.sh                       # uses default AWS profile
#   AWS_PROFILE=sec-edgar bash deploy.sh
#
# Migrating from deploy_aws.sh (existing infra)?
#   If resources already exist, import them before applying:
#     terraform import aws_s3_bucket.bronze              paulananth11-sec-edgar-bronze
#     terraform import aws_ecr_repository.ingest         sec-edgar-ingest
#     terraform import aws_ecs_cluster.main              sec-edgar-cluster
#     terraform import aws_iam_role.ecs_task             sec-edgar-ecs-task-role
#     terraform import aws_iam_role.ecs_execution        sec-edgar-ecs-execution-role
#     terraform import aws_iam_role.step_functions       sec-edgar-stepfunctions-role
#     terraform import aws_iam_role.scheduler            sec-edgar-scheduler-role
#     terraform import aws_sfn_state_machine.pipeline    <state-machine-arn>
#     terraform import aws_scheduler_schedule.daily_ingest default/sec-edgar-daily-ingest
#   Then run this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] [OK] $*"; }
fail() { echo "[$(date '+%H:%M:%S')] [FAIL] $*" >&2; exit 1; }

cd "$SCRIPT_DIR"

# --- 1. Verify AWS credentials -----------------------------------------------
log "Verifying AWS credentials..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) \
    || fail "AWS credentials not configured. Run: aws configure --profile sec-edgar"
CALLER_ARN=$(aws sts get-caller-identity --query Arn --output text)
ok "Account: $ACCOUNT_ID  |  Caller: $CALLER_ARN"

# --- 2. Terraform init -------------------------------------------------------
log "=== terraform init ==="
terraform init

# --- 3. Terraform validate ---------------------------------------------------
log "=== terraform validate ==="
terraform validate
ok "Configuration valid"

# --- 4. Terraform plan -------------------------------------------------------
log "=== terraform plan ==="
terraform plan -out=tfplan

# --- 5. Confirm before apply -------------------------------------------------
echo ""
read -r -p "Apply the plan above? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { log "Aborted — no changes made."; rm -f tfplan; exit 0; }

# --- 6. Terraform apply ------------------------------------------------------
log "=== terraform apply ==="
terraform apply tfplan
rm -f tfplan
ok "Infrastructure deployed successfully."

# --- 7. Print post-apply instructions ----------------------------------------
ECR_URL=$(terraform output -raw ecr_repository_url 2>/dev/null || echo "")
SF_ARN=$(terraform output -raw state_machine_arn   2>/dev/null || echo "")
REGION=$(terraform output -raw aws_region          2>/dev/null || echo "us-east-1")
ACCOUNT=$(echo "$ECR_URL" | cut -d'.' -f1)

echo ""
echo "============================================================"
echo "  Deployment complete!"
echo ""
echo "  NEXT STEP: push the Docker image to ECR"
echo "  (run from the repo root — requires Docker)"
echo ""
echo "    aws ecr get-login-password --region $REGION | \\"
echo "      docker login --username AWS --password-stdin ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
echo "    docker build --platform linux/amd64 -t ${ECR_URL}:latest ../../"
echo "    docker push ${ECR_URL}:latest"
echo ""
echo "  Smoke test (incremental, today):"
echo "    aws stepfunctions start-execution \\"
echo "      --state-machine-arn $SF_ARN \\"
echo "      --input '{\"ingestDate\":\"$(date +%Y-%m-%d)\",\"fullRefresh\":false}'"
echo ""
echo "  Full refresh (all ~5k CIKs — run Sundays):"
echo "    aws stepfunctions start-execution \\"
echo "      --state-machine-arn $SF_ARN \\"
echo "      --input '{\"ingestDate\":\"$(date +%Y-%m-%d)\",\"fullRefresh\":true}'"
echo ""
echo "  Check $(terraform output -raw sns_topic_arn 2>/dev/null || echo 'SNS topic') —"
echo "  confirm the email subscription to receive failure alerts."
echo "============================================================"

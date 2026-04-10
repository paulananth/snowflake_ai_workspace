#!/usr/bin/env bash
# terraform/aws/destroy.sh
#
# COMPLETELY DESTROY all SEC EDGAR Bronze AWS infrastructure.
#
# WARNING — this is IRREVERSIBLE:
#   - All Parquet data in S3 (including versioned objects) is deleted.
#   - All Docker images in ECR are deleted.
#   - All logs, task definitions, IAM roles, schedulers, and alarms are removed.
#
# The script pre-empties S3 and ECR before calling terraform destroy because
# Terraform cannot delete non-empty buckets / repositories.
#
# Usage:
#   cd terraform/aws
#   bash destroy.sh
#   AWS_PROFILE=sec-edgar bash destroy.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
warn() { echo "[$(date '+%H:%M:%S')] [WARN] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] [OK] $*"; }
fail() { echo "[$(date '+%H:%M:%S')] [FAIL] $*" >&2; exit 1; }

cd "$SCRIPT_DIR"

# --- Guard: confirm Terraform state exists -----------------------------------
if [ ! -f terraform.tfstate ] && [ ! -f .terraform/terraform.tfstate ]; then
    # Check for remote state too — if using S3 backend, tfstate won't be local
    log "No local terraform.tfstate found. If using a remote backend, make sure"
    log "terraform init has been run so state can be fetched."
fi

# --- Verify AWS credentials --------------------------------------------------
log "Verifying AWS credentials..."
aws sts get-caller-identity > /dev/null 2>&1 \
    || fail "AWS credentials not configured. Run: aws configure"

# --- Stern warning -----------------------------------------------------------
echo ""
echo "============================================================"
echo "  !!!  DESTRUCTIVE OPERATION: FULL INFRASTRUCTURE DESTROY  !!!"
echo ""
echo "  The following AWS resources will be PERMANENTLY deleted:"
echo "    S3 bucket + ALL stored Parquet data (versioned)"
echo "    ECR repository + all Docker images"
echo "    ECS cluster + 4 task definitions"
echo "    Step Functions state machine"
echo "    EventBridge Scheduler (daily trigger)"
echo "    4 x IAM roles + inline policies"
echo "    4 x CloudWatch log groups + all log data"
echo "    SNS topic + email subscription"
echo "    VPC S3 Gateway Endpoint"
echo "    CloudWatch alarm"
echo "============================================================"
echo ""
read -r -p "Type exactly 'destroy' to confirm: " confirm
[[ "$confirm" == "destroy" ]] || { log "Aborted — nothing was deleted."; exit 0; }

# --- Step 1: Empty the S3 bucket (versioned) ---------------------------------
BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null || echo "")
REGION=$(terraform output -raw aws_region     2>/dev/null || echo "us-east-1")

if [ -n "$BUCKET" ]; then
    warn "Step 1/3: Emptying S3 bucket: s3://$BUCKET (including all versions)"

    # Delete current objects
    aws s3 rm "s3://$BUCKET" --recursive --quiet 2>/dev/null \
        || warn "  Could not remove objects (may already be empty)"

    # Delete all object versions
    python3 - <<PYEOF
import json, subprocess, sys

bucket = "$BUCKET"
region = "$REGION"

def batch_delete(bucket, region, object_list):
    if not object_list:
        return
    # AWS allows max 1000 objects per delete-objects call
    for i in range(0, len(object_list), 1000):
        chunk = object_list[i:i+1000]
        subprocess.run([
            "aws", "s3api", "delete-objects",
            "--bucket", bucket,
            "--region", region,
            "--delete", json.dumps({"Objects": chunk, "Quiet": True}),
        ], check=False, capture_output=True)

# Delete non-current versions
result = subprocess.run([
    "aws", "s3api", "list-object-versions",
    "--bucket", bucket, "--region", region,
    "--query", "{Versions: Versions[].{Key:Key,VersionId:VersionId}}",
    "--output", "json",
], capture_output=True, text=True)
if result.returncode == 0:
    data = json.loads(result.stdout or "{}")
    batch_delete(bucket, region, data.get("Versions") or [])

# Delete delete-markers
result = subprocess.run([
    "aws", "s3api", "list-object-versions",
    "--bucket", bucket, "--region", region,
    "--query", "{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}",
    "--output", "json",
], capture_output=True, text=True)
if result.returncode == 0:
    data = json.loads(result.stdout or "{}")
    batch_delete(bucket, region, data.get("Objects") or [])

print("  Bucket emptied.")
PYEOF
    ok "S3 bucket emptied: s3://$BUCKET"
else
    warn "Could not read bucket name from Terraform state — skipping S3 cleanup."
    warn "If the bucket has objects, terraform destroy will fail."
fi

# --- Step 2: Delete all ECR images -------------------------------------------
ECR_REPO=$(terraform output -raw ecr_repository_name 2>/dev/null || echo "")

if [ -n "$ECR_REPO" ]; then
    warn "Step 2/3: Deleting all images from ECR: $ECR_REPO"
    python3 - <<PYEOF
import json, subprocess

repo   = "$ECR_REPO"
region = "$REGION"

result = subprocess.run([
    "aws", "ecr", "list-images",
    "--repository-name", repo, "--region", region,
    "--query", "imageIds[*]", "--output", "json",
], capture_output=True, text=True)

if result.returncode == 0:
    image_ids = json.loads(result.stdout or "[]")
    if image_ids:
        subprocess.run([
            "aws", "ecr", "batch-delete-image",
            "--repository-name", repo, "--region", region,
            "--image-ids", json.dumps(image_ids),
        ], check=False, capture_output=True)
        print(f"  Deleted {len(image_ids)} image(s).")
    else:
        print("  No images found.")
PYEOF
    ok "ECR images deleted: $ECR_REPO"
else
    warn "Could not read ECR repo name from Terraform state — skipping image cleanup."
fi

# --- Step 3: Terraform plan + destroy ----------------------------------------
log "Step 3/3: Running terraform destroy..."
terraform plan -destroy -out=tfplan-destroy

echo ""
read -r -p "Execute the destroy plan above? [y/N] " confirm2
if [[ ! "$confirm2" =~ ^[Yy]$ ]]; then
    log "Aborted — plan saved to tfplan-destroy but not applied."
    exit 0
fi

terraform apply tfplan-destroy
rm -f tfplan-destroy

echo ""
ok "All SEC EDGAR Bronze AWS infrastructure has been destroyed."

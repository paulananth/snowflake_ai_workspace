#!/usr/bin/env bash
# deploy/create_deployer.sh
#
# Creates the sec-loader-deployer IAM user with least-privilege permissions
# needed to run deploy/deploy_aws.sh.
#
# Run ONCE as an AWS root user or an IAM user with AdministratorAccess.
# Prints access keys to stdout — save them, they are shown only once.
#
# Usage:
#   bash deploy/create_deployer.sh
#
# After running, configure AWS CLI:
#   aws configure --profile sec-edgar
#   (enter the KeyId + Secret printed below, region us-east-1, output json)
#
# Then deploy:
#   AWS_PROFILE=sec-edgar bash deploy/deploy_aws.sh

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
DEPLOYER_USER="sec-loader-deployer"
POLICY_NAME="sec-edgar-deploy-policy"
BUCKET="paulananth11-sec-edgar-bronze"
AWS_REGION="us-east-1"
# ─────────────────────────────────────────────────────────────────────────────

echo "============================================================"
echo "  SEC EDGAR Pipeline — Create Deployer IAM User"
echo "  User   : $DEPLOYER_USER"
echo "  Bucket : $BUCKET"
echo "  Region : $AWS_REGION"
echo "============================================================"
echo ""

# Verify caller has admin-level access
echo "[1/3] Verifying caller identity..."
CALLER=$(aws sts get-caller-identity --output json)
echo "  Caller: $(echo "$CALLER" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Arn'])")"

# Create user (idempotent: ignore AlreadyExists)
echo ""
echo "[2/3] Creating IAM user: $DEPLOYER_USER"
if aws iam get-user --user-name "$DEPLOYER_USER" > /dev/null 2>&1; then
    echo "  [already exists] — updating policy"
else
    aws iam create-user --user-name "$DEPLOYER_USER" --output text > /dev/null
    echo "  [created]"
fi

# Inline policy — least-privilege for deploy_aws.sh
POLICY_DOC=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IAMRolesForPipeline",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole", "iam:GetRole", "iam:DeleteRole",
        "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy",
        "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
        "iam:PassRole"
      ],
      "Resource": "arn:aws:iam::*:role/sec-edgar-*"
    },
    {
      "Sid": "ECRRepoManage",
      "Effect": "Allow",
      "Action": [
        "ecr:CreateRepository", "ecr:DescribeRepositories",
        "ecr:PutLifecyclePolicy", "ecr:SetRepositoryPolicy",
        "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
        "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"
      ],
      "Resource": "arn:aws:ecr:*:*:repository/sec-edgar-*"
    },
    {
      "Sid": "ECRAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "ECS",
      "Effect": "Allow",
      "Action": [
        "ecs:CreateCluster", "ecs:DescribeClusters",
        "ecs:RegisterTaskDefinition", "ecs:DeregisterTaskDefinition",
        "ecs:DescribeTaskDefinition", "ecs:ListTaskDefinitions"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3BucketSetup",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket", "s3:GetBucketLocation",
        "s3:PutBucketVersioning", "s3:GetBucketVersioning",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration", "s3:GetEncryptionConfiguration",
        "s3:PutBucketPolicy", "s3:GetBucketPolicy",
        "s3:ListBucket",
        "s3:PutObject", "s3:GetObject", "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET}",
        "arn:aws:s3:::${BUCKET}/*"
      ]
    },
    {
      "Sid": "StepFunctions",
      "Effect": "Allow",
      "Action": [
        "states:CreateStateMachine", "states:UpdateStateMachine",
        "states:DescribeStateMachine", "states:StartExecution",
        "states:ListExecutions", "states:DescribeExecution"
      ],
      "Resource": "arn:aws:states:*:*:stateMachine:sec-edgar-*"
    },
    {
      "Sid": "EventBridgeScheduler",
      "Effect": "Allow",
      "Action": [
        "scheduler:CreateSchedule", "scheduler:UpdateSchedule",
        "scheduler:GetSchedule", "scheduler:DeleteSchedule"
      ],
      "Resource": "arn:aws:scheduler:*:*:schedule/default/sec-edgar-*"
    },
    {
      "Sid": "CloudWatchAndLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup", "logs:DescribeLogGroups",
        "logs:PutRetentionPolicy",
        "cloudwatch:PutMetricAlarm", "cloudwatch:DescribeAlarms"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SNS",
      "Effect": "Allow",
      "Action": [
        "sns:CreateTopic", "sns:Subscribe",
        "sns:GetTopicAttributes", "sns:SetTopicAttributes"
      ],
      "Resource": "arn:aws:sns:*:*:sec-edgar-*"
    },
    {
      "Sid": "VPCReadAndEndpoint",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVpcs", "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups", "ec2:DescribeVpcEndpoints",
        "ec2:CreateSecurityGroup", "ec2:AuthorizeSecurityGroupEgress",
        "ec2:RevokeSecurityGroupIngress", "ec2:DeleteSecurityGroup",
        "ec2:CreateVpcEndpoint", "ec2:ModifyVpcEndpoint",
        "ec2:CreateTags"
      ],
      "Resource": "*"
    }
  ]
}
POLICY
)

echo ""
echo "[3/3] Attaching inline policy: $POLICY_NAME"
aws iam put-user-policy \
    --user-name "$DEPLOYER_USER" \
    --policy-name "$POLICY_NAME" \
    --policy-document "$POLICY_DOC"
echo "  [OK]"

# Create access key (always creates a new one — max 2 per user)
echo ""
echo "Creating access keys (shown ONCE — save them now):"
echo "------------------------------------------------------------"
aws iam create-access-key \
    --user-name "$DEPLOYER_USER" \
    --query 'AccessKey.{KeyId:AccessKeyId,Secret:SecretAccessKey}' \
    --output table
echo "------------------------------------------------------------"

echo ""
echo "Next steps:"
echo "  1. Configure AWS CLI:"
echo "       aws configure --profile sec-edgar"
echo "       Region: $AWS_REGION  |  Output: json"
echo ""
echo "  2. Edit deploy/deploy_aws.sh — set your VPC_ID and VPC_SUBNET_IDS"
echo ""
echo "  3. Deploy:"
echo "       AWS_PROFILE=sec-edgar bash deploy/deploy_aws.sh"
echo ""
echo "  4. Weekly full refresh (all ~10k CIKs — run manually each Sunday):"
echo "       aws stepfunctions start-execution \\"
echo "         --state-machine-arn <ARN from deploy output> \\"
echo "         --input '{\"ingestDate\":\"YYYY-MM-DD\",\"fullRefresh\":true}'"

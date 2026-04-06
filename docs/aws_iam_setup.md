# AWS IAM Setup — SEC EDGAR Platform

Step-by-step guide to create all IAM roles and policies required to run the SEC EDGAR ingestion and transform pipeline on AWS (ECS Fargate + Step Functions + EventBridge).

---

## Prerequisites

- AWS account with admin access (or permission to create IAM roles and policies)
- AWS CLI installed and configured: `aws configure`
- Decisions made:
  - S3 bucket name (replace `{BUCKET}` throughout)
  - S3 key prefix (replace `{PREFIX}` — e.g. `sec-edgar`)
  - AWS region (replace `{REGION}` — e.g. `us-east-1`)
  - AWS account ID (replace `{ACCOUNT}` — find with `aws sts get-caller-identity`)

---

## Step 0 — Set Shell Variables

Run these once in your terminal. Every command below uses them.

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1          # change to your region
BUCKET=my-sec-edgar-bucket
PREFIX=sec-edgar
```

---

## Step 1 — Create the ECS Task Role

The task role is what your Python code runs as inside the container. It needs S3 read/write access to the bronze/silver/gold Parquet data.

### 1a. Write the trust policy

```bash
cat > /tmp/ecs_task_trust.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ecs-tasks.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF
```

### 1b. Write the permissions policy

```bash
cat > /tmp/ecs_task_policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET}",
        "arn:aws:s3:::${BUCKET}/${PREFIX}/*"
      ]
    }
  ]
}
EOF
```

### 1c. Create the role

```bash
aws iam create-role \
  --role-name sec-edgar-ecs-task-role \
  --assume-role-policy-document file:///tmp/ecs_task_trust.json \
  --description "Runtime identity for SEC EDGAR pipeline containers"

aws iam put-role-policy \
  --role-name sec-edgar-ecs-task-role \
  --policy-name sec-edgar-s3-readwrite \
  --policy-document file:///tmp/ecs_task_policy.json
```

### 1d. Note the ARN

```bash
ECS_TASK_ROLE_ARN=$(aws iam get-role \
  --role-name sec-edgar-ecs-task-role \
  --query Role.Arn --output text)
echo "ECS Task Role ARN: $ECS_TASK_ROLE_ARN"
```

**Verify:** Your Python code running in ECS Fargate will automatically inherit this role via the instance metadata endpoint — no `AWS_ACCESS_KEY_ID` env var needed.

---

## Step 2 — Create the ECS Task Execution Role

The execution role is used by the ECS control plane (not your code) to pull the Docker image from ECR and write container logs to CloudWatch. The AWS-managed policy covers all required permissions.

```bash
aws iam create-role \
  --role-name sec-edgar-ecs-execution-role \
  --assume-role-policy-document file:///tmp/ecs_task_trust.json \
  --description "ECS control-plane role: ECR pull + CloudWatch logs"

aws iam attach-role-policy \
  --role-name sec-edgar-ecs-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
```

```bash
ECS_EXEC_ROLE_ARN=$(aws iam get-role \
  --role-name sec-edgar-ecs-execution-role \
  --query Role.Arn --output text)
echo "ECS Execution Role ARN: $ECS_EXEC_ROLE_ARN"
```

---

## Step 3 — Create the Step Functions Execution Role

The Step Functions state machine needs permission to submit ECS Fargate tasks and pass the task/execution roles to ECS.

### 3a. Write the trust policy

```bash
cat > /tmp/sfn_trust.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "states.amazonaws.com" },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "aws:SourceAccount": "${ACCOUNT}"
      }
    }
  }]
}
EOF
```

### 3b. Write the permissions policy

```bash
cat > /tmp/sfn_policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RunECSTask",
      "Effect": "Allow",
      "Action": [
        "ecs:RunTask",
        "ecs:StopTask",
        "ecs:DescribeTasks"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassTaskRoles",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::${ACCOUNT}:role/sec-edgar-ecs-task-role",
        "arn:aws:iam::${ACCOUNT}:role/sec-edgar-ecs-execution-role"
      ]
    },
    {
      "Sid": "ECSTaskEventSync",
      "Effect": "Allow",
      "Action": [
        "events:PutTargets",
        "events:PutRule",
        "events:DescribeRule"
      ],
      "Resource": "arn:aws:events:${REGION}:${ACCOUNT}:rule/StepFunctionsGetEventsForECSTaskRule"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries",
        "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies",
        "logs:DescribeLogGroups"
      ],
      "Resource": "*"
    }
  ]
}
EOF
```

### 3c. Create the role

```bash
aws iam create-role \
  --role-name sec-edgar-sfn-role \
  --assume-role-policy-document file:///tmp/sfn_trust.json \
  --description "Step Functions execution role for SEC EDGAR pipeline"

aws iam put-role-policy \
  --role-name sec-edgar-sfn-role \
  --policy-name sec-edgar-sfn-permissions \
  --policy-document file:///tmp/sfn_policy.json
```

```bash
SFN_ROLE_ARN=$(aws iam get-role \
  --role-name sec-edgar-sfn-role \
  --query Role.Arn --output text)
echo "Step Functions Role ARN: $SFN_ROLE_ARN"
```

---

## Step 4 — Create the EventBridge Scheduler Role

EventBridge Scheduler triggers the Step Functions state machine daily at 06:00 UTC.

### 4a. Write the trust policy

```bash
cat > /tmp/scheduler_trust.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "scheduler.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF
```

### 4b. Write the permissions policy

```bash
cat > /tmp/scheduler_policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "StartStateMachine",
    "Effect": "Allow",
    "Action": "states:StartExecution",
    "Resource": "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:sec-edgar-daily"
  }]
}
EOF
```

### 4c. Create the role

```bash
aws iam create-role \
  --role-name sec-edgar-scheduler-role \
  --assume-role-policy-document file:///tmp/scheduler_trust.json \
  --description "EventBridge Scheduler role to trigger sec-edgar-daily state machine"

aws iam put-role-policy \
  --role-name sec-edgar-scheduler-role \
  --policy-name sec-edgar-scheduler-permissions \
  --policy-document file:///tmp/scheduler_policy.json
```

```bash
SCHEDULER_ROLE_ARN=$(aws iam get-role \
  --role-name sec-edgar-scheduler-role \
  --query Role.Arn --output text)
echo "Scheduler Role ARN: $SCHEDULER_ROLE_ARN"
```

---

## Step 5 — Create the ECR Repository

Stores the Docker image used by ECS Fargate tasks.

```bash
aws ecr create-repository \
  --repository-name sec-edgar-ingest \
  --region $REGION \
  --image-scanning-configuration scanOnPush=true

ECR_URI=$(aws ecr describe-repositories \
  --repository-names sec-edgar-ingest \
  --query "repositories[0].repositoryUri" --output text)
echo "ECR URI: $ECR_URI"
```

The ECS execution role's managed policy (`AmazonECSTaskExecutionRolePolicy`) already includes ECR pull permissions — no additional ECR policy needed.

---

## Step 6 — Create the S3 Bucket

```bash
# us-east-1 does NOT use CreateBucketConfiguration
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket \
    --bucket $BUCKET \
    --region $REGION
else
  aws s3api create-bucket \
    --bucket $BUCKET \
    --region $REGION \
    --create-bucket-configuration LocationConstraint=$REGION
fi

# Block all public access
aws s3api put-public-access-block \
  --bucket $BUCKET \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Enable versioning (optional but recommended for audit trail)
aws s3api put-bucket-versioning \
  --bucket $BUCKET \
  --versioning-configuration Status=Enabled
```

---

## Step 7 (Path B only) — Snowflake S3 Storage Integration IAM Role

Skip this step if using Path A (DuckDB).

Snowflake needs a dedicated IAM role to read bronze Parquet from S3. After creating the Storage Integration in Snowflake (`DESC INTEGRATION sec_edgar_s3_int`), Snowflake provides an IAM user ARN and external ID — use them in the trust policy below.

```bash
# Run DESC INTEGRATION in Snowflake first to get these values:
SNOWFLAKE_IAM_USER=arn:aws:iam::123456789012:user/abc-def   # from DESC INTEGRATION output
SNOWFLAKE_EXTERNAL_ID=ExternalId_from_DESC_INTEGRATION       # from DESC INTEGRATION output

cat > /tmp/snowflake_trust.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "${SNOWFLAKE_IAM_USER}" },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "sts:ExternalId": "${SNOWFLAKE_EXTERNAL_ID}"
      }
    }
  }]
}
EOF

cat > /tmp/snowflake_policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "SnowflakeS3Read",
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::${BUCKET}",
      "arn:aws:s3:::${BUCKET}/${PREFIX}/bronze/*"
    ]
  }]
}
EOF

aws iam create-role \
  --role-name snowflake-s3-reader \
  --assume-role-policy-document file:///tmp/snowflake_trust.json \
  --description "Read-only S3 access for Snowflake Storage Integration"

aws iam put-role-policy \
  --role-name snowflake-s3-reader \
  --policy-name snowflake-bronze-read \
  --policy-document file:///tmp/snowflake_policy.json

SNOWFLAKE_ROLE_ARN=$(aws iam get-role \
  --role-name snowflake-s3-reader --query Role.Arn --output text)
echo "Snowflake Role ARN: $SNOWFLAKE_ROLE_ARN"
# Paste this ARN into: ALTER STORAGE INTEGRATION sec_edgar_s3_int SET STORAGE_AWS_ROLE_ARN = '...'
```

---

## Step 8 — Local Dev IAM User (optional)

For local development, create a named IAM user with S3 access scoped to the same bucket/prefix. Do **not** use admin credentials or root keys locally.

```bash
aws iam create-user --user-name sec-edgar-local-dev

cat > /tmp/local_dev_policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
               "s3:ListBucket", "s3:GetBucketLocation"],
    "Resource": [
      "arn:aws:s3:::${BUCKET}",
      "arn:aws:s3:::${BUCKET}/${PREFIX}/*"
    ]
  }]
}
EOF

aws iam put-user-policy \
  --user-name sec-edgar-local-dev \
  --policy-name sec-edgar-s3-readwrite \
  --policy-document file:///tmp/local_dev_policy.json

# Create access keys (store securely — shown once only)
aws iam create-access-key --user-name sec-edgar-local-dev
```

Configure locally:
```bash
aws configure --profile sec-edgar
# Enter Access Key ID and Secret Access Key from above
# Region: us-east-1 (or your region)

export AWS_PROFILE=sec-edgar
```

---

## Verification

### Confirm all roles exist

```bash
for ROLE in sec-edgar-ecs-task-role sec-edgar-ecs-execution-role \
            sec-edgar-sfn-role sec-edgar-scheduler-role; do
  ARN=$(aws iam get-role --role-name $ROLE --query Role.Arn --output text 2>/dev/null)
  echo "$ROLE → $ARN"
done
```

### Confirm task role can access S3

```bash
# Simulate what the ECS container does (assumes the task role manually)
aws sts assume-role \
  --role-arn $ECS_TASK_ROLE_ARN \
  --role-session-name test-session \
  --query "Credentials.[AccessKeyId,SecretAccessKey,SessionToken]" \
  --output text

# Then set those as env vars and test:
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
aws s3 ls s3://$BUCKET/$PREFIX/ && echo "S3 access OK"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
```

### Confirm ECR repository exists

```bash
aws ecr describe-repositories --repository-names sec-edgar-ingest \
  --query "repositories[0].repositoryUri" --output text
```

---

## Summary — ARNs to record

Copy these ARNs into your ECS task definition and Step Functions state machine:

| Resource | ARN |
|---|---|
| ECS Task Role | `arn:aws:iam::{ACCOUNT}:role/sec-edgar-ecs-task-role` |
| ECS Execution Role | `arn:aws:iam::{ACCOUNT}:role/sec-edgar-ecs-execution-role` |
| Step Functions Role | `arn:aws:iam::{ACCOUNT}:role/sec-edgar-sfn-role` |
| EventBridge Scheduler Role | `arn:aws:iam::{ACCOUNT}:role/sec-edgar-scheduler-role` |
| Snowflake S3 Reader Role (Path B) | `arn:aws:iam::{ACCOUNT}:role/snowflake-s3-reader` |
| ECR repository | `{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/sec-edgar-ingest` |
| S3 bucket | `s3://{BUCKET}/{PREFIX}/` |

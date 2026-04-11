# ---------------------------------------------------------------------------
# IAM Role: ECS Task Role
# Grants containers permission to read/write S3 bronze data.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "ecs_task" {
  name        = local.task_role_name
  description = "SEC EDGAR ingest pipeline — ECS task role"
  tags        = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "sec-edgar-inline-policy"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3WriteRead"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::${var.s3_bucket_name}/sec-edgar/bronze/*"
      },
      {
        Sid      = "S3List"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::${var.s3_bucket_name}"
        Condition = {
          StringLike = { "s3:prefix" = ["sec-edgar/bronze/*"] }
        }
      },
      {
        Sid      = "S3BucketLocation"
        Effect   = "Allow"
        Action   = "s3:GetBucketLocation"
        Resource = "arn:aws:s3:::${var.s3_bucket_name}"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# IAM Role: ECS Execution Role
# Grants ECS to pull the image from ECR and write logs to CloudWatch.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "ecs_execution" {
  name        = local.execution_role_name
  description = "SEC EDGAR ingest pipeline — ECS execution role"
  tags        = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ecs_execution" {
  name = "sec-edgar-inline-policy"
  role = aws_iam_role.ecs_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ECRAuth"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid    = "ECRPull"
        Effect = "Allow"
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchCheckLayerAvailability",
        ]
        Resource = "arn:aws:ecr:${var.aws_region}:${local.account_id}:repository/${var.ecr_repo_name}"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/ecs/sec-edgar-ingest-*:*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# IAM Role: Step Functions Role
# Grants Step Functions to run ECS tasks and register EventBridge rules.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "step_functions" {
  name        = local.sf_role_name
  description = "SEC EDGAR ingest pipeline — Step Functions role"
  tags        = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "sec-edgar-inline-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECSRunTask"
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:DescribeTasks",
          "ecs:StopTask",
        ]
        Resource = "*"
        Condition = {
          ArnLike = {
            "ecs:cluster" = local.cluster_arn
          }
        }
      },
      {
        Sid    = "PassRoleToECS"
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          local.task_role_arn,
          local.execution_role_arn,
        ]
      },
      {
        # Step Functions auto-creates this rule when using ecs:runTask.sync
        Sid    = "EventBridgeManagedRules"
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
        ]
        Resource = "arn:aws:events:${var.aws_region}:${local.account_id}:rule/StepFunctionsGetEventsForECSTaskRule"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# IAM Role: EventBridge Scheduler Role
# Grants the scheduler to trigger the Step Functions state machine.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "scheduler" {
  name        = local.scheduler_role_name
  description = "SEC EDGAR ingest pipeline — EventBridge Scheduler role"
  tags        = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "sec-edgar-inline-policy"
  role = aws_iam_role.scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "StartStateMachine"
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = "arn:aws:states:${var.aws_region}:${local.account_id}:stateMachine:${var.step_functions_name}"
    }]
  })
}

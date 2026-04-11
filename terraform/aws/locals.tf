locals {
  account_id = data.aws_caller_identity.current.account_id

  # Full ECR image URI — referenced in every ECS task definition
  ecr_image_uri = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com/${var.ecr_repo_name}:latest"

  # IAM role names (used in ARN construction before the resources are created
  # to break circular dependencies in the Step Functions definition)
  task_role_name      = "sec-edgar-ecs-task-role"
  execution_role_name = "sec-edgar-ecs-execution-role"
  sf_role_name        = "sec-edgar-stepfunctions-role"
  scheduler_role_name = "sec-edgar-scheduler-role"

  # Pre-built ARNs used in IAM policies and Step Functions definition
  task_role_arn      = "arn:aws:iam::${local.account_id}:role/${local.task_role_name}"
  execution_role_arn = "arn:aws:iam::${local.account_id}:role/${local.execution_role_name}"
  cluster_arn        = "arn:aws:ecs:${var.aws_region}:${local.account_id}:cluster/${var.ecs_cluster_name}"

  # Environment variables common to all four ECS ingest tasks
  common_env = [
    { name = "CLOUD_PROVIDER",     value = "aws" },
    { name = "AWS_BUCKET",         value = var.s3_bucket_name },
    { name = "AWS_DEFAULT_REGION", value = var.aws_region },
    { name = "SEC_USER_AGENT",     value = var.sec_user_agent },
  ]
}

# terraform/aws/tests/validate.tftest.hcl
#
# Built-in Terraform tests for the SEC EDGAR Bronze AWS infrastructure module.
# Requires Terraform >= 1.7 (mock_provider support).
#
# All tests use `command = plan` + mock_provider so they run without real AWS
# credentials — safe for CI and local development.
#
# Run:
#   cd terraform/aws
#   terraform test
#
# Or via the test runner:
#   bash tests/run_tests.sh

# ---------------------------------------------------------------------------
# Mock the AWS provider so no real credentials are needed.
# Provides dummy responses for the two data sources used by the module.
# ---------------------------------------------------------------------------
mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:user/ci-test"
      user_id    = "AKIAIOSFODNN7EXAMPLE"
    }
  }

  mock_data "aws_route_tables" {
    defaults = {
      ids = ["rtb-aabbccdd", "rtb-ddccbbaa"]
    }
  }
}

# ---------------------------------------------------------------------------
# Default test variables — mirrors production defaults in variables.tf
# ---------------------------------------------------------------------------
variables {
  s3_bucket_name     = "paulananth11-sec-edgar-bronze"
  vpc_id             = "vpc-0c6563d45766bc9eb"
  subnet_ids         = ["subnet-0b4b36da5bb4ac69c", "subnet-03eb778d9f7b5ae2b"]
  log_retention_days = 30
  sns_alert_email    = "paul.ananth@yahoo.com"
  sec_user_agent     = "SEC EDGAR Pipeline paul.ananth@yahoo.com"
  tags = {
    Project     = "sec-edgar-bronze"
    ManagedBy   = "terraform"
    Environment = "production"
  }
}

# ===========================================================================
# S3 — bucket configuration
# ===========================================================================

run "s3_bucket_name_matches_variable" {
  command = plan

  assert {
    condition     = aws_s3_bucket.bronze.bucket == var.s3_bucket_name
    error_message = "S3 bucket name '${aws_s3_bucket.bronze.bucket}' does not match variable '${var.s3_bucket_name}'"
  }
}

run "s3_versioning_enabled" {
  command = plan

  assert {
    condition     = aws_s3_bucket_versioning.bronze.versioning_configuration[0].status == "Enabled"
    error_message = "S3 versioning must be Enabled, got: ${aws_s3_bucket_versioning.bronze.versioning_configuration[0].status}"
  }
}

run "s3_encryption_aes256" {
  command = plan

  assert {
    condition = (
      aws_s3_bucket_server_side_encryption_configuration.bronze
        .rule[0]
        .apply_server_side_encryption_by_default[0]
        .sse_algorithm == "AES256"
    )
    error_message = "S3 encryption must use AES256"
  }
}

run "s3_public_access_fully_blocked" {
  command = plan

  assert {
    condition     = aws_s3_bucket_public_access_block.bronze.block_public_acls == true
    error_message = "block_public_acls must be true"
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.bronze.ignore_public_acls == true
    error_message = "ignore_public_acls must be true"
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.bronze.block_public_policy == true
    error_message = "block_public_policy must be true"
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.bronze.restrict_public_buckets == true
    error_message = "restrict_public_buckets must be true"
  }
}

# ===========================================================================
# ECR — repository configuration
# ===========================================================================

run "ecr_scan_on_push_enabled" {
  command = plan

  assert {
    condition     = aws_ecr_repository.ingest.image_scanning_configuration[0].scan_on_push == true
    error_message = "ECR must have scan_on_push enabled"
  }
}

run "ecr_repo_name_matches_variable" {
  command = plan

  assert {
    condition     = aws_ecr_repository.ingest.name == var.ecr_repo_name
    error_message = "ECR repo name '${aws_ecr_repository.ingest.name}' does not match variable '${var.ecr_repo_name}'"
  }
}

# ===========================================================================
# Security Group — network isolation
# ===========================================================================

run "sg_no_ingress_rules" {
  command = plan

  assert {
    condition     = length(aws_security_group.ecs_tasks.ingress) == 0
    error_message = "ECS security group must have zero ingress rules, found ${length(aws_security_group.ecs_tasks.ingress)}"
  }
}

run "sg_https_only_egress" {
  command = plan

  assert {
    condition     = aws_security_group.ecs_tasks.egress[0].from_port == 443
    error_message = "ECS egress must start at port 443"
  }
  assert {
    condition     = aws_security_group.ecs_tasks.egress[0].to_port == 443
    error_message = "ECS egress must end at port 443"
  }
  assert {
    condition     = aws_security_group.ecs_tasks.egress[0].protocol == "tcp"
    error_message = "ECS egress must use TCP protocol"
  }
  assert {
    condition     = contains(aws_security_group.ecs_tasks.egress[0].cidr_blocks, "0.0.0.0/0")
    error_message = "ECS egress cidr_blocks must include 0.0.0.0/0"
  }
}

# ===========================================================================
# ECS Task Definitions — CPU and memory sizing
# ===========================================================================

run "task01_sizing_1vcpu_2gb" {
  command = plan

  assert {
    condition     = aws_ecs_task_definition.task01.cpu == "1024"
    error_message = "Task 01 must use 1024 CPU units, got: ${aws_ecs_task_definition.task01.cpu}"
  }
  assert {
    condition     = aws_ecs_task_definition.task01.memory == "2048"
    error_message = "Task 01 must use 2048 MB memory, got: ${aws_ecs_task_definition.task01.memory}"
  }
}

run "task02_sizing_1vcpu_2gb" {
  command = plan

  assert {
    condition     = aws_ecs_task_definition.task02.cpu == "1024"
    error_message = "Task 02 must use 1024 CPU units"
  }
  assert {
    condition     = aws_ecs_task_definition.task02.memory == "2048"
    error_message = "Task 02 must use 2048 MB memory"
  }
}

run "task03_sizing_2vcpu_4gb" {
  command = plan

  assert {
    condition     = aws_ecs_task_definition.task03.cpu == "2048"
    error_message = "Task 03 must use 2048 CPU units (submissions are heavier)"
  }
  assert {
    condition     = aws_ecs_task_definition.task03.memory == "4096"
    error_message = "Task 03 must use 4096 MB memory"
  }
}

run "task04_sizing_2vcpu_4gb" {
  command = plan

  assert {
    condition     = aws_ecs_task_definition.task04.cpu == "2048"
    error_message = "Task 04 must use 2048 CPU units"
  }
  assert {
    condition     = aws_ecs_task_definition.task04.memory == "4096"
    error_message = "Task 04 must use 4096 MB memory"
  }
}

run "all_tasks_use_fargate" {
  command = plan

  assert {
    condition     = contains(aws_ecs_task_definition.task01.requires_compatibilities, "FARGATE")
    error_message = "Task 01 must use FARGATE"
  }
  assert {
    condition     = contains(aws_ecs_task_definition.task04.requires_compatibilities, "FARGATE")
    error_message = "Task 04 must use FARGATE"
  }
}

run "all_tasks_use_awsvpc_network_mode" {
  command = plan

  assert {
    condition     = aws_ecs_task_definition.task01.network_mode == "awsvpc"
    error_message = "Task definitions must use awsvpc network mode for Fargate"
  }
}

# ===========================================================================
# CloudWatch — log retention
# ===========================================================================

run "log_groups_have_30_day_retention" {
  command = plan

  assert {
    condition     = aws_cloudwatch_log_group.task01.retention_in_days == 30
    error_message = "task01 log group must have 30-day retention"
  }
  assert {
    condition     = aws_cloudwatch_log_group.task02.retention_in_days == 30
    error_message = "task02 log group must have 30-day retention"
  }
  assert {
    condition     = aws_cloudwatch_log_group.task03.retention_in_days == 30
    error_message = "task03 log group must have 30-day retention"
  }
  assert {
    condition     = aws_cloudwatch_log_group.task04.retention_in_days == 30
    error_message = "task04 log group must have 30-day retention"
  }
}

run "log_group_names_follow_convention" {
  command = plan

  assert {
    condition     = aws_cloudwatch_log_group.task01.name == "/ecs/sec-edgar-ingest-01"
    error_message = "task01 log group name must be /ecs/sec-edgar-ingest-01"
  }
  assert {
    condition     = aws_cloudwatch_log_group.task04.name == "/ecs/sec-edgar-ingest-04"
    error_message = "task04 log group name must be /ecs/sec-edgar-ingest-04"
  }
}

# ===========================================================================
# IAM — trust policies (correct service principals)
# ===========================================================================

run "iam_task_role_trusts_ecs" {
  command = plan

  assert {
    condition     = can(jsondecode(aws_iam_role.ecs_task.assume_role_policy))
    error_message = "ECS task role assume_role_policy must be valid JSON"
  }
  assert {
    condition = contains(
      jsondecode(aws_iam_role.ecs_task.assume_role_policy).Statement[0].Principal.Service,
      "ecs-tasks.amazonaws.com"
    )
    error_message = "ECS task role must trust ecs-tasks.amazonaws.com"
  }
}

run "iam_step_functions_role_trusts_states" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role.step_functions.assume_role_policy).Statement[0].Principal.Service,
      "states.amazonaws.com"
    )
    error_message = "Step Functions role must trust states.amazonaws.com"
  }
}

run "iam_scheduler_role_trusts_scheduler" {
  command = plan

  assert {
    condition = contains(
      jsondecode(aws_iam_role.scheduler.assume_role_policy).Statement[0].Principal.Service,
      "scheduler.amazonaws.com"
    )
    error_message = "Scheduler role must trust scheduler.amazonaws.com"
  }
}

# ===========================================================================
# Step Functions — state machine type
# ===========================================================================

run "state_machine_is_standard_type" {
  command = plan

  assert {
    condition     = aws_sfn_state_machine.pipeline.type == "STANDARD"
    error_message = "State machine must be STANDARD type (not EXPRESS)"
  }
}

run "state_machine_name_matches_variable" {
  command = plan

  assert {
    condition     = aws_sfn_state_machine.pipeline.name == var.step_functions_name
    error_message = "State machine name must match step_functions_name variable"
  }
}

# ===========================================================================
# SNS — alert subscription endpoint
# ===========================================================================

run "sns_subscription_uses_email_protocol" {
  command = plan

  assert {
    condition     = aws_sns_topic_subscription.email_alert.protocol == "email"
    error_message = "SNS subscription must use email protocol"
  }
  assert {
    condition     = aws_sns_topic_subscription.email_alert.endpoint == var.sns_alert_email
    error_message = "SNS subscription endpoint must match sns_alert_email variable"
  }
}

# ===========================================================================
# VPC Endpoint — S3 Gateway type
# ===========================================================================

run "vpc_endpoint_is_gateway_type" {
  command = plan

  assert {
    condition     = aws_vpc_endpoint.s3_gateway.vpc_endpoint_type == "Gateway"
    error_message = "S3 VPC endpoint must be Gateway type (not Interface)"
  }
  assert {
    condition     = aws_vpc_endpoint.s3_gateway.vpc_id == var.vpc_id
    error_message = "S3 VPC endpoint must be in the configured VPC"
  }
}

# ===========================================================================
# Variable override tests — ensure key settings are overridable
# ===========================================================================

run "custom_bucket_name_is_applied" {
  command = plan

  variables {
    s3_bucket_name = "my-custom-test-bucket"
  }

  assert {
    condition     = aws_s3_bucket.bronze.bucket == "my-custom-test-bucket"
    error_message = "Custom s3_bucket_name variable must propagate to the bucket resource"
  }
}

run "custom_log_retention_is_applied" {
  command = plan

  variables {
    log_retention_days = 90
  }

  assert {
    condition     = aws_cloudwatch_log_group.task01.retention_in_days == 90
    error_message = "Custom log_retention_days variable must propagate to log groups"
  }
}

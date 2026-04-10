# ---------------------------------------------------------------------------
# Step Functions State Machine
# Orchestrates 4 sequential ECS Fargate tasks using ecs:runTask.sync.
# Input schema: {"ingestDate": "YYYY-MM-DD", "fullRefresh": false}
# ---------------------------------------------------------------------------

locals {
  # Network config block reused across all four task states
  sfn_network_config = {
    AwsvpcConfiguration = {
      Subnets        = var.subnet_ids
      SecurityGroups = [aws_security_group.ecs_tasks.id]
      AssignPublicIp = "ENABLED"
    }
  }

  # Role overrides reused across all four task states
  sfn_role_overrides = {
    TaskRoleArn      = aws_iam_role.ecs_task.arn
    ExecutionRoleArn = aws_iam_role.ecs_execution.arn
  }

  # Environment variables with FULL_REFRESH passed through from state input
  sfn_env_with_refresh = [
    { Name = "CLOUD_PROVIDER",     Value = "aws" },
    { Name = "AWS_BUCKET",         Value = var.s3_bucket_name },
    { Name = "AWS_DEFAULT_REGION", Value = var.aws_region },
    { Name = "SEC_USER_AGENT",     Value = var.sec_user_agent },
    # Value.$ evaluates the JSONPath expression at runtime
    { "Name" = "FULL_REFRESH", "Value.$" = "States.Format('{}', $.fullRefresh)" },
  ]

  # Environment variables for task 01 (no FULL_REFRESH needed)
  sfn_env_base = [
    { Name = "CLOUD_PROVIDER",     Value = "aws" },
    { Name = "AWS_BUCKET",         Value = var.s3_bucket_name },
    { Name = "AWS_DEFAULT_REGION", Value = var.aws_region },
    { Name = "SEC_USER_AGENT",     Value = var.sec_user_agent },
  ]

  sfn_retry = [{
    ErrorEquals     = ["States.TaskFailed", "States.HeartbeatTimeout"]
    IntervalSeconds = 300
    MaxAttempts     = 1
    BackoffRate     = 1.0
  }]
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = var.step_functions_name
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"
  tags     = var.tags

  definition = jsonencode({
    Comment = "SEC EDGAR Bronze ingest — 4 sequential ECS Fargate tasks. Incremental by default (CIKs that filed today); set fullRefresh:true to scan all ~5k NYSE/Nasdaq CIKs. Input: {\"ingestDate\": \"YYYY-MM-DD\", \"fullRefresh\": false}"
    StartAt = "IngestTickersExchange"

    States = {

      # ------------------------------------------------------------------
      # Task 01 — Tickers snapshot (1 HTTP call, ~10 k rows)
      # ------------------------------------------------------------------
      IngestTickersExchange = {
        Type            = "Task"
        Comment         = "Script 01: fetch company_tickers_exchange.json snapshot"
        Resource        = "arn:aws:states:::ecs:runTask.sync"
        TimeoutSeconds  = 900
        HeartbeatSeconds = 300
        Parameters = {
          LaunchType           = "FARGATE"
          Cluster              = aws_ecs_cluster.main.arn
          TaskDefinition       = aws_ecs_task_definition.task01.family
          NetworkConfiguration = local.sfn_network_config
          Overrides = merge(local.sfn_role_overrides, {
            ContainerOverrides = [{
              Name        = "ingest"
              "Command.$" = "States.Array('scripts/ingest/01_ingest_tickers_exchange.py', '--date', $.ingestDate)"
              Environment = local.sfn_env_base
            }]
          })
        }
        Retry      = local.sfn_retry
        ResultPath = null
        Next       = "IngestDailyIndex"
      }

      # ------------------------------------------------------------------
      # Task 02 — Daily index (identifies changed CIKs for the day)
      # ------------------------------------------------------------------
      IngestDailyIndex = {
        Type            = "Task"
        Comment         = "Script 02: fetch EDGAR daily master.idx -> write changed CIKs. FULL_REFRESH bypasses the index."
        Resource        = "arn:aws:states:::ecs:runTask.sync"
        TimeoutSeconds  = 900
        HeartbeatSeconds = 300
        Parameters = {
          LaunchType           = "FARGATE"
          Cluster              = aws_ecs_cluster.main.arn
          TaskDefinition       = aws_ecs_task_definition.task02.family
          NetworkConfiguration = local.sfn_network_config
          Overrides = merge(local.sfn_role_overrides, {
            ContainerOverrides = [{
              Name        = "ingest"
              "Command.$" = "States.Array('scripts/ingest/02_ingest_daily_index.py', '--date', $.ingestDate)"
              Environment = local.sfn_env_with_refresh
            }]
          })
        }
        Retry      = local.sfn_retry
        ResultPath = null
        Next       = "IngestSubmissions"
      }

      # ------------------------------------------------------------------
      # Task 03 — Submissions (incremental ~200-600 CIKs; full ~5k, ~90 min)
      # ------------------------------------------------------------------
      IngestSubmissions = {
        Type            = "Task"
        Comment         = "Script 03: fetch submission forms for changed CIKs"
        Resource        = "arn:aws:states:::ecs:runTask.sync"
        TimeoutSeconds  = 5400
        HeartbeatSeconds = 300
        Parameters = {
          LaunchType           = "FARGATE"
          Cluster              = aws_ecs_cluster.main.arn
          TaskDefinition       = aws_ecs_task_definition.task03.family
          NetworkConfiguration = local.sfn_network_config
          Overrides = merge(local.sfn_role_overrides, {
            ContainerOverrides = [{
              Name        = "ingest"
              "Command.$" = "States.Array('scripts/ingest/03_ingest_submissions.py', '--date', $.ingestDate)"
              Environment = local.sfn_env_with_refresh
            }]
          })
        }
        Retry      = local.sfn_retry
        ResultPath = null
        Next       = "IngestCompanyFacts"
      }

      # ------------------------------------------------------------------
      # Task 04 — Company facts / XBRL (incremental ~200-600; full ~5k, ~90 min)
      # ------------------------------------------------------------------
      IngestCompanyFacts = {
        Type            = "Task"
        Comment         = "Script 04: fetch XBRL company facts for changed CIKs"
        Resource        = "arn:aws:states:::ecs:runTask.sync"
        TimeoutSeconds  = 5400
        HeartbeatSeconds = 300
        Parameters = {
          LaunchType           = "FARGATE"
          Cluster              = aws_ecs_cluster.main.arn
          TaskDefinition       = aws_ecs_task_definition.task04.family
          NetworkConfiguration = local.sfn_network_config
          Overrides = merge(local.sfn_role_overrides, {
            ContainerOverrides = [{
              Name        = "ingest"
              "Command.$" = "States.Array('scripts/ingest/04_ingest_companyfacts.py', '--date', $.ingestDate)"
              Environment = local.sfn_env_with_refresh
            }]
          })
        }
        Retry      = local.sfn_retry
        ResultPath = null
        End        = true
      }

    }
  })
}

# ---------------------------------------------------------------------------
# Security Group — HTTPS-only egress for ECS Fargate tasks
# No ingress; tasks only make outbound calls to SEC EDGAR + AWS APIs.
# ---------------------------------------------------------------------------

resource "aws_security_group" "ecs_tasks" {
  name        = "sec-edgar-ecs-sg"
  description = "SEC EDGAR ECS tasks — HTTPS-only egress, no ingress"
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "sec-edgar-ecs-sg" })

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS egress to SEC EDGAR and AWS service endpoints"
  }
}

# ---------------------------------------------------------------------------
# ECS Cluster
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = var.ecs_cluster_name
  tags = var.tags

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE"]
}

# ---------------------------------------------------------------------------
# ECS Task Definitions (4 sequential ingest scripts)
# Each task definition depends on the CloudWatch log groups so that logs
# are ready before any execution attempts to write to them.
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "task01" {
  family                   = "sec-edgar-ingest-01"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  task_role_arn            = aws_iam_role.ecs_task.arn
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  tags                     = var.tags

  container_definitions = jsonencode([{
    name    = "ingest"
    image   = local.ecr_image_uri
    command = ["python", "scripts/ingest/01_ingest_tickers_exchange.py"]
    environment = local.common_env
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.task01.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "task02" {
  family                   = "sec-edgar-ingest-02"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  task_role_arn            = aws_iam_role.ecs_task.arn
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  tags                     = var.tags

  container_definitions = jsonencode([{
    name    = "ingest"
    image   = local.ecr_image_uri
    command = ["python", "scripts/ingest/02_ingest_daily_index.py"]
    environment = local.common_env
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.task02.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "task03" {
  family                   = "sec-edgar-ingest-03"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "2048"
  memory                   = "4096"
  task_role_arn            = aws_iam_role.ecs_task.arn
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  tags                     = var.tags

  container_definitions = jsonencode([{
    name    = "ingest"
    image   = local.ecr_image_uri
    command = ["python", "scripts/ingest/03_ingest_submissions.py"]
    environment = local.common_env
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.task03.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "task04" {
  family                   = "sec-edgar-ingest-04"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "2048"
  memory                   = "4096"
  task_role_arn            = aws_iam_role.ecs_task.arn
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  tags                     = var.tags

  container_definitions = jsonencode([{
    name    = "ingest"
    image   = local.ecr_image_uri
    command = ["python", "scripts/ingest/04_ingest_companyfacts.py"]
    environment = local.common_env
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.task04.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

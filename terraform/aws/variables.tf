variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for SEC EDGAR bronze Parquet data"
  type        = string
  default     = "paulananth11-sec-edgar-bronze"
}

variable "ecr_repo_name" {
  description = "ECR repository name for the ingest Docker image"
  type        = string
  default     = "sec-edgar-ingest"
}

variable "ecs_cluster_name" {
  description = "ECS cluster name"
  type        = string
  default     = "sec-edgar-cluster"
}

variable "step_functions_name" {
  description = "Step Functions state machine name"
  type        = string
  default     = "sec-edgar-bronze-ingest"
}

variable "vpc_id" {
  description = "VPC ID where ECS Fargate tasks run"
  type        = string
  default     = "vpc-0c6563d45766bc9eb"
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS Fargate tasks (must have outbound internet access)"
  type        = list(string)
  default     = ["subnet-0b4b36da5bb4ac69c", "subnet-03eb778d9f7b5ae2b"]
}

variable "sns_alert_email" {
  description = "Email address for pipeline failure alerts (must confirm the SNS subscription)"
  type        = string
  default     = "paul.ananth@yahoo.com"
}

variable "sec_user_agent" {
  description = "User-Agent header for SEC EDGAR HTTP requests (SEC requires valid contact info)"
  type        = string
  default     = "SEC EDGAR Pipeline paul.ananth@yahoo.com"
}

variable "scheduler_cron" {
  description = "EventBridge Scheduler cron expression in UTC"
  type        = string
  default     = "cron(0 6 * * ? *)" # 06:00 UTC daily
}

variable "log_retention_days" {
  description = "CloudWatch log group retention in days"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Common resource tags applied to all supported resources"
  type        = map(string)
  default = {
    Project     = "sec-edgar-bronze"
    ManagedBy   = "terraform"
    Environment = "production"
  }
}

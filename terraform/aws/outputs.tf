output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

output "s3_bucket_name" {
  description = "S3 bucket name for SEC EDGAR bronze data"
  value       = aws_s3_bucket.bronze.bucket
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.bronze.arn
}

output "ecr_repository_url" {
  description = "Full ECR repository URL — use this for docker push"
  value       = aws_ecr_repository.ingest.repository_url
}

output "ecr_repository_name" {
  description = "ECR repository name"
  value       = aws_ecr_repository.ingest.name
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = aws_ecs_cluster.main.arn
}

output "security_group_id" {
  description = "ECS task security group ID"
  value       = aws_security_group.ecs_tasks.id
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "sns_topic_arn" {
  description = "SNS alert topic ARN (confirm the subscription email before pipeline runs)"
  value       = aws_sns_topic.alerts.arn
}

output "docker_push_commands" {
  description = "Commands to build and push the Docker image after apply"
  value = <<-EOT
    # Authenticate Docker to ECR
    aws ecr get-login-password --region ${var.aws_region} | \
      docker login --username AWS --password-stdin ${aws_ecr_repository.ingest.repository_url}

    # Build and push (run from the repo root)
    docker build --platform linux/amd64 -t ${aws_ecr_repository.ingest.repository_url}:latest .
    docker push ${aws_ecr_repository.ingest.repository_url}:latest
  EOT
}

output "smoke_test_command" {
  description = "Run an incremental smoke test (today's date)"
  value       = "aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.pipeline.arn} --input '{\"ingestDate\":\"YYYY-MM-DD\",\"fullRefresh\":false}'"
}

output "full_refresh_command" {
  description = "Run a full refresh (all ~5k NYSE/Nasdaq CIKs — use on Sundays)"
  value       = "aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.pipeline.arn} --input '{\"ingestDate\":\"YYYY-MM-DD\",\"fullRefresh\":true}'"
}

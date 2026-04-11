# ---------------------------------------------------------------------------
# CloudWatch Log Groups — one per ingest task, 30-day retention
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "task01" {
  name              = "/ecs/sec-edgar-ingest-01"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "task02" {
  name              = "/ecs/sec-edgar-ingest-02"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "task03" {
  name              = "/ecs/sec-edgar-ingest-03"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "task04" {
  name              = "/ecs/sec-edgar-ingest-04"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# ---------------------------------------------------------------------------
# CloudWatch Alarm — fires SNS alert on any Step Functions execution failure
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "pipeline_failure" {
  alarm_name          = "sec-edgar-pipeline-failure"
  alarm_description   = "SEC EDGAR Bronze ingest pipeline failed"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = var.tags
}

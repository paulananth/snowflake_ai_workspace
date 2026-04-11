# ---------------------------------------------------------------------------
# EventBridge Scheduler — triggers the pipeline daily at 06:00 UTC
# Uses the newer aws_scheduler_schedule resource (AWS provider >= 4.61)
# ---------------------------------------------------------------------------

resource "aws_scheduler_schedule" "daily_ingest" {
  name        = "sec-edgar-daily-ingest"
  description = "Daily trigger for SEC EDGAR Bronze ingest pipeline"
  group_name  = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.scheduler_cron
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn

    # EventBridge Scheduler evaluates <aws.scheduler.scheduled-time> at runtime
    # and injects the ISO 8601 execution timestamp; we slice to YYYY-MM-DD.
    input = jsonencode({
      ingestDate  = "<aws.scheduler.scheduled-time>"
      fullRefresh = false
    })
  }
}

# ---------------------------------------------------------------------------
# SNS Topic — pipeline failure alerts
# The email subscription requires manual confirmation from the inbox.
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "alerts" {
  name = "sec-edgar-pipeline-alerts"
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.sns_alert_email
}

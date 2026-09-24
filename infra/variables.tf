variable "aws_region" {
  default = "eu-west-1"
}

variable "bucket_name" {
  description = "S3 bucket for KRM data"
  type        = string
  default     = "krm-validatie-data"
}

variable "waterinfo_settings_url" {
  description = "Location of the Waterinfo download settings file read by the downloading lambda"
  type        = string
  # TODO switch back to refs/heads/main once the waterinfo_downloading branch is merged
  default = "https://raw.githubusercontent.com/openearth/krmvalidatie/refs/heads/waterinfo_downloading/data/waterinfo_downloading_settings.toml"
}

variable "waterinfo_schedule_expression" {
  description = "Schedule for the automatic Waterinfo download (EventBridge cron/rate expression)"
  type        = string
  default     = "cron(0 3 * * ? *)"
}

variable "waterinfo_schedule_enabled" {
  description = "Whether the scheduled Waterinfo download runs automatically. Manual SNS triggering works regardless."
  type        = bool
  default     = false
}
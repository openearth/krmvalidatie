variable "aws_region" {
  default = "eu-west-1"
}

variable "bucket_name" {
  description = "S3 bucket for KRM data"
  type        = string
  default     = "krm-validatie-data-${terraform.workspace}"
}
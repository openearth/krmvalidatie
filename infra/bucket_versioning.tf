resource "aws_s3_bucket_versioning" "krm_validatie_bucket" {
  count  = local.is_prod ? 0 : 1
  bucket = aws_s3_bucket.krm_validatie_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "krm_validatie_bucket" {
  count  = local.is_prod ? 0 : 1
  bucket = aws_s3_bucket.krm_validatie_bucket.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    # the whole bucket
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  # the rule only makes sense with versioning switched on
  depends_on = [aws_s3_bucket_versioning.krm_validatie_bucket]
}

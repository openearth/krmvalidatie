resource "aws_s3_bucket" "krm_validatie_bucket" {
  bucket = "krm-validatie-data-${terraform.workspace}"
}

resource "aws_iam_user" "s3_reader" {
  name = "krm-validatie-s3-reader-${terraform.workspace}"
}

resource "aws_iam_access_key" "s3_reader" {
  user = aws_iam_user.s3_reader.name
}

resource "aws_secretsmanager_secret" "s3_reader_credentials" {
  name = "s3-reader-credentials-${terraform.workspace}"
}

resource "aws_secretsmanager_secret_version" "s3_reader_credentials" {
  secret_id = aws_secretsmanager_secret.s3_reader_credentials.id
  secret_string = jsonencode({
    id     = aws_iam_access_key.s3_reader.id,
    secret = aws_iam_access_key.s3_reader.secret
  })
}

resource "aws_iam_user_policy" "s3_reader" {
  name = "geoserver-s3-reader-policy-${terraform.workspace}"
  user = aws_iam_user.s3_reader.name

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.krm_validatie_bucket.arn,
          "${aws_s3_bucket.krm_validatie_bucket.arn}/*"
        ]
      },
    ]
  })
}

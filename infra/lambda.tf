# Lambda Function
resource "aws_lambda_function" "krm_validatie_lambda" {
  function_name = "krm-validatie-lambda-${terraform.workspace}"
  runtime       = "python3.11"
  role          = aws_iam_role.function_role.arn
  handler       = "krm-validatie.lambda_handler"
  filename      = "functions/validatie/krm-validatie.zip"  # Make sure to create and upload this file
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout       = 900
  memory_size   = 2048
  ephemeral_storage {
    size = 1024
  }

  layers = [
    "arn:aws:lambda:eu-west-1:637423531264:layer:geopandas:2",
    "arn:aws:lambda:eu-west-1:637423531264:layer:tabulate:2"
  ]
}

# Lambda Function
resource "aws_lambda_function" "krm_publicatie_lambda" {
  function_name = "krm-publicatie-lambda-${terraform.workspace}"
  runtime       = "python3.11"
  role          = aws_iam_role.function_role.arn
  handler       = "krm-publicatie.lambda_handler"
  filename      = "functions/publicatie/krm-publicatie.zip"  # Make sure to create and upload this file
  source_code_hash = data.archive_file.lambda_publicatie.output_base64sha256
  timeout       = 900
  memory_size   = 1024

  layers = [
    "arn:aws:lambda:eu-west-1:637423531264:layer:geopandas:2"
  ]
}

# Create the function
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "functions/validatie/krm-validatie.py"
  output_path = "functions/validatie/krm-validatie.zip"
}

# Create the function
data "archive_file" "lambda_publicatie" {
  type        = "zip"
  source_file = "functions/publicatie/krm-publicatie.py"
  output_path = "functions/publicatie/krm-publicatie.zip"
}

# IAM policy document for accessing Secrets Manager
data "aws_iam_policy_document" "lambda_secrets_manager_policy" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue",
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "lambda_s3_policy" {
  statement {
    actions = [
      "s3:*"
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
}

resource "aws_iam_role" "function_role" {
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  ]
  inline_policy {
    name   = "lambda_secrets_manager_policy"
    policy = data.aws_iam_policy_document.lambda_secrets_manager_policy.json
  }

  inline_policy {
    name   = "lambda_s3_policy"
    policy = data.aws_iam_policy_document.lambda_s3_policy.json
  }
}

resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.krm_validatie_lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.krm_validatie_bucket.arn
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.krm_validatie_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.krm_validatie_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "input/"
    filter_suffix       = ".zip"
  }

  depends_on = [aws_lambda_permission.allow_bucket]
}

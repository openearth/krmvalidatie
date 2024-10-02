# Lambda Function
resource "aws_lambda_function" "krm_validatie_lambda" {
  function_name = "krm-validatie-lambda"
  runtime       = "python3.8"
  role          = aws_iam_role.function_role.arn
  handler       = "krm-validatie.lambda_handler"
  filename      = "functions/src/krm-validatie.zip"  # Make sure to create and upload this file
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout       = 120

  layers = [
    # "arn:aws:lambda:eu-west-1:336392948345:layer:AWSDataWrangler-Python38:1",
    "arn:aws:lambda:eu-west-1:637423531264:layer:geopandas:1"
  ]

  # environment {
  #   #   variables = {
  #   #     DB_HOST     = aws_db_instance.postgres.address
  #   #     DB_NAME     = aws_db_instance.postgres.name
  #   #     DB_USER     = aws_db_instance.postgres.username
  #   #     DB_PASSWORD = aws_db_instance.postgres.password
  #   #   }
  # }

  # vpc_config {
  #   security_group_ids = [aws_security_group.lambda_sg.id]
  #   subnet_ids         = aws_subnet.public.*.id
  # }
}

# resource "aws_lambda_layer_version" "pandas_layer" {
#   filename   = "pandas_layer.zip"
#   layer_name = "pandas_layer"
#   compatible_runtimes = ["python3.8"]
# }

# Create the function
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "functions/src/krm-validatie.py"
  output_path = "functions/src/krm-validatie.zip"
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

# # (Optional) Lambda Permission to Access RDS
# resource "aws_iam_policy" "lambda_rds_policy" {
#   name   = "lambda_rds_policy"
#   policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Effect   = "Allow",
#         Action   = ["rds:Connect"],
#         Resource = "*"
#       }
#     ]
#   })
# }

# resource "aws_iam_role_policy_attachment" "lambda_rds_policy_attachment" {
#   role       = aws_iam_role.lambda_role.name
#   policy_arn = aws_iam_policy.lambda_rds_policy.arn
# }
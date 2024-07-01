# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "krm-validatie-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
}

# Attach IAM policy to Lambda Role
resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLambdaBasicExecutionRole"
}

# Lambda Function
resource "aws_lambda_function" "krm_validatie_lambda" {
  function_name = "krm-validatie-lambda"
  runtime       = "python3.8"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  filename      = "lambda_function_payload.zip"  # Make sure to create and upload this file
  source_code_hash = filebase64sha256("lambda_function_payload.zip")

  vpc_config {
    security_group_ids = [aws_security_group.lambda_sg.id]
    subnet_ids         = aws_subnet.public.*.id
  }
}

# Lambda Function Environment Variables
resource "aws_lambda_function_environment" "krm_validatie_lambda_env" {
  function_name = aws_lambda_function.krm_validatie_lambda.function_name
  variables = {
    DB_HOST     = aws_db_instance.postgres.address
    DB_NAME     = aws_db_instance.postgres.name
    DB_USER     = aws_db_instance.postgres.username
    DB_PASSWORD = aws_db_instance.postgres.password
  }
}

# Create Lambda Function Payload (ZIP File)
resource "local_file" "lambda_zip" {
  filename = "lambda_function_payload.zip"
  content  = base64decode(filebase64("path_to_your_lambda_function_code.zip"))
}

# (Optional) Lambda Permission to Access RDS
resource "aws_iam_policy" "lambda_rds_policy" {
  name   = "lambda_rds_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["rds:Connect"],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_rds_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_rds_policy.arn
}
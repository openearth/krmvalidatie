

data "archive_file" "lambda_transformatie" {
  type        = "zip"
  source_file = "functions/transformatie-${terraform.workspace}/krm-transformatie.py"
  output_path = "functions/transformatie-${terraform.workspace}/krm-transformatie.zip"
}

resource "aws_lambda_function" "krm_transformatie_lambda" {
  function_name    = "krm-transformatie-lambda-${terraform.workspace}"
  runtime          = "python3.11"
  role             = aws_iam_role.function_role.arn
  handler          = "krm-transformatie.lambda_handler"
  filename         = "functions/transformatie-${terraform.workspace}/krm-transformatie.zip"
  source_code_hash = data.archive_file.lambda_transformatie.output_base64sha256
  timeout          = 900
  memory_size      = 2048

  layers = [
    # the geopandas layer supplies pandas
    "arn:aws:lambda:eu-west-1:637423531264:layer:geopandas:2"
  ]

  environment {
    variables = {
      KRM_BUCKET_NAME   = "${var.bucket_name}-${terraform.workspace}"
      KRM_OUTPUT_PREFIX = "transformed"
    }
  }
}

# SNS topic for starting a transformation
resource "aws_sns_topic" "transform_data" {
  name = "TransformData-${terraform.workspace}"
}

# Allow the SNS topic to invoke the transformation lambda
resource "aws_lambda_permission" "allow_sns_transform_data" {
  statement_id  = "AllowExecutionFromSNSTransformData"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.krm_transformatie_lambda.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.transform_data.arn
}

# Connect the SNS topic to the transformation lambda
resource "aws_sns_topic_subscription" "transform_data_lambda" {
  topic_arn = aws_sns_topic.transform_data.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.krm_transformatie_lambda.arn

  depends_on = [aws_lambda_permission.allow_sns_transform_data]
}

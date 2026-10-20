data "archive_file" "lambda_downloading_validatielijst" {
  type        = "zip"
  source_file = "functions/downloading-${terraform.workspace}/krm-downloading-validatielijst.py"
  output_path = "functions/downloading-${terraform.workspace}/krm-downloading-validatielijst.zip"
}

resource "aws_lambda_function" "krm_downloading_validatielijst_lambda" {
  function_name    = "krm-downloading-validatielijst-lambda-${terraform.workspace}"
  runtime          = "python3.11"
  role             = aws_iam_role.function_role.arn
  handler          = "krm-downloading-validatielijst.lambda_handler"
  filename         = "functions/downloading-${terraform.workspace}/krm-downloading-validatielijst.zip"
  source_code_hash = data.archive_file.lambda_downloading_validatielijst.output_base64sha256
  timeout          = 900
  memory_size      = 2048

  layers = [
    # geopandas layer supplies pandas, the rws-waterinfo layer the Waterinfo client
    "arn:aws:lambda:eu-west-1:637423531264:layer:geopandas:2",
    "arn:aws:lambda:eu-west-1:637423531264:layer:rws-waterinfo:1"
  ]

  environment {
    variables = {
      KRM_BUCKET_NAME = "${var.bucket_name}-${terraform.workspace}"
      KRM_S3_PREFIX   = "downloaded"
    }
  }
}

# SNS topic for starting a download
resource "aws_sns_topic" "download_validatielijst" {
  name = "DownloadValidatielijst-${terraform.workspace}"
}

# Allow the SNS topic to invoke the downloading lambda
resource "aws_lambda_permission" "allow_sns_download_validatielijst" {
  statement_id  = "AllowExecutionFromSNSDownloadValidatielijst"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.krm_downloading_validatielijst_lambda.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.download_validatielijst.arn
}

# Connect the SNS topic to the downloading lambda
resource "aws_sns_topic_subscription" "download_validatielijst_lambda" {
  topic_arn = aws_sns_topic.download_validatielijst.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.krm_downloading_validatielijst_lambda.arn

  depends_on = [aws_lambda_permission.allow_sns_download_validatielijst]
}

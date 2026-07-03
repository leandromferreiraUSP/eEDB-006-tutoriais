# main.tf -- Infraestrutura AWS de STREAMING POR FILAS (Tutorial 2 - AWS), no Learner Lab.
# Provisiona a topologia serverless:
#
#   producer (sua máquina) --> SQS (vendas-queue) --> Lambda (micro-lote) --> S3 (Parquet)
#
# - S3 bucket   : data lake de DESTINO.
# - SQS queue   : fila de eventos (transporte).
# - Lambda      : consumidor. Recebe um LOTE de mensagens por invocação (batch_size /
#                 batch_window) e grava 1 Parquet por lote. Usa a role LabRole e a layer
#                 gerenciada "AWS SDK for pandas" (pandas + pyarrow) para escrever Parquet.
# - Event source mapping : liga o SQS na Lambda (a Lambda faz o polling da fila).
#
# O código da Lambda (handler.py) você cria em build/handler.py seguindo o TUTORIAL.md;
# o Terraform empacota esse arquivo em .zip automaticamente (data.archive_file abaixo).

data "aws_caller_identity" "current" {}

# Role pré-existente do Learner Lab (não criamos roles novas no Lab).
data "aws_iam_role" "lab" {
  name = var.lambda_role_name
}

locals {
  bucket_name = "${data.aws_caller_identity.current.account_id}-streaming-lab"
  # Layer pública "AWS SDK for pandas" (conta AWS 336392948345). Traz pandas + pyarrow.
  pandas_layer_arn = "arn:aws:lambda:${var.aws_region}:336392948345:layer:AWSSDKPandas-Python312:${var.pandas_layer_version}"
}

# ----------------------------------------------------------------- S3 (destino)
resource "aws_s3_bucket" "datalake" {
  bucket        = local.bucket_name
  force_destroy = true

  tags = {
    Name      = local.bucket_name
    Project   = "streaming-lab"
    ManagedBy = "terraform"
  }
}

# ----------------------------------------------------------------- SQS (fila de eventos)
resource "aws_sqs_queue" "vendas" {
  name                       = var.queue_name
  message_retention_seconds  = 3600
  # A visibility timeout precisa ser >= timeout da Lambda (recomendado ~6x).
  visibility_timeout_seconds = var.lambda_timeout * 6

  tags = {
    Name    = var.queue_name
    Project = "streaming-lab"
  }
}

# ----------------------------------------------------------------- Lambda (consumidor)
# Empacota build/handler.py (que VOCÊ cria seguindo o tutorial) em um .zip.
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/build/handler.py"
  output_path = "${path.module}/build/handler.zip"
}

resource "aws_lambda_function" "consumer" {
  function_name    = "vendas-consumer"
  role             = data.aws_iam_role.lab.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = var.lambda_timeout
  memory_size      = 512

  # pandas + pyarrow para gravar Parquet, via layer gerenciada.
  layers = [local.pandas_layer_arn]

  environment {
    variables = {
      BUCKET = aws_s3_bucket.datalake.bucket
      PREFIX = "filas"
    }
  }

  tags = {
    Name    = "vendas-consumer"
    Project = "streaming-lab"
  }
}

# ----------------------------------------------------------------- SQS -> Lambda
resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn                   = aws_sqs_queue.vendas.arn
  function_name                      = aws_lambda_function.consumer.arn
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.batch_window_seconds
  function_response_types            = ["ReportBatchItemFailures"]
}

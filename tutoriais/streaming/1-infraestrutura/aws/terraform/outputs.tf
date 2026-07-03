output "s3_bucket" {
  description = "Nome do bucket S3 (data lake de destino)."
  value       = aws_s3_bucket.datalake.bucket
}

output "sqs_queue_url" {
  description = "URL da fila SQS (use no producer para enviar eventos)."
  value       = aws_sqs_queue.vendas.url
}

output "sqs_queue_arn" {
  description = "ARN da fila SQS."
  value       = aws_sqs_queue.vendas.arn
}

output "lambda_function_name" {
  description = "Nome da Lambda consumidora (veja logs em CloudWatch)."
  value       = aws_lambda_function.consumer.function_name
}

output "pandas_layer_arn" {
  description = "ARN da layer AWS SDK for pandas em uso."
  value       = local.pandas_layer_arn
}

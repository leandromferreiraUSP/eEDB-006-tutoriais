variable "aws_region" {
  description = "Região AWS (Learner Lab: us-east-1)."
  type        = string
  default     = "us-east-1"
}

variable "lambda_role_name" {
  description = "Role IAM pré-criada do Learner Lab usada pela Lambda (tem acesso a S3/SQS/Logs)."
  type        = string
  default     = "LabRole"
}

variable "queue_name" {
  description = "Nome da fila SQS que recebe os eventos de venda."
  type        = string
  default     = "vendas-queue"
}

variable "lambda_timeout" {
  description = "Timeout (s) da Lambda consumidora."
  type        = number
  default     = 60
}

variable "batch_size" {
  description = "Máx. de mensagens que a Lambda recebe por invocação (o 'micro-lote')."
  type        = number
  default     = 100
}

variable "batch_window_seconds" {
  description = "Tempo máx. (s) que o SQS espera acumulando mensagens antes de invocar a Lambda."
  type        = number
  default     = 30
}

variable "pandas_layer_version" {
  description = <<-EOT
    Versão da layer gerenciada 'AWS SDK for pandas' (AWSSDKPandas-Python312), que traz
    pandas + pyarrow prontos para a Lambda gravar Parquet.
    ATENÇÃO: no Learner Lab o 'aws lambda list-layer-versions' costuma dar AccessDenied
    (a layer é de outra conta, 336392948345). Descubra/confirme a versão atual na doc oficial:
      https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html  (coluna us-east-1, Python 3.12)
    e ajuste aqui com -var="pandas_layer_version=NN" se necessário.
  EOT
  type        = number
  default     = 29
}

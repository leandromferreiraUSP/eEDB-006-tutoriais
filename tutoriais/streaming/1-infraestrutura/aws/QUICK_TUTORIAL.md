# Quick Tutorial 1 (AWS): Infraestrutura de Streaming por Filas (SQS + Lambda + S3)

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: SQS (`vendas-queue`) + Lambda (`vendas-consumer`) + S3 provisionados via Terraform.

---

## 1. Pré-requisitos (uma vez)

- **macOS**: `brew install hashicorp/tap/terraform awscli python@3.12`
- **Ubuntu**: Terraform (repo HashiCorp) · AWS CLI v2 · `python3.12`
- **Windows**: `winget install Hashicorp.Terraform Amazon.AWSCLI Python.Python.3.12`

---

## 2. Credenciais do Learner Lab

```bash
# macOS / Linux
mkdir -p ~/.aws
cp tutoriais/aws_credenciais/credentials ~/.aws/credentials
cp tutoriais/aws_credenciais/config      ~/.aws/config
aws sts get-caller-identity              # confirma identidade
```

```powershell
# Windows: copie credentials/config para $env:USERPROFILE\.aws\
```

---

## 3. Criar o código da Lambda

```bash
mkdir -p tutoriais/streaming/1-infraestrutura/aws/terraform/build
# crie terraform/build/handler.py com o conteúdo do TUTORIAL.md (seção 7)
```

---

## 4. Provisionar

```bash
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform init
terraform apply        # yes
terraform output       # anote sqs_queue_url e s3_bucket
```

> Default da layer pandas = **29** (Python 3.12, us-east-1). Se falhar dizendo que a layer não
> existe: `terraform apply -var="pandas_layer_version=NN"` — descubra NN na doc oficial
> <https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html> (no Learner Lab o
> `aws lambda list-layer-versions` dá AccessDenied).

---

## 5. Validar (envia 1 msg, espera ~30s, olha o S3)

```bash
QURL=$(terraform output -raw sqs_queue_url); BUCKET=$(terraform output -raw s3_bucket)
aws sqs send-message --queue-url "$QURL" --message-body \
  '{"evento_id":"t1","cliente_id":1,"produto_id":1,"categoria":"Eletronicos","quantidade":1,"valor_total":100.0,"data_venda":"2026-07-02T12:00:00.000"}'
sleep 30
aws s3 ls s3://$BUCKET/filas/ --recursive          # deve aparecer um lote-*.parquet
aws logs tail /aws/lambda/vendas-consumer --since 5m
```

---

## 6. Destruir (evite custos!)

```bash
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform destroy      # yes
```

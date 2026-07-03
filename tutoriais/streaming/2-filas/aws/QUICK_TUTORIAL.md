# Quick Tutorial 2 (AWS): Streaming por Filas (producer → SQS → Lambda → S3)

> Só os comandos. Explicações: `TUTORIAL.md`.
> **Pré-requisito**: `1-infraestrutura/aws` aplicado (SQS `vendas-queue` + Lambda
> `vendas-consumer` + S3). Aqui você só roda o **producer** e observa.

---

## 1. Credenciais + valores da infra

```bash
# credenciais do Lab (macOS / Linux)
mkdir -p ~/.aws
cp tutoriais/aws_credenciais/credentials ~/.aws/credentials
cp tutoriais/aws_credenciais/config      ~/.aws/config
aws sts get-caller-identity                       # confirma identidade

# URL da fila e bucket (outputs do Tutorial 1 AWS)
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform output -raw sqs_queue_url               # anote
terraform output -raw s3_bucket                   # anote
```

```powershell
# Windows: copie credentials/config para $env:USERPROFILE\.aws\
```

---

## 2. Ambiente Python (venv + boto3)

```bash
cd tutoriais/streaming/2-filas/aws
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install boto3
```

---

## 3. Criar o producer

```bash
# crie producer.py com o conteúdo do TUTORIAL.md (seção 6.2)
```

Resumo do que ele faz: gera evento de venda (mesmo catálogo dos outros tutoriais) e chama
`sqs.send_message(QueueUrl=..., MessageBody=json)` em loop, `EPS` vezes/s.

---

## 4. Rodar o producer (deixe rodando)

```bash
python producer.py "<sqs_queue_url>" 8            # 8 eventos/s
# ou pegando a URL direto do terraform:
python producer.py "$(cd ../../1-infraestrutura/aws/terraform && terraform output -raw sqs_queue_url)" 8
```

Esperado: `10 eventos enviados`, `20 eventos enviados`, ...

---

## 5. Observar o fluxo (outro terminal)

```bash
# logs da Lambda (aparece "gravado N eventos em s3://.../lote-....parquet")
aws logs tail /aws/lambda/vendas-consumer --since 5m --follow

# profundidade da fila
QURL=$(cd tutoriais/streaming/1-infraestrutura/aws/terraform && terraform output -raw sqs_queue_url)
aws sqs get-queue-attributes --queue-url "$QURL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

---

## 6. Verificar no S3

```bash
BUCKET=$(cd tutoriais/streaming/1-infraestrutura/aws/terraform && terraform output -raw s3_bucket)
aws s3 ls s3://$BUCKET/filas/ --recursive          # vários lote-*.parquet

# baixar 1 e ler (7 colunas do evento)
pip install pyarrow
DT=$(date -u +%F)
KEY=$(aws s3 ls s3://$BUCKET/filas/dt=$DT/ | awk '{print $4}' | head -1)
aws s3 cp s3://$BUCKET/filas/dt=$DT/$KEY /tmp/lote.parquet
python -c "import pyarrow.parquet as pq; t=pq.read_table('/tmp/lote.parquet'); print(t.column_names); print(t.to_pylist()[:2])"
```

---

## 7. Parar e limpar

```bash
# Ctrl+C no producer (imprime total: N). A fila esvazia e a Lambda para de ser invocada.
# Destruir a infra (evite custos!) -> feito no Tutorial 1 AWS:
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform destroy      # yes
```

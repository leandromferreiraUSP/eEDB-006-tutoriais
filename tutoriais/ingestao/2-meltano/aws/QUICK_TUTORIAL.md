# Quick Tutorial 2 (AWS): Ingestão com Meltano na Nuvem

> Só os comandos. Explicações: `TUTORIAL.md`.
> Pré-requisito: `1-infraestrutura/aws` provisionado e RDS populado. Destino: `s3://<bucket>/meltano/`.

---

## 1. Endereços (na sua máquina)

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform output       # ec2_public_ip, rds_endpoint, s3_bucket
ssh -i ~/.ssh/labsuser.pem ec2-user@<EC2_PUBLIC_IP>
```

## 2. Na EC2: ambiente + projeto

```bash
python3.11 -m venv ~/meltano-venv && source ~/meltano-venv/bin/activate
pip install "meltano==4.2.1"

export MELTANO_SEND_ANONYMOUS_USAGE_STATS=False
meltano init projeto_ingestao && cd projeto_ingestao
meltano add --plugin-type extractor tap-postgres
meltano add --plugin-type extractor tap-rest-api-msdk
meltano add --plugin-type loader target-parquet --variant automattic
```

Edite o `meltano.yml` igual ao `2-meltano/local`, **mudando só** `tap-postgres host:` para o
`<RDS_ENDPOINT>`. Depois:

```bash
echo "TAP_POSTGRES_PASSWORD=ecommerce123" > .env
meltano install extractor tap-rest-api-msdk
```

## 3. Rodar + publicar no S3 real

```bash
meltano run tap-postgres target-parquet
meltano run tap-rest-api-msdk target-parquet

BUCKET=<s3_bucket>     # ex.: 849967252385-ingestao-lab
aws s3 sync output/public-vendas s3://$BUCKET/meltano/vendas/  --exclude "*" --include "*.parquet"
aws s3 sync output/pokemon       s3://$BUCKET/meltano/pokemon/ --exclude "*" --include "*.parquet"
aws s3 ls s3://$BUCKET/meltano/ --recursive
```

> No S3 real: SEM `--endpoint-url` e SEM `AWS_REQUEST_CHECKSUM_CALCULATION` (creds vêm do LabInstanceProfile).

## 4. Limpar

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform && terraform destroy
```

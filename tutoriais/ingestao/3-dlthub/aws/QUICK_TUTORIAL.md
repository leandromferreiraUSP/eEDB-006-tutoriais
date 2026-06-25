# Quick Tutorial 3 (AWS): Ingestão com DLTHub (dlt) + Python na Nuvem

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: `vendas` (RDS) e PokéAPI ingeridas em **Parquet** no S3 real
> (`s3://<account_id>-ingestao-lab/postgres/vendas/` e `.../pokeapi/pokemon/`), rodando na EC2.

**Pré-requisito**: Tutorial 1 (AWS) provisionado — RDS, S3 (`<account_id>-ingestao-lab`) e EC2
no ar. Valores vêm de `terraform output`.

---

## 1. Outputs + conectar na EC2

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform output            # rds_endpoint, s3_bucket, ec2_public_ip
ssh -i ~/.ssh/labsuser.pem ec2-user@<EC2_IP>
```

---

## 2. Ambiente Python na EC2 (Amazon Linux 2023)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install "dlt[filesystem,sql_database,parquet]" psycopg2-binary
```

> Versões de referência: dlt 1.28.1, pyarrow 24.0.0, s3fs 2026.6.0.

---

## 3. Destino S3 real (sem endpoint_url, sem chaves — usa o IAM role)

```bash
export DESTINATION__FILESYSTEM__BUCKET_URL="s3://<account_id>-ingestao-lab"
export DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME="us-east-1"
```

Alternativa `.dlt/secrets.toml`:

```toml
[destination.filesystem]
bucket_url = "s3://<account_id>-ingestao-lab"
[destination.filesystem.credentials]
region_name = "us-east-1"
```

---

## 4. Pipeline Postgres (RDS) → S3

Crie `pipeline_postgres.py` (host = `rds_endpoint`, senha = `ecommerce123`):

```python
import dlt
from dlt.sources.sql_database import sql_table

pipe = dlt.pipeline(
    pipeline_name="ingestao_postgres",
    destination="filesystem",
    dataset_name="postgres",
)
fonte = sql_table(
    credentials="postgresql://ecommerce:ecommerce123@<RDS_ENDPOINT>:5432/ecommerce",
    table="vendas",
)
info = pipe.run(fonte, loader_file_format="parquet")
print(info)
```

```bash
python pipeline_postgres.py
```

---

## 5. Pipeline PokéAPI → S3

Crie `pipeline_api.py` (igual ao local):

```python
import dlt
from dlt.sources.rest_api import rest_api_source

fonte = rest_api_source({
    "client": {"base_url": "https://pokeapi.co/api/v2/"},
    "resources": [
        {
            "name": "pokemon",
            "endpoint": {
                "path": "pokemon",
                "data_selector": "results",
                "paginator": {
                    "type": "offset",
                    "limit": 100,
                    "offset": 0,
                    "limit_param": "limit",
                    "offset_param": "offset",
                    "total_path": "count",
                    "maximum_offset": 200,
                },
            },
        }
    ],
})
pipe = dlt.pipeline(
    pipeline_name="ingestao_pokeapi",
    destination="filesystem",
    dataset_name="pokeapi",
)
info = pipe.run(fonte, loader_file_format="parquet")
print(info)
```

```bash
python pipeline_api.py
```

---

## 6. Validar (da sua máquina — S3 real, sem --endpoint-url)

```bash
aws s3 ls s3://<account_id>-ingestao-lab/ --recursive
```

**Resultado esperado**: linhas com `postgres/vendas/....parquet` e `pokeapi/pokemon/....parquet`.

---

## 7. Destruir (ao final — evita custos!)

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform destroy     # yes
```

> `AccessDenied` no S3 → EC2 precisa do `LabInstanceProfile` (Tutorial 1 AWS) e bucket do output.
> Conexão Postgres recusada → use o `rds_endpoint` e rode **de dentro da EC2**.

# Quick Tutorial 3 (Local): Ingestão com DLTHub (dlt) + Python

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: tabela `vendas` (Postgres) e PokéAPI ingeridas em **Parquet** no S3 local
> (`s3://datalake/postgres/vendas/` e `s3://datalake/pokeapi/pokemon/`).

**Pré-requisito**: Tutorial 1 (Local) no ar — Postgres `localhost:5432` (db `ecommerce`) e
MiniStack S3 `http://localhost:4566` (bucket `datalake`).

---

## 1. Instalar (Python 3.12 + venv)

```bash
cd tutoriais/ingestao/3-dlthub/local            # crie tudo nesta pasta
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\Activate.ps1
pip install "dlt[filesystem,sql_database,parquet]" psycopg2-binary
```

> Versões de referência: dlt 1.28.1, pyarrow 24.0.0, s3fs 2026.6.0.

---

## 2. Configurar o destino S3 (MiniStack)

```bash
# macOS / Linux  (Windows: $env:NOME="valor")
export DESTINATION__FILESYSTEM__BUCKET_URL="s3://datalake"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="test"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="test"
export DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL="http://localhost:4566"
export DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME="us-east-1"
```

Alternativa: arquivo `.dlt/secrets.toml`

```toml
[destination.filesystem]
bucket_url = "s3://datalake"
[destination.filesystem.credentials]
aws_access_key_id = "test"
aws_secret_access_key = "test"
endpoint_url = "http://localhost:4566"
region_name = "us-east-1"
```

---

## 3. Pipeline Postgres → S3

Crie `pipeline_postgres.py`:

```python
import dlt
from dlt.sources.sql_database import sql_table

pipe = dlt.pipeline(
    pipeline_name="ingestao_postgres",
    destination="filesystem",
    dataset_name="postgres",
)
fonte = sql_table(
    credentials="postgresql://ecommerce:ecommerce@localhost:5432/ecommerce",
    table="vendas",
)
info = pipe.run(fonte, loader_file_format="parquet")
print(info)
```

```bash
python pipeline_postgres.py
```

---

## 4. Pipeline PokéAPI → S3

Crie `pipeline_api.py`:

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

## 5. Validar

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/ --recursive
```

**Resultado esperado**: linhas com `postgres/vendas/....parquet` e `pokeapi/pokemon/....parquet`.

> `ModuleNotFoundError: dlt.sources.sql_database` → `pip install "dlt[sql_database]"`.
> Conexão Postgres recusada → suba os containers do Tutorial 1.

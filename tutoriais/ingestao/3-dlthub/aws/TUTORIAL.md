# Tutorial 3 (AWS): Ingestão de Dados com DLTHub (dlt) + Python na Nuvem

> Versão **longa e explicativa**. A mesma ingestão do tutorial local, agora na **AWS**: o
> **dlt** roda **dentro de uma EC2**, lê a tabela `vendas` de um **RDS PostgreSQL** e os dados
> da **PokéAPI**, e grava tudo em **Parquet** num **bucket S3 real**. A grande diferença é que
> a EC2 usa o **LabInstanceProfile** (papel IAM), então o dlt pega as credenciais da AWS
> **automaticamente** — sem chaves no código.
>
> Só os comandos? Veja o `QUICK_TUTORIAL.md`.

**Pré-requisito**: ter concluído o **Tutorial 1 (AWS)** (`1-infraestrutura/aws`), que provisiona
via Terraform o **RDS PostgreSQL** (origem, já populado com `vendas`), o **bucket S3**
(`<account_id>-ingestao-lab`) e a **EC2 runner** (Amazon Linux 2023). Os valores (`rds_endpoint`,
`s3_bucket`, `ec2_public_ip`, `ssh_command`) vêm do `terraform output`. Deixe RDS e EC2
**ligados** e lembre de **destruir** ao final (seção 7).

---

## Sumário

1. [Arquitetura na AWS](#1-arquitetura-na-aws)
2. [O que muda em relação ao tutorial local](#2-o-que-muda-em-relação-ao-tutorial-local)
3. [Conectar na EC2 e pegar os outputs](#3-conectar-na-ec2-e-pegar-os-outputs)
4. [Preparar o ambiente Python na EC2](#4-preparar-o-ambiente-python-na-ec2)
5. [Configurar o destino S3 real](#5-configurar-o-destino-s3-real)
6. [Rodar as pipelines e validar](#6-rodar-as-pipelines-e-validar)
7. [Destruir tudo (evite custos!)](#7-destruir-tudo-evite-custos)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Arquitetura na AWS

```
            ┌──────────────────────────── AWS (us-east-1) ───────────────────────────┐
            │                          default VPC                                    │
  você ─SSH─►  ┌──────────────────┐    psql(5432)    ┌──────────────────────┐         │
            │  │   EC2 runner     │  ─────────────►  │  RDS PostgreSQL       │  ORIGEM │
            │  │  dlt + Python    │                  │  tabela vendas        │         │
            │  │ LabInstanceProf  │                  └──────────────────────┘         │
            │  └────────┬─────────┘                                                    │
            │           │ s3:PutObject (via IAM role do Lab — sem chaves no código)    │
            │           ▼                                                              │  DESTINO
            │   ┌──────────────────────────┐         PokéAPI entra pela internet      │  (data lake)
            │   │ S3 <conta>-ingestao-lab  │ ◄─── e também é gravada aqui (Parquet)    │
            │   └──────────────────────────┘                                          │
            └──────────────────────────────────────────────────────────────────────────┘
```

- A **EC2** é onde o dlt roda (você conecta nela por SSH).
- A origem **banco** é o **RDS** (privado, acessível de dentro da VPC — por isso rodamos na EC2).
- O destino é o **bucket S3 real** criado pelo Terraform.
- A **PokéAPI** continua sendo a mesma API pública, acessada pela internet.

---

## 2. O que muda em relação ao tutorial local

O código das pipelines é **o mesmo**. São só **três diferenças**:

| Aspecto | Local (`3-dlthub/local`) | AWS (este tutorial) |
|---|---|---|
| **Onde roda** | sua máquina | **dentro da EC2** (SSH) |
| **Origem Postgres** | `...@localhost:5432/ecommerce`, senha `ecommerce` | `...@<RDS_ENDPOINT>:5432/ecommerce`, senha `ecommerce123` |
| **Destino S3** | bucket `datalake` + `endpoint_url` do MiniStack + chaves `test` | bucket real, **sem `endpoint_url` e sem chaves** (usa o IAM role) |

> **Por que sem chaves no destino?** A EC2 foi criada com o **LabInstanceProfile** (Tutorial 1
> AWS). O `botocore`/`s3fs` que o dlt usa segue a **cadeia de credenciais padrão da AWS** e
> encontra automaticamente as credenciais do papel da instância. Logo, no S3 real basta dizer
> o `bucket_url` e a `region_name`.

---

## 3. Conectar na EC2 e pegar os outputs

Na **sua máquina**, na pasta do Terraform, releia os outputs do Tutorial 1 (AWS):

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform output
```

**Resultado esperado** (valores variam):

```
ec2_public_ip  = "54.x.x.x"
rds_endpoint   = "ingestao-postgres.xxxxx.us-east-1.rds.amazonaws.com"
s3_bucket      = "849967252385-ingestao-lab"
ssh_command    = "ssh -i ~/.ssh/labsuser.pem ec2-user@54.x.x.x"
```

Anote o **`rds_endpoint`** e o **`s3_bucket`** (você vai colá-los nos arquivos Python e na
config). Conecte na EC2 com o `ssh_command` (ou diretamente):

```bash
ssh -i ~/.ssh/labsuser.pem ec2-user@<EC2_IP>
```

> Se as credenciais do Learner Lab expiraram, o `terraform output` ainda funciona (lê o estado
> local), mas o SSH e o S3 precisam da sessão ativa — reabra o Lab e recopie `credentials` se
> necessário (veja o Tutorial 1 AWS).

---

## 4. Preparar o ambiente Python na EC2

**Já dentro da EC2** (Amazon Linux 2023, que traz `python3.11`), crie um venv e instale o dlt —
exatamente os mesmos extras do tutorial local:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install "dlt[filesystem,sql_database,parquet]" psycopg2-binary
```

Confira:

```bash
dlt --version
```

**Resultado esperado** (algo como):

```
dlt 1.28.1
```

> Versões de referência (testadas): dlt 1.28.1, pyarrow 24.0.0, s3fs 2026.6.0.

---

## 5. Configurar o destino S3 real

Ainda na EC2, configure o destino apontando para o **bucket real**. Note que **não** há
`endpoint_url` nem chaves de acesso — o dlt usa o LabInstanceProfile.

### 5.1 — Forma A: variáveis de ambiente

```bash
export DESTINATION__FILESYSTEM__BUCKET_URL="s3://<account_id>-ingestao-lab"
export DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME="us-east-1"
```

(Troque `<account_id>-ingestao-lab` pelo valor de `terraform output -raw s3_bucket`.)

### 5.2 — Forma B: arquivo `.dlt/secrets.toml`

Crie `.dlt/secrets.toml` com **apenas** o bucket e a região (sem `endpoint_url`, sem chaves):

```toml
[destination.filesystem]
bucket_url = "s3://<account_id>-ingestao-lab"

[destination.filesystem.credentials]
region_name = "us-east-1"
```

> Compare com o tutorial local: lá tínhamos `endpoint_url`, `aws_access_key_id` e
> `aws_secret_access_key`. Aqui some tudo isso — é a única mudança de configuração do destino.

---

## 6. Rodar as pipelines e validar

### 6.1 — Pipeline Postgres (RDS) → S3

Ainda na EC2, **crie** `pipeline_postgres.py`. É idêntico ao local, **trocando só a credencial**
para apontar ao RDS (host = `rds_endpoint`, senha = `ecommerce123`, sua `db_password`):

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

Execute:

```bash
python pipeline_postgres.py
```

**Resultado esperado**: um resumo de carga `LOADED`, gravando em
`s3://<account_id>-ingestao-lab/postgres/vendas/*.parquet`.

### 6.2 — Pipeline PokéAPI → S3

**Crie** `pipeline_api.py` — este é **igual** ao do tutorial local (a PokéAPI é a mesma):

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

Execute:

```bash
python pipeline_api.py
```

**Resultado esperado**: carga `LOADED`, gravando em
`s3://<account_id>-ingestao-lab/pokeapi/pokemon/*.parquet`.

### 6.3 — Validar (da sua máquina)

No **S3 real** você **não** usa `--endpoint-url` nem `AWS_REQUEST_CHECKSUM_CALCULATION` (aquilo
era específico do MiniStack local). Da sua máquina, com a sessão do Lab ativa:

```bash
aws s3 ls s3://<account_id>-ingestao-lab/ --recursive
```

**Resultado esperado** (caminhos com hash variam):

```
2026-06-25 18:20:11      12345 postgres/vendas/1718...0.parquet
2026-06-25 18:21:47       6789 pokeapi/pokemon/1718...0.parquet
```

Ou seja: linhas com **`postgres/vendas/....parquet`** e **`pokeapi/pokemon/....parquet`**.

> Para o nome do bucket sem digitar: `aws s3 ls s3://$(cd tutoriais/ingestao/1-infraestrutura/aws/terraform && terraform output -raw s3_bucket)/ --recursive`.

---

## 7. Destruir tudo (evite custos!)

Ao terminar, derrube a infraestrutura do Tutorial 1 (AWS) para não gastar o orçamento do Lab:

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform destroy     # digite "yes"
```

> `force_destroy = true` no bucket faz o Terraform apagar os objetos (inclusive os Parquet que
> você acabou de gravar) junto com o bucket. Confirme no console que **RDS** e **EC2** sumiram.

---

## 8. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ModuleNotFoundError: dlt.sources.sql_database` | Faltou o extra de banco | `pip install "dlt[sql_database]"` (venv ativo na EC2) |
| `connection refused` / timeout ao Postgres | RDS fora do ar, ou host/porta errados, ou rodando fora da EC2 | Use o `rds_endpoint` correto; rode **de dentro da EC2** (o RDS é privado); confira o Tutorial 1 AWS |
| `AccessDenied` ao gravar no S3 | EC2 sem o **LabInstanceProfile**, ou bucket errado | Confira que a EC2 do Tutorial 1 AWS tem o `LabInstanceProfile`; use o bucket do `terraform output -raw s3_bucket` |
| `ExpiredToken` / credenciais expiraram | Sessão do Learner Lab acabou | Reabra o Lab e recopie `credentials` para `~/.aws/` (Tutorial 1 AWS) |
| PokéAPI lenta / poucos registros | `maximum_offset: 200` limita a ~200 pokémons (de propósito) | Aumente `maximum_offset` no `pipeline_api.py` |
| Erro de Parquet / `pyarrow` ausente | Faltou o extra `parquet` | `pip install "dlt[parquet]"` |

---

**Pronto!** Você ingeriu um **RDS** e uma **API REST** para um **S3 real** usando dlt + Python,
rodando numa EC2 — com as credenciais vindo do papel IAM, sem chaves no código. **Não esqueça
do `terraform destroy`** (seção 7) para evitar custos.

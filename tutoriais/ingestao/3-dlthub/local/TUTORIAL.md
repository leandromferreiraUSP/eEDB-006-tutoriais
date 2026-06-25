# Tutorial 3 (Local): Ingestão de Dados com DLTHub (dlt) + Python

> Versão **longa e explicativa**. Aqui você usa o **dlt** (data load tool) — uma
> biblioteca Python de ingestão — para ler a tabela `vendas` do **PostgreSQL** e os dados
> da **PokéAPI** (uma API REST pública e paginada) e gravar tudo em **Parquet** no seu
> **data lake S3** local (emulado pelo MiniStack). Tudo roda na sua máquina, com poucas
> linhas de Python — o dlt cuida da extração, da inferência de schema e da carga.
>
> Quer só os comandos? Veja o `QUICK_TUTORIAL.md`.

**Pré-requisito**: ter concluído o **Tutorial 1 (Local)** (`1-infraestrutura/local`) com os
containers **no ar** — Postgres (`localhost:5432`, db `ecommerce`, populado com a tabela
`vendas` de 200 linhas) e MiniStack S3 (`http://localhost:4566`, bucket `datalake`). Confira
com `docker compose ps` na pasta `1-infraestrutura/local/docker`.

---

## Sumário

1. [O que é o dlt e o que vamos construir](#1-o-que-é-o-dlt-e-o-que-vamos-construir)
2. [Conceitos do dlt](#2-conceitos-do-dlt)
3. [Instalação (Python 3.12 + venv)](#3-instalação-python-312--venv)
4. [Configurando o destino S3 (MiniStack)](#4-configurando-o-destino-s3-ministack)
5. [Pipeline 1: Postgres → S3 (Parquet)](#5-pipeline-1-postgres--s3-parquet)
6. [Pipeline 2: PokéAPI → S3 (Parquet)](#6-pipeline-2-pokéapi--s3-parquet)
7. [Validando os Parquet no S3 local](#7-validando-os-parquet-no-s3-local)
8. [Entendendo onde os arquivos caem](#8-entendendo-onde-os-arquivos-caem)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. O que é o dlt e o que vamos construir

O **dlt** (*data load tool*, em letras minúsculas mesmo) é uma biblioteca **Python**
open-source de ingestão de dados. Diferente de uma ferramenta de linha de comando, você o
usa **dentro do seu próprio script Python**: declara a **origem** (um banco, uma API, um
arquivo), declara o **destino** (S3, BigQuery, Postgres...) e chama `pipeline.run()`. O dlt
extrai os dados, **infere e normaliza o schema** automaticamente e carrega no destino no
formato que você pedir.

Neste tutorial vamos criar **duas pipelines** que gravam Parquet no mesmo data lake S3 local:

```
        ┌────────────────────── sua máquina ──────────────────────┐
        │                                                          │
 ORIGEM │  ┌──────────────┐                                        │
 (banco)│  │ PostgreSQL   │ ──► pipeline_postgres.py ──┐           │
        │  │ tabela vendas│      (dlt + sql_table)     │           │   DESTINO
        │  └──────────────┘                            ▼           │   (data lake)
        │                                       ┌──────────────┐   │
 ORIGEM │  ┌──────────────┐                     │ MiniStack S3 │   │
 (API)  │  │   PokéAPI    │ ──► pipeline_api.py ►│  datalake/   │   │
        │  │  /pokemon    │      (dlt rest_api)  └──────────────┘   │
        │  └──────────────┘                            :4566        │
        └──────────────────────────────────────────────────────────┘
```

- **Pipeline 1**: lê a tabela `vendas` do Postgres → grava em `s3://datalake/postgres/vendas/`.
- **Pipeline 2**: lê `https://pokeapi.co/api/v2/pokemon` (paginada) → grava em
  `s3://datalake/pokeapi/pokemon/`.

O mesmo código, com pequenos ajustes de configuração, roda contra o **S3 real** na versão AWS
(`3-dlthub/aws`).

---

## 2. Conceitos do dlt

Antes de codar, fixe o vocabulário do dlt — são poucos termos e eles se repetem:

| Termo | O que é |
|---|---|
| **`dlt.pipeline(...)`** | O objeto central. Liga uma **origem** a um **destino** e controla a carga. Recebe `pipeline_name`, `destination` e `dataset_name`. |
| **`destination`** | Para onde os dados vão. Aqui usamos `"filesystem"` (S3/local). O dlt também fala BigQuery, Snowflake, Postgres, DuckDB... |
| **`dataset_name`** | O "namespace" lógico da carga. No destino `filesystem` vira a **pasta de primeiro nível** dentro do bucket (ex.: `postgres/`, `pokeapi/`). |
| **Source / Resource** | A **origem** dos dados. Uma *source* pode ter vários *resources* (cada resource vira uma tabela no destino). |
| **`sql_table` / `sql_database`** | Fontes prontas para bancos relacionais. `sql_table` traz **uma** tabela; `sql_database` traz **várias**. |
| **`rest_api_source`** | Fonte **declarativa** para APIs REST: você descreve a URL base, os endpoints e a **paginação** em um dicionário, sem escrever o loop de requisições. |
| **`loader_file_format`** | O formato físico do arquivo carregado. Usaremos `"parquet"` (colunar, ótimo para data lake). |

> **Por que dlt e não um script `requests` + `to_parquet` na mão?** Porque o dlt resolve de
> graça as partes chatas: paginação da API, inferência de tipos, **normalização de schema**
> (nomes de colunas, tabelas aninhadas), nomes de arquivo, particionamento por carga e
> idempotência. Você foca na **origem** e no **destino**.

> **Inferência e normalização de schema**: o dlt olha os dados que chegam, deduz os tipos
> (inteiro, texto, timestamp...) e cria o schema do Parquet automaticamente. Também
> "achata"/normaliza estruturas aninhadas de JSON em tabelas — útil para a resposta da PokéAPI.

---

## 3. Instalação (Python 3.12 + venv)

Entre na pasta deste tutorial e crie um ambiente virtual Python isolado (todos os comandos e
arquivos a seguir — `pipeline_postgres.py`, `pipeline_api.py`, `.dlt/secrets.toml` — devem
ficar **nesta mesma pasta**):

```bash
# macOS / Linux
cd tutoriais/ingestao/3-dlthub/local
python3 -m venv .venv
source .venv/bin/activate
pip install "dlt[filesystem,sql_database,parquet]" psycopg2-binary
```

```powershell
# Windows (PowerShell)
cd tutoriais\ingestao\3-dlthub\local
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install "dlt[filesystem,sql_database,parquet]" psycopg2-binary
```

O que cada parte instala:

| Pacote / extra | Para quê |
|---|---|
| `dlt[filesystem]` | destino **filesystem** (S3, local) — usa `s3fs`/`botocore` por baixo |
| `dlt[sql_database]` | fonte de **banco relacional** (`sql_table` / `sql_database`, via SQLAlchemy) |
| `dlt[parquet]` | suporte a gravar **Parquet** (puxa o `pyarrow`) |
| `psycopg2-binary` | driver **PostgreSQL** que o SQLAlchemy usa para conectar |

**Versões de referência** (testado e funcionando): **dlt 1.28.1**, **pyarrow 24.0.0**,
**s3fs 2026.6.0**. Confira a do dlt:

```bash
dlt --version
```

**Resultado esperado** (algo como):

```
dlt 1.28.1
```

---

## 4. Configurando o destino S3 (MiniStack)

O destino `filesystem` do dlt precisa saber **qual bucket** usar e **como autenticar**. Como
o MiniStack emula a AWS na porta 4566, passamos um `endpoint_url` local e credenciais dummy
(`test`/`test`). Há **duas formas equivalentes** de configurar — escolha uma.

### 4.1 — Forma A: variáveis de ambiente (método verificado)

O dlt lê configuração de variáveis de ambiente seguindo o padrão
`DESTINATION__<DESTINO>__<CHAVE>` (note o **duplo underscore** `__` separando os níveis):

```bash
# macOS / Linux
export DESTINATION__FILESYSTEM__BUCKET_URL="s3://datalake"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="test"
export DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="test"
export DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL="http://localhost:4566"
export DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME="us-east-1"
```

```powershell
# Windows (PowerShell)
$env:DESTINATION__FILESYSTEM__BUCKET_URL="s3://datalake"
$env:DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="test"
$env:DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="test"
$env:DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL="http://localhost:4566"
$env:DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME="us-east-1"
```

> Essas variáveis valem só para a **sessão atual** do terminal. Se abrir um terminal novo,
> exporte de novo (ou use a Forma B, que fica salva em arquivo).

### 4.2 — Forma B: arquivo `.dlt/secrets.toml` (idiomática)

O dlt procura, na pasta onde você roda o script, um diretório `.dlt/` com um `secrets.toml`.
Crie o arquivo `.dlt/secrets.toml` com:

```toml
[destination.filesystem]
bucket_url = "s3://datalake"

[destination.filesystem.credentials]
aws_access_key_id = "test"
aws_secret_access_key = "test"
endpoint_url = "http://localhost:4566"
region_name = "us-east-1"
```

> As duas formas são **equivalentes**: a estrutura `destination.filesystem.credentials.*` do
> TOML é exatamente o que o `DESTINATION__FILESYSTEM__CREDENTIALS__*` representa nas variáveis
> de ambiente. Use o TOML se quiser deixar a config persistida no projeto (e fora do Git).

> **Boa notícia sobre o MiniStack**: o destino `filesystem` do dlt usa `s3fs`/`botocore`, que
> conversam direto com o endpoint do MiniStack — **não** é preciso o workaround de checksum
> `CRC64NVME` que o **AWS CLI v2** exigia no Tutorial 1. O dlt grava no S3 local sem ajuste extra.

---

## 5. Pipeline 1: Postgres → S3 (Parquet)

Agora o código. **Crie um arquivo** `pipeline_postgres.py` (na mesma pasta onde você ativou o
venv e configurou o destino) com o conteúdo:

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

Linha a linha:

- **`sql_table(credentials=..., table="vendas")`** — declara a origem: a tabela `vendas` do
  Postgres local. A `credentials` é a URL de conexão SQLAlchemy
  `postgresql://usuario:senha@host:porta/banco` (aqui `ecommerce:ecommerce@localhost:5432/ecommerce`).
- **`dlt.pipeline(...)`** — cria a pipeline. `destination="filesystem"` usa a config da seção 4;
  `dataset_name="postgres"` define a pasta de destino dentro do bucket.
- **`pipe.run(fonte, loader_file_format="parquet")`** — executa: extrai a tabela, infere o
  schema, e grava **Parquet** no S3. O `info` traz um resumo da carga (pacotes, linhas, destino).

Execute:

```bash
python pipeline_postgres.py
```

**Resultado esperado** (um resumo parecido com este):

```
Pipeline ingestao_postgres load step completed in ...
1 load package(s) were loaded to destination filesystem and into dataset postgres
The filesystem destination used s3://datalake location to store data
Load package ... is LOADED and contains no failed jobs
```

Os dados foram parar em `s3://datalake/postgres/vendas/*.parquet`.

> **Se der `ModuleNotFoundError: dlt.sources.sql_database`**: faltou o extra de banco —
> rode `pip install "dlt[sql_database]"`. Veja o Troubleshooting.

---

## 6. Pipeline 2: PokéAPI → S3 (Parquet)

A segunda origem é uma **API REST pública e paginada**: `https://pokeapi.co/api/v2/pokemon`.
O dlt traz a fonte declarativa `rest_api_source`, onde você descreve a API num dicionário —
sem escrever o loop de requisições nem o controle de paginação na mão.

**Crie um arquivo** `pipeline_api.py` com:

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

Entendendo o dicionário do `rest_api_source`:

- **`client.base_url`** — a raiz da API (`https://pokeapi.co/api/v2/`). Os `path` dos recursos
  são relativos a ela.
- **`resources`** — a lista de "tabelas" a extrair. Aqui só uma: `pokemon`.
- **`endpoint.path`** — o caminho do recurso (`pokemon` → `.../api/v2/pokemon`).
- **`endpoint.data_selector`** — onde, na resposta JSON, está a **lista** de itens. A PokéAPI
  responde `{"count": ..., "next": ..., "results": [...]}`, então os dados estão em `results`.
- **`endpoint.paginator`** — como **virar as páginas**. Este é um paginador do tipo **`offset`**:

| Chave do paginator | Significado |
|---|---|
| `type: "offset"` | paginação por deslocamento (offset/limit) |
| `limit: 100` | quantos itens por página (a API recebe via `limit_param`) |
| `offset: 0` | de onde começar |
| `limit_param: "limit"` | nome do parâmetro de tamanho na URL (`?limit=100`) |
| `offset_param: "offset"` | nome do parâmetro de deslocamento na URL (`?offset=100`) |
| `total_path: "count"` | onde, no JSON, está o total de registros (campo `count`) |
| `maximum_offset: 200` | **para cedo**: não passa do offset 200 (≈ 200 pokémons) |

> **Por que `maximum_offset: 200`?** A PokéAPI tem mais de 1300 pokémons. Para o tutorial ser
> rápido (e gentil com a API pública), limitamos a ~200 registros. Aumente esse número se
> quiser ingerir mais.

Execute:

```bash
python pipeline_api.py
```

**Resultado esperado** (resumo análogo ao da pipeline 1):

```
Pipeline ingestao_pokeapi load step completed in ...
1 load package(s) were loaded to destination filesystem and into dataset pokeapi
The filesystem destination used s3://datalake location to store data
Load package ... is LOADED and contains no failed jobs
```

Os dados foram parar em `s3://datalake/pokeapi/pokemon/*.parquet`.

---

## 7. Validando os Parquet no S3 local

Use o **AWS CLI** apontando para o MiniStack para listar o que as pipelines gravaram. Exporte
as credenciais dummy e liste o bucket recursivamente:

```bash
# macOS / Linux
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/ --recursive
```

```powershell
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="test"; $env:AWS_SECRET_ACCESS_KEY="test"; $env:AWS_DEFAULT_REGION="us-east-1"
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/ --recursive
```

**Resultado esperado** (caminhos com hash variam; o que importa são as pastas e a extensão
`.parquet`):

```
2026-06-25 18:10:21      12345 postgres/vendas/1718...0.parquet
2026-06-25 18:12:03       6789 pokeapi/pokemon/1718...0.parquet
```

Ou seja: linhas com **`postgres/vendas/....parquet`** e **`pokeapi/pokemon/....parquet`**.

> Além dos Parquet, o dlt grava arquivos de **metadado/estado** dele (pasta de schema/load
> packages). É normal aparecerem alguns objetos extras além dos `.parquet` de dados.

---

## 8. Entendendo onde os arquivos caem

O destino `filesystem` do dlt monta o caminho assim:

```
s3://<bucket>/<dataset_name>/<table_name>/<arquivos>.parquet
        │            │             │
     datalake     postgres       vendas        → s3://datalake/postgres/vendas/*.parquet
     datalake     pokeapi        pokemon       → s3://datalake/pokeapi/pokemon/*.parquet
```

- O **`<bucket>`** vem do `bucket_url` (`s3://datalake`).
- O **`<dataset_name>`** é o que você passou em `dlt.pipeline(dataset_name=...)`.
- O **`<table_name>`** vem do recurso/tabela: `vendas` (nome da tabela do Postgres) e
  `pokemon` (nome do resource da PokéAPI).

Trocar o `dataset_name` ou o nome da tabela/resource muda a pasta de destino — útil para
organizar o lake por origem.

---

## 9. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ModuleNotFoundError: dlt.sources.sql_database` | Faltou o extra de banco na instalação | `pip install "dlt[sql_database]"` (e confira que o venv está ativo) |
| `connection refused` / `could not connect to server` (Postgres) | Container do Postgres não está no ar, ou host/porta errados | Volte ao **Tutorial 1 (Local)** e suba os containers (`docker compose ps`); confira `localhost:5432` |
| Nada aparece no `s3 ls` do MiniStack | Container do MiniStack parado ou bucket inexistente | Suba o MiniStack (Tutorial 1) e confira o bucket `datalake` |
| Erro de Parquet / `pyarrow` ausente | Faltou o extra `parquet` | `pip install "dlt[parquet]"` |
| Credenciais/endpoint do destino "não encontrados" | Variáveis não exportadas nesta sessão (ou `.dlt/secrets.toml` em outra pasta) | Reexporte as variáveis da seção 4 **ou** rode o script na pasta que contém `.dlt/secrets.toml` |
| PokéAPI lenta / poucos registros | `maximum_offset: 200` limita a ~200 pokémons (de propósito) | Aumente `maximum_offset` no `pipeline_api.py` se quiser mais |

---

**Pronto!** Você ingeriu um **banco** e uma **API REST** para o data lake usando dlt + Python,
em Parquet. Quando quiser fazer o mesmo na nuvem (RDS + S3 real, rodando numa EC2), siga o
**`3-dlthub/aws/TUTORIAL.md`**.

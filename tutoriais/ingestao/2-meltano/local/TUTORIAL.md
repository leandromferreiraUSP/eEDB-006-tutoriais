# Tutorial 2 (Local): Ingestão de Dados com Meltano

> Versão **longa e explicativa**. Você vai usar o **Meltano** (um orquestrador de pipelines
> Singer) para **extrair** dados de duas origens — a tabela `vendas` do **PostgreSQL** e a
> **PokéAPI** (uma API REST paginada) — e **carregá-los em Parquet** no seu data lake S3 local
> (MiniStack).
>
> **Pré-requisito**: ter feito o `1-infraestrutura/local` e estar com os containers
> (`ingestao_postgres` + `ingestao_ministack`) **rodando**. Só os comandos? Veja `QUICK_TUTORIAL.md`.

---

## Sumário

1. [O que é Meltano (e Singer)](#1-o-que-é-meltano-e-singer)
2. [O fluxo deste tutorial](#2-o-fluxo-deste-tutorial)
3. [Instalando o Meltano](#3-instalando-o-meltano)
4. [Criando o projeto e adicionando plugins](#4-criando-o-projeto-e-adicionando-plugins)
5. [Configurando o `meltano.yml`](#5-configurando-o-meltanoyml)
6. [Ingestão 1: Postgres → Parquet](#6-ingestão-1-postgres--parquet)
7. [Enviando os Parquet para o S3 (MiniStack)](#7-enviando-os-parquet-para-o-s3-ministack)
8. [Ingestão 2: PokéAPI → Parquet → S3](#8-ingestão-2-pokéapi--parquet--s3)
9. [Validando no data lake](#9-validando-no-data-lake)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. O que é Meltano (e Singer)

**Singer** é um padrão aberto onde um **tap** (extrator) lê dados de uma origem e emite
mensagens JSON num formato padronizado; um **target** (carregador) recebe essas mensagens e
grava no destino. Qualquer tap conversa com qualquer target.

**Meltano** é o orquestrador que gerencia esses plugins: instala taps/targets em ambientes
isolados, guarda a configuração declarativamente num arquivo `meltano.yml`, controla seleção
de tabelas, estado incremental e a execução (`meltano run tap target`).

| Conceito Singer | Neste tutorial |
|---|---|
| **tap** (extrator) | `tap-postgres` (lê o banco) e `tap-rest-api-msdk` (lê a PokéAPI) |
| **target** (carregador) | `target-parquet` (grava arquivos Parquet) |
| **meltano.yml** | onde declaramos plugins, conexões e a tabela a extrair |

> **Por que `target-parquet` + `aws s3 sync`, e não um target "direto pro S3"?** Os targets
> Singer que escrevem Parquet **direto no S3** com endpoint customizável ou não suportam
> Python 3.12, ou exigem dependências problemáticas. O caminho mais robusto e didático é:
> o Meltano grava **Parquet local** → publicamos no lake com `aws s3 sync`. É o mesmo padrão
> "landing → publish" de muitos pipelines reais, e funciona igual local e na AWS.

---

## 2. O fluxo deste tutorial

```
  tap-postgres ───►                          ┌── output/public-vendas/*.parquet ──┐
                    target-parquet (local) ──┤                                     ├─ aws s3 sync ─► s3://datalake/meltano/
  tap-rest-api ───►                          └── output/pokemon/*.parquet ────────┘
   (PokéAPI)
```

---

## 3. Instalando o Meltano

O Meltano é uma ferramenta Python. Crie um ambiente virtual (Python 3.12) só para ele:

```bash
cd tutoriais/ingestao/2-meltano/local
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install "meltano==4.2.1"
meltano --version                          # meltano, version 4.2.1
```

> Pinamos `meltano==4.2.1`. A CLI da série 4.x mudou em relação a tutoriais antigos: o tipo do
> plugin agora vai em `--plugin-type` (veja a seguir).

---

## 4. Criando o projeto e adicionando plugins

Inicialize o projeto Meltano e adicione os dois extratores e o carregador. **Atenção à
sintaxe da v4** (`--plugin-type`):

```bash
# desative a telemetria (opcional) e crie o projeto
export MELTANO_SEND_ANONYMOUS_USAGE_STATS=False
meltano init projeto_ingestao
cd projeto_ingestao

# extrator do Postgres
meltano add --plugin-type extractor tap-postgres

# extrator de API REST (usado para a PokéAPI)
meltano add --plugin-type extractor tap-rest-api-msdk

# carregador Parquet (variante mantida pela Automattic, compatível com Python 3.12)
meltano add --plugin-type loader target-parquet --variant automattic
```

**Resultado esperado**: cada comando termina com `Installed extractor/loader '...'`.

> Se você vir um erro de instalação mencionando `pkg_resources`/`setuptools` no
> `tap-rest-api-msdk`, é esperado — resolvemos isso no `meltano.yml` a seguir, fixando
> `setuptools<81` no `pip_url` do plugin.

---

## 5. Configurando o `meltano.yml`

O `meltano init` + `meltano add` criaram um `meltano.yml`. Abra-o e **edite a seção
`plugins`** para ficar exatamente assim (as partes em `config:` e `select:` são o que você
adiciona):

```yaml
plugins:
  extractors:
  - name: tap-postgres
    variant: meltanolabs
    pip_url: meltanolabs-tap-postgres
    config:
      host: localhost
      port: 5432
      user: ecommerce
      database: ecommerce
    select:
    - public-vendas.*          # extrair SOMENTE a tabela vendas (schema public)
  - name: tap-rest-api-msdk
    variant: widen
    # setuptools<81 restaura o pkg_resources (removido no setuptools 82) que este tap importa
    pip_url: tap-rest-api-msdk setuptools<81
    config:
      api_url: https://pokeapi.co/api/v2
      pagination_request_style: simple_offset_paginator
      pagination_response_style: offset
      pagination_page_size: 100
      offset_records_jsonpath: $.results        # NOTA: $.results (lista), não $.results[*]
      streams:
      - name: pokemon
        path: /pokemon
        records_path: $.results[*]
        primary_keys:
        - name
  loaders:
  - name: target-parquet
    variant: automattic
    pip_url: git+https://github.com/Automattic/target-parquet.git
    config:
      destination_path: output
```

A senha do banco **não** vai no `meltano.yml` (boa prática). Crie um arquivo **`.env`** na raiz
do projeto Meltano:

```bash
echo "TAP_POSTGRES_PASSWORD=ecommerce" > .env
```

> Como mudamos o `pip_url` do `tap-rest-api-msdk`, reinstale-o para aplicar o `setuptools<81`:
> ```bash
> meltano install extractor tap-rest-api-msdk
> ```

### Detalhes importantes da configuração

- **`select: [public-vendas.*]`** — diz ao `tap-postgres` para extrair **apenas** a tabela
  `vendas` (todas as colunas). Confira com `meltano select tap-postgres --list`.
- **Paginação da PokéAPI** — a API devolve `{count, next, results:[...]}`. Usamos o
  `simple_offset_paginator` (envia `?offset=&limit=`) e ele continua enquanto a página vier
  cheia. O `offset_records_jsonpath` precisa apontar para a **lista** (`$.results`) — apontar
  para `$.results[*]` faz o tap contar errado e parar na 1ª página.

---

## 6. Ingestão 1: Postgres → Parquet

Com o ambiente local de pé e o venv ativo, rode o pipeline `tap-postgres → target-parquet`:

```bash
meltano run tap-postgres target-parquet
```

**Resultado esperado** (trechos):

```
tap-postgres   Beginning sync of 'public-vendas' in full_table mode
tap-postgres   METRIC ... record_count 200
target-parquet Target 'target-parquet' completed reading 202 lines of input (... 200 records ...)
meltano        Block run completed
```

Confira o arquivo Parquet gerado localmente:

```bash
find output -name "*.parquet"
# output/public-vendas/public-vendas-20260625_204032-0-0.gz.parquet
```

---

## 7. Enviando os Parquet para o S3 (MiniStack)

Os Parquet estão locais; vamos publicá-los no data lake (MiniStack). Configure o AWS CLI para
o S3 local e **lembre do gotcha do checksum** (o MiniStack não aceita CRC64NVME):

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required   # essencial p/ MiniStack
```

Publique a pasta da tabela `vendas` (só os `.parquet`):

```bash
aws --endpoint-url http://localhost:4566 s3 sync output/public-vendas \
  s3://datalake/meltano/vendas/ --exclude "*" --include "*.parquet"
```

**Resultado esperado**:

```
upload: output/public-vendas/public-vendas-...gz.parquet to s3://datalake/meltano/vendas/...gz.parquet
```

---

## 8. Ingestão 2: PokéAPI → Parquet → S3

Agora a origem do tipo **API REST**. Rode o outro extrator com o mesmo target:

```bash
meltano run tap-rest-api-msdk target-parquet
```

**Resultado esperado**: `record_count 1350` (todos os pokémons, paginados de 100 em 100) e um
Parquet em `output/pokemon/`.

Publique no lake:

```bash
aws --endpoint-url http://localhost:4566 s3 sync output/pokemon \
  s3://datalake/meltano/pokemon/ --exclude "*" --include "*.parquet"
```

---

## 9. Validando no data lake

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/meltano/ --recursive
```

**Resultado esperado** (dois objetos):

```
... meltano/pokemon/pokemon-...gz.parquet
... meltano/vendas/public-vendas-...gz.parquet
```

🎉 Você ingeriu um **banco relacional** e uma **API REST** para o data lake, em Parquet,
usando Meltano. Os mesmos dados podem ser lidos pelos tutoriais de Spark/Athena deste repositório.

> Para fazer o mesmo **na nuvem** (RDS → S3 real, rodando na EC2), siga `2-meltano/aws/`.

---

## 10. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Utility 'extractor' is not known` | Sintaxe antiga | Use `meltano add --plugin-type extractor <nome>` (v4) |
| `No such command 'tap-postgres'` no `meltano config` | Sintaxe da v4 | Configure pelo `meltano.yml` (como neste tutorial) |
| Erro de instalação com `pkg_resources` | `setuptools` 82 removeu `pkg_resources` | Fixe `setuptools<81` no `pip_url` do tap e `meltano install` |
| `requires a different Python: ... <3.12` (target-s3) | Plugin não suporta 3.12 | Use `target-parquet` (automattic) + `aws s3 sync`, como aqui |
| PokéAPI traz só 100 registros | `offset_records_jsonpath: $.results[*]` | Use `$.results` (a lista), não `$.results[*]` |
| `... CRC64NVME` ao subir pro S3 local | Checksum padrão do AWS CLI v2 | `export AWS_REQUEST_CHECKSUM_CALCULATION=when_required` |
| `connection refused` no Postgres | Containers parados | Volte ao `1-infraestrutura/local` e `docker compose up -d` |
| `password authentication failed` | `.env` ausente/errado | Crie `.env` com `TAP_POSTGRES_PASSWORD=ecommerce` |

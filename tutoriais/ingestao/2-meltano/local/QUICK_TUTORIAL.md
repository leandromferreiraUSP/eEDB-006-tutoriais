# Quick Tutorial 2 (Local): Ingestão com Meltano

> Só os comandos. Explicações: `TUTORIAL.md`.
> Pré-requisito: `1-infraestrutura/local` no ar (Postgres + MiniStack). Destino: `s3://datalake/meltano/`.

---

## 1. Instalar e criar o projeto

```bash
cd tutoriais/ingestao/2-meltano/local
python3 -m venv .venv && source .venv/bin/activate      # Win: .venv\Scripts\Activate.ps1
pip install "meltano==4.2.1"

export MELTANO_SEND_ANONYMOUS_USAGE_STATS=False
meltano init projeto_ingestao && cd projeto_ingestao
meltano add --plugin-type extractor tap-postgres
meltano add --plugin-type extractor tap-rest-api-msdk
meltano add --plugin-type loader target-parquet --variant automattic
```

## 2. Configurar `meltano.yml` (edite a seção `plugins`)

```yaml
  extractors:
  - name: tap-postgres
    variant: meltanolabs
    pip_url: meltanolabs-tap-postgres
    config: { host: localhost, port: 5432, user: ecommerce, database: ecommerce }
    select: [ public-vendas.* ]
  - name: tap-rest-api-msdk
    variant: widen
    pip_url: tap-rest-api-msdk setuptools<81
    config:
      api_url: https://pokeapi.co/api/v2
      pagination_request_style: simple_offset_paginator
      pagination_response_style: offset
      pagination_page_size: 100
      offset_records_jsonpath: $.results
      streams:
      - { name: pokemon, path: /pokemon, records_path: $.results[*], primary_keys: [name] }
  loaders:
  - name: target-parquet
    variant: automattic
    pip_url: git+https://github.com/Automattic/target-parquet.git
    config: { destination_path: output }
```

```bash
echo "TAP_POSTGRES_PASSWORD=ecommerce" > .env
meltano install extractor tap-rest-api-msdk      # aplica o setuptools<81
```

## 3. Rodar as duas ingestões

```bash
meltano run tap-postgres target-parquet          # vendas -> output/public-vendas/
meltano run tap-rest-api-msdk target-parquet     # pokemon -> output/pokemon/
```

## 4. Publicar no S3 local (MiniStack)

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required        # MiniStack não aceita CRC64NVME

aws --endpoint-url http://localhost:4566 s3 sync output/public-vendas s3://datalake/meltano/vendas/   --exclude "*" --include "*.parquet"
aws --endpoint-url http://localhost:4566 s3 sync output/pokemon       s3://datalake/meltano/pokemon/  --exclude "*" --include "*.parquet"

aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/meltano/ --recursive   # 2 parquet
```

# Tutorial 2 (AWS): Ingestão com Meltano na Nuvem

> Versão **longa e explicativa**. Mesmo pipeline do tutorial local, agora **na AWS**: o Meltano
> roda na **EC2**, extrai do **RDS PostgreSQL** e da **PokéAPI**, e publica os Parquet no
> **bucket S3 real**.
>
> **Pré-requisito**: ter feito o `1-infraestrutura/aws` (RDS + S3 + EC2 provisionados e o RDS
> populado pelo seed). Só os comandos? Veja `QUICK_TUTORIAL.md`.

---

## Sumário

1. [O que muda em relação ao local](#1-o-que-muda-em-relação-ao-local)
2. [Pegando os endereços da infraestrutura](#2-pegando-os-endereços-da-infraestrutura)
3. [Conectando na EC2 e preparando o ambiente](#3-conectando-na-ec2-e-preparando-o-ambiente)
4. [Criando o projeto Meltano na EC2](#4-criando-o-projeto-meltano-na-ec2)
5. [Rodando as ingestões](#5-rodando-as-ingestões)
6. [Publicando no S3 real](#6-publicando-no-s3-real)
7. [Validando](#7-validando)
8. [Limpando](#8-limpando)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. O que muda em relação ao local

A lógica é **idêntica** ao `2-meltano/local`. Apenas três coisas mudam:

| | Local | AWS |
|---|---|---|
| Onde o Meltano roda | sua máquina | **EC2** (via SSH) |
| Origem (`tap-postgres host`) | `localhost` | **endpoint do RDS** |
| Destino (`aws s3 sync`) | MiniStack (`--endpoint-url`, checksum) | **S3 real** (sem endpoint, sem checksum) |

> Na EC2 as credenciais da AWS vêm do **LabInstanceProfile** (anexado pelo Tutorial 1 AWS), então
> o `aws s3 sync` para o bucket real funciona **sem** configurar chaves.

---

## 2. Pegando os endereços da infraestrutura

Na sua máquina, na pasta do Terraform, leia os outputs do Tutorial 1 AWS:

```bash
cd tutoriais/ingestao/1-infraestrutura/aws/terraform
terraform output
```

Anote: `ec2_public_ip`, `rds_endpoint` e `s3_bucket` (algo como `849967252385-ingestao-lab`).

---

## 3. Conectando na EC2 e preparando o ambiente

```bash
ssh -i ~/.ssh/labsuser.pem ec2-user@<EC2_PUBLIC_IP>
```

Já dentro da EC2 (Amazon Linux 2023), prepare o Python (a `user_data` já instalou `python3.11`
e o cliente `psql`):

```bash
python3.11 -m venv ~/meltano-venv
source ~/meltano-venv/bin/activate
pip install --upgrade pip
pip install "meltano==4.2.1"
meltano --version
```

> Confirme que o RDS está populado (seed do Tutorial 1 AWS):
> ```bash
> export PGPASSWORD=ecommerce123
> psql -h <RDS_ENDPOINT> -U ecommerce -d ecommerce -c "SELECT count(*) FROM vendas;"   # 200
> ```

---

## 4. Criando o projeto Meltano na EC2

São os **mesmos comandos** do tutorial local:

```bash
export MELTANO_SEND_ANONYMOUS_USAGE_STATS=False
meltano init projeto_ingestao && cd projeto_ingestao
meltano add --plugin-type extractor tap-postgres
meltano add --plugin-type extractor tap-rest-api-msdk
meltano add --plugin-type loader target-parquet --variant automattic
```

Edite o `meltano.yml` igual ao local, **mudando apenas o `host`** do `tap-postgres` para o
endpoint do RDS:

```yaml
  extractors:
  - name: tap-postgres
    variant: meltanolabs
    pip_url: meltanolabs-tap-postgres
    config:
      host: <RDS_ENDPOINT>        # <- endpoint do RDS (terraform output rds_endpoint)
      port: 5432
      user: ecommerce
      database: ecommerce
    select:
    - public-vendas.*
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

Senha do RDS no `.env` (use a sua `db_password`, padrão `ecommerce123`):

```bash
echo "TAP_POSTGRES_PASSWORD=ecommerce123" > .env
meltano install extractor tap-rest-api-msdk      # aplica o setuptools<81
```

---

## 5. Rodando as ingestões

```bash
meltano run tap-postgres target-parquet          # vendas (do RDS) -> output/public-vendas/
meltano run tap-rest-api-msdk target-parquet     # pokemon (PokéAPI) -> output/pokemon/
```

**Resultado esperado**: `record_count 200` (vendas) e `record_count 1350` (pokemon), com
Parquet em `output/`.

---

## 6. Publicando no S3 real

Na EC2, com o `LabInstanceProfile` ativo, publique direto no bucket real — **sem**
`--endpoint-url` e **sem** o ajuste de checksum (aquilo era só do MiniStack):

```bash
BUCKET=849967252385-ingestao-lab      # use o seu s3_bucket (terraform output)

aws s3 sync output/public-vendas s3://$BUCKET/meltano/vendas/   --exclude "*" --include "*.parquet"
aws s3 sync output/pokemon       s3://$BUCKET/meltano/pokemon/  --exclude "*" --include "*.parquet"
```

---

## 7. Validando

Da **sua máquina** (ou da EC2):

```bash
aws s3 ls s3://849967252385-ingestao-lab/meltano/ --recursive
```

**Resultado esperado**: dois objetos `.parquet` em `meltano/vendas/` e `meltano/pokemon/`.

---

## 8. Limpando

Os Parquet ficam no S3 (apague-os se quiser). O mais importante: **destrua a infraestrutura**
para não gerar custos — volte ao `1-infraestrutura/aws` e rode `terraform destroy`.

---

## 9. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `psql ... timeout` para o RDS (da sua máquina) | RDS não é público | Rode tudo **de dentro da EC2** |
| `AccessDenied` no `aws s3 sync` | EC2 sem o LabInstanceProfile | Confirme `iam_instance_profile` no Tutorial 1 AWS |
| `password authentication failed` | `.env` com senha errada | Use a `db_password` do Terraform (padrão `ecommerce123`) |
| `command not found: meltano` | venv não ativo | `source ~/meltano-venv/bin/activate` |
| Erros de plugin (Postgres/PokéAPI) | mesmos do local | Veja o Troubleshooting de `2-meltano/local` |
| Credenciais expiraram durante o SSH | sessão do Lab venceu | Reabra o Lab, recopie credenciais, refaça SSH |

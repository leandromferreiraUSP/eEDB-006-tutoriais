# Tutorial 1 (Local): Infraestrutura de Ingestão com Docker

> Versão **longa e explicativa**. Aqui você monta, **na sua própria máquina**, todo o
> ambiente que os Tutoriais 2 (Meltano) e 3 (DLTHub) vão usar: um banco de dados **PostgreSQL**
> de origem (já populado com dados de e-commerce) e um **data lake S3** local emulado pelo
> **MiniStack**. Tudo roda em containers Docker — nada é instalado "sujo" na sua máquina.
>
> Quer só os comandos? Veja o `QUICK_TUTORIAL.md`.

---

## Sumário

1. [O que vamos construir](#1-o-que-vamos-construir)
2. [Conceitos: por que Postgres + MiniStack?](#2-conceitos-por-que-postgres--ministack)
3. [Pré-requisitos por sistema operacional](#3-pré-requisitos-por-sistema-operacional)
4. [Os dados mock (origem)](#4-os-dados-mock-origem)
5. [O arquivo `docker-compose.yml`](#5-o-arquivo-docker-composeyml)
6. [O script de criação do bucket (MiniStack)](#6-o-script-de-criação-do-bucket-ministack)
7. [Subindo o ambiente](#7-subindo-o-ambiente)
8. [Validando o ambiente](#8-validando-o-ambiente)
9. [Explorando o Postgres e o S3 local](#9-explorando-o-postgres-e-o-s3-local)
10. [Parando e limpando](#10-parando-e-limpando)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. O que vamos construir

Ingestão de dados é o processo de **trazer dados de uma origem** (um banco, uma API, um
arquivo) **para um destino** onde eles serão armazenados e processados — normalmente um
**data lake** (no nosso caso, o S3). Para praticar isso sem depender da nuvem (e sem gastar
nada), vamos emular o ambiente da AWS **localmente**:

```
            ┌─────────────────────── sua máquina (Docker) ───────────────────────┐
            │                                                                     │
  ORIGEM    │   ┌──────────────────┐        ferramenta de         ┌────────────┐  │
  (banco)   │   │  PostgreSQL 16   │  ───►   ingestão (Tut. 2/3)   │  MiniStack │  │  DESTINO
            │   │  ecommerce       │         (Meltano / DLTHub)    │  S3 :4566  │  │  (data lake)
            │   │  clientes/       │  ───►                    ───► │ datalake/  │  │
            │   │  produtos/vendas │                               └────────────┘  │
            │   └──────────────────┘                                               │
            │          :5432                                                        │
            └─────────────────────────────────────────────────────────────────────┘
                                              ▲
                                              │  API pública (PokéAPI) entra pela internet
                                              │  e também é gravada no S3 local
```

- **PostgreSQL**: o banco transacional de origem, com três tabelas de e-commerce.
- **MiniStack**: um emulador **open-source** dos serviços da AWS (S3, e muitos outros) que
  roda em um único container e responde na porta **4566** — exatamente como o S3 real, mas
  na sua máquina. É a peça que nos dá **paridade local ↔ nuvem**: o código dos Tutoriais 2 e
  3 é o mesmo, mudando apenas o `endpoint_url` e as credenciais.

> **Por que MiniStack e não o S3 de verdade?** Para você praticar à vontade, offline, de
> graça e sem risco de "esquecer um bucket ligado". Quando for para a AWS (Tutoriais
> `2-meltano/aws` e `3-dlthub/aws`), trocamos o endpoint local pelo S3 real. Mais nada muda.

---

## 2. Conceitos: por que Postgres + MiniStack?

| Peça | Papel na ingestão | Equivalente na AWS |
|---|---|---|
| PostgreSQL | Sistema de origem (OLTP) de onde **extraímos** os dados | Amazon RDS (Tutorial 1 AWS) |
| MiniStack (S3) | Data lake de **destino** onde **carregamos** os dados | Amazon S3 |
| PokéAPI | Origem do tipo **API REST** (ingestão via HTTP) | (a mesma API pública) |
| Parquet | Formato **colunar** em que gravamos no lake | (o mesmo) |

> **MiniStack** é um substituto livre do LocalStack: imagem Docker única, sem cadastro, sem
> chave de API, fala o protocolo da AWS na porta 4566. Site: <https://ministack.org>.

---

## 3. Pré-requisitos por sistema operacional

Você precisa de **três** ferramentas: **Docker**, **Python 3.12** (para os Tutoriais 2 e 3)
e o **AWS CLI v2** (para conversar com o S3, local e na nuvem). Siga a coluna do seu SO.

### 3.1 — macOS (caso corrente)

```bash
# Docker Desktop (ou OrbStack) — se ainda não tiver:
brew install --cask docker        # depois ABRA o Docker Desktop uma vez

# Python 3.12 e AWS CLI:
brew install python@3.12 awscli

# Confirme:
docker --version
python3 --version                  # Python 3.12.x
aws --version                      # aws-cli/2.x
```

### 3.2 — Linux (Ubuntu)

```bash
# Docker Engine + plugin compose (repositório oficial):
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER       # depois FAÇA logout/login para valer

# Python 3.12 e AWS CLI v2:
sudo apt-get install -y python3.12 python3.12-venv unzip curl
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip

docker --version && python3.12 --version && aws --version
```

### 3.3 — Windows (PowerShell puro, sem WSL no terminal)

No Windows você usa o **Docker Desktop** (que internamente roda sobre o WSL2/Hyper-V — isso é
o "motor" do Docker, você **não precisa abrir um terminal WSL**: todos os comandos abaixo são
**PowerShell nativo**).

```powershell
# Instale via winget (Windows 10/11):
winget install -e --id Docker.DockerDesktop
winget install -e --id Python.Python.3.12
winget install -e --id Amazon.AWSCLI

# ABRA o Docker Desktop uma vez (ele liga o motor). Depois confirme em um NOVO PowerShell:
docker --version
python --version           # Python 3.12.x
aws --version              # aws-cli/2.x
```

> **Importante (Windows)**: abra o **Docker Desktop** e espere o ícone ficar verde ("Engine
> running") antes de rodar `docker compose`. Se `docker` não for reconhecido, feche e reabra
> o PowerShell para recarregar o `PATH`.

---

## 4. Os dados mock (origem)

Os dados já estão prontos na pasta **`tutoriais/ingestao/dados/`** (compartilhada por todos os
tutoriais). São três tabelas de um e-commerce fictício:

| Tabela | Linhas | Colunas principais |
|---|---|---|
| `clientes` | 20 | `cliente_id`, `nome`, `email`, `cidade`, `estado`, `data_cadastro` |
| `produtos` | 15 | `produto_id`, `nome`, `categoria`, `preco` |
| `vendas` | 200 | `venda_id`, `cliente_id`, `produto_id`, `quantidade`, `valor_total`, `data_venda` |

A tabela **`vendas`** é a "fato" que vamos ingerir nos Tutoriais 2 e 3.

Dois arquivos SQL fazem a carga **automaticamente** quando o container do Postgres sobe:

- **`schema.sql`** — cria as três tabelas (DDL).
- **`seed.sql`** — insere os dados (235 `INSERT`s, sem depender de arquivo externo).

> **Por que `INSERT` e não `COPY` de CSV?** Para o `seed.sql` rodar **igual** no Postgres
> local (init automático) e no **RDS** (Tutorial 1 AWS), sem depender de caminhos de arquivo.
> Os CSVs (`clientes.csv`, `produtos.csv`, `vendas.csv`) também estão na pasta, como
> referência dos dados de origem.

O Postgres executa qualquer `*.sql` em `/docker-entrypoint-initdb.d/` em ordem alfabética —
por isso `schema.sql` roda antes de `seed.sql` (`c` < `e`).

---

## 5. O arquivo `docker-compose.yml`

Esse é um arquivo de **infraestrutura como código**: ele declara os dois containers. Ele já
existe em `1-infraestrutura/local/docker/docker-compose.yml`, mas leia cada parte — você vai
querer entender o que está subindo.

```yaml
services:
  postgres:
    image: postgres:16
    container_name: ingestao_postgres
    environment:
      POSTGRES_USER: ecommerce
      POSTGRES_PASSWORD: ecommerce
      POSTGRES_DB: ecommerce
    ports:
      - "5432:5432"
    volumes:
      # ../../../dados é a pasta tutoriais/ingestao/dados (schema.sql + seed.sql rodam no init)
      - ../../../dados:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ecommerce -d ecommerce"]
      interval: 5s
      timeout: 5s
      retries: 10

  ministack:
    image: ministackorg/ministack:latest
    container_name: ingestao_ministack
    environment:
      GATEWAY_PORT: "4566"
      MINISTACK_REGION: "us-east-1"
      SERVICES: "s3"
    ports:
      - "4566:4566"
    volumes:
      - ./ministack-init:/docker-entrypoint-initaws.d:ro
    healthcheck:
      # a imagem do MiniStack não traz curl; usamos o python que já roda o serviço
      test: ["CMD-SHELL", "python -c \"import urllib.request as u; u.urlopen('http://localhost:4566/_ministack/health')\" || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20
```

Pontos de atenção (aprendizados reais):

- **`../../../dados`** sobe a partir de `docker/` até `ingestao/` e entra em `dados/`. O
  caminho é relativo ao **arquivo** compose.
- **`SERVICES: s3`** liga só o S3 no MiniStack (leve e suficiente).
- O **healthcheck do MiniStack usa `python`**, não `curl` — a imagem não inclui `curl`. Sem
  isso, o container ficaria eternamente "health: starting".

---

## 6. O script de criação do bucket (MiniStack)

Quando o S3 do MiniStack fica pronto, queremos que o bucket **`datalake`** já exista. O
MiniStack roda scripts de init, **mas atenção a dois detalhes que descobrimos na prática**:

1. Scripts em `/docker-entrypoint-initaws.d/` rodam **antes** do gateway S3 estar ouvindo.
   Para rodar **depois** que o S3 está no ar, o script precisa ficar na subpasta **`ready.d/`**.
2. O MiniStack **não injeta credenciais** automaticamente nos scripts — passamos credenciais
   dummy (`test`/`test`) e o `--endpoint-url` explicitamente.

Arquivo `1-infraestrutura/local/docker/ministack-init/ready.d/01-bucket.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"
EP="http://localhost:4566"

echo "[ready] criando bucket s3://datalake ..."
aws --endpoint-url "$EP" s3 mb s3://datalake 2>/dev/null || echo "[ready] bucket já existe (ok)"

echo "[ready] buckets disponíveis:"
aws --endpoint-url "$EP" s3 ls
echo "[ready] pronto."
```

---

## 7. Subindo o ambiente

Entre na pasta do compose e suba os containers (na primeira vez, o Docker baixa as imagens —
pode levar alguns minutos):

```bash
cd tutoriais/ingestao/1-infraestrutura/local/docker
docker compose up -d
```

No **Windows (PowerShell)** os comandos são idênticos (`cd` e `docker compose up -d`).

Acompanhe até os dois ficarem **healthy**:

```bash
docker compose ps
```

**Resultado esperado** (os dois `Up` e `healthy`):

```
NAME                 IMAGE                           STATUS                   PORTS
ingestao_ministack   ministackorg/ministack:latest   Up (healthy)             0.0.0.0:4566->4566/tcp
ingestao_postgres    postgres:16                     Up (healthy)             0.0.0.0:5432->5432/tcp
```

> O MiniStack leva ~20–30s para ficar `healthy` na primeira subida (ele inicializa os
> serviços e roda o `ready.d/01-bucket.sh`).

---

## 8. Validando o ambiente

### 8.1 — Configurando o AWS CLI para falar com o MiniStack

O MiniStack aceita **qualquer** credencial (use `test`/`test`). Exporte as variáveis (na sessão
atual do terminal):

```bash
# macOS / Linux
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

```powershell
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="test"
$env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"
```

Liste os buckets do S3 local:

```bash
aws --endpoint-url http://localhost:4566 s3 ls
```

**Resultado esperado**:

```
2026-06-25 17:29:59 datalake
```

> ⚠️ **Gotcha do AWS CLI v2 + MiniStack (importante para os Tutoriais 2 e 3)**: o AWS CLI v2
> recente usa, por padrão, o checksum **CRC64NVME** ao **enviar** objetos, e o MiniStack não
> suporta esse algoritmo (só SHA256/SHA1/CRC32). Ao **subir** arquivos para o S3 local, defina:
> ```bash
> export AWS_REQUEST_CHECKSUM_CALCULATION=when_required   # PowerShell: $env:AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
> ```
> No S3 **real** (AWS) isso não é necessário.

### 8.2 — Script de validação

Há um script de validação pronto em `1-infraestrutura/local/validar.sh` que confere o Postgres
e o S3 de uma vez (macOS/Linux):

```bash
cd tutoriais/ingestao/1-infraestrutura/local
bash validar.sh
```

**Resultado esperado**:

```
==> 1/3 Postgres: contagem de linhas por tabela
clientes|20
produtos|15
vendas|200
    OK (esperado: clientes=20, produtos=15, vendas=200)
==> 2/3 MiniStack: health-check
    OK (S3 no ar em http://localhost:4566)
==> 3/3 MiniStack: bucket datalake existe?
    OK (s3://datalake presente)

✅ Ambiente local OK.
```

> No **Windows**, rode as verificações manualmente (próxima seção) — o `.sh` é para shells
> Unix.

---

## 9. Explorando o Postgres e o S3 local

### 9.1 — Postgres (contagem das tabelas)

```bash
docker exec ingestao_postgres psql -U ecommerce -d ecommerce -c \
  "SELECT count(*) FROM vendas;"
```

**Resultado esperado**:

```
 count
-------
   200
```

Espie algumas vendas:

```bash
docker exec ingestao_postgres psql -U ecommerce -d ecommerce -c \
  "SELECT venda_id, cliente_id, produto_id, valor_total, data_venda FROM vendas LIMIT 5;"
```

### 9.2 — S3 local (bucket vazio, por enquanto)

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/ --recursive
```

Ainda não há nada — os dados só chegam quando você rodar os **Tutoriais 2 (Meltano)** ou
**3 (DLTHub)**.

Para testar a escrita (e o gotcha do checksum), faça um upload de teste:

```bash
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
echo "ola data lake" > /tmp/teste.txt
aws --endpoint-url http://localhost:4566 s3 cp /tmp/teste.txt s3://datalake/teste.txt
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/
```

---

## 10. Parando e limpando

```bash
cd tutoriais/ingestao/1-infraestrutura/local/docker

# Parar mantendo os dados (sobe rápido depois):
docker compose stop

# Parar e REMOVER os containers e a rede (o Postgres é recriado e re-populado na próxima subida):
docker compose down

# Remover também volumes/órfãos:
docker compose down -v
```

> Como não declaramos volume persistente, **`down`** já zera o estado: na próxima `up` o
> `schema.sql`/`seed.sql` rodam de novo e o bucket é recriado. Bom para recomeçar limpo.

---

## 11. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop/Engine não está rodando | macOS/Win: abra o Docker Desktop e espere "running". Linux: `sudo systemctl start docker` |
| MiniStack fica `health: starting` para sempre | Healthcheck usando `curl` (ausente na imagem) | Use o healthcheck com `python` deste tutorial |
| Bucket `datalake` não aparece | Script de init na pasta errada ou sem credenciais | O script precisa estar em `ministack-init/ready.d/` e exportar `AWS_*=test` + `--endpoint-url` |
| `Could not connect to the endpoint URL` no init | Script rodou antes do S3 subir | Mover o script para a subpasta `ready.d/` |
| `An error occurred (InvalidRequest) ... CRC64NVME` ao subir arquivo | Checksum padrão do AWS CLI v2 | `export AWS_REQUEST_CHECKSUM_CALCULATION=when_required` |
| `port is already allocated` (5432/4566) | Outro serviço usando a porta | Pare o serviço conflitante ou ajuste o mapeamento de portas no compose |
| `psql: FATAL: database "ecommerce" does not exist` | Init não rodou (volume antigo) | `docker compose down -v` e suba de novo |

---

**Pronto!** Seu ambiente local está de pé. Agora siga para:

- **`2-meltano/local/TUTORIAL.md`** — ingestão com Meltano.
- **`3-dlthub/local/TUTORIAL.md`** — ingestão com DLTHub + Python.

Deixe os containers **rodando** enquanto faz esses tutoriais.

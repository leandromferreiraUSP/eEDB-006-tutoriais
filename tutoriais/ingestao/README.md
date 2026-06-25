# Tutoriais de Ingestão de Dados

Conjunto de tutoriais práticos sobre **ingestão de dados**: trazer dados de um **banco
relacional (PostgreSQL)** e de uma **API REST pública (PokéAPI)** para um **data lake S3**,
em formato **Parquet** — tanto **localmente** (com Docker + MiniStack) quanto na **AWS**
(RDS + EC2 + S3, via Terraform).

Cada tutorial tem **duas versões**:

- **`TUTORIAL.md`** — versão longa, didática e explicativa (o "porquê" de cada passo).
- **`QUICK_TUTORIAL.md`** — versão rápida, só o passo a passo (o "como").

---

## Ordem recomendada

```
1) Infraestrutura  ──►  2) Meltano   (ou)   3) DLTHub + Python
```

Comece **sempre** pelo Tutorial 1 (ele prepara o banco, os dados e o destino). Depois faça o
Tutorial 2 e/ou o 3 — eles são independentes entre si e mostram **duas ferramentas
diferentes** para o mesmo objetivo.

| # | Tutorial | Local | AWS |
|---|---|---|---|
| 1 | **Infraestrutura** | [`1-infraestrutura/local`](1-infraestrutura/local/TUTORIAL.md) — Docker (Postgres + MiniStack S3) | [`1-infraestrutura/aws`](1-infraestrutura/aws/TUTORIAL.md) — Terraform (RDS + EC2 + S3) |
| 2 | **Meltano** | [`2-meltano/local`](2-meltano/local/TUTORIAL.md) | [`2-meltano/aws`](2-meltano/aws/TUTORIAL.md) |
| 3 | **DLTHub + Python** | [`3-dlthub/local`](3-dlthub/local/TUTORIAL.md) | [`3-dlthub/aws`](3-dlthub/aws/TUTORIAL.md) |

---

## O que você vai construir

```
   ORIGEM                         FERRAMENTA DE INGESTÃO              DESTINO (data lake)
 ┌───────────────┐                ┌─────────────────────┐            ┌──────────────────┐
 │ PostgreSQL    │  ── tabela ──► │  Meltano  (Tut. 2)  │ ── Parquet►│  S3 / MiniStack  │
 │  vendas       │                │     ou              │            │  datalake/...    │
 │ PokéAPI (REST)│  ── HTTP ────► │  DLTHub   (Tut. 3)  │            │                  │
 └───────────────┘                └─────────────────────┘            └──────────────────┘
```

- **Local** usa **Docker** e o **MiniStack** (emulador S3 da AWS, porta 4566) — de graça, offline.
- **AWS** usa **RDS**, **EC2** e **S3** reais no **AWS Academy Learner Lab**, provisionados por
  **Terraform**. O código de ingestão é o mesmo; muda só o endpoint/host/credenciais.

---

## Os dados (pasta `dados/`)

Mocks de um e-commerce, compartilhados por todos os tutoriais e carregados pelo Tutorial 1:

| Tabela | Linhas | Conteúdo |
|---|---|---|
| `clientes` | 20 | cadastro de clientes |
| `produtos` | 15 | catálogo de produtos |
| `vendas` | 200 | **tabela ingerida** nos Tutoriais 2 e 3 |

Arquivos: `dados/schema.sql` (DDL), `dados/seed.sql` (carga via INSERT, roda igual no Postgres
local e no RDS) e os CSVs de referência (`clientes.csv`, `produtos.csv`, `vendas.csv`).

---

## Convenções

- **Idioma**: português (Brasil).
- **Sistemas operacionais cobertos** no Tutorial 1: **macOS**, **Linux (Ubuntu)** e **Windows
  (PowerShell)**.
- **Sem scripts de aplicação prontos**: os tutoriais **mostram** os comandos/códigos para você
  **criar no seu ambiente**. Apenas infraestrutura como código (`docker-compose.yml`,
  Terraform), scripts de validação e os dados/seed já vêm prontos no repositório.
- **Versões fixadas** (testadas): PostgreSQL 16, Meltano 4.2.1, dlt 1.28.1, Terraform `~> 5.0`
  (provider AWS), MiniStack `latest`.

---

## Credenciais AWS

As credenciais do Learner Lab ficam em [`../aws_credenciais/`](../aws_credenciais/) e são
**temporárias** (expiram a cada sessão do Lab). O Tutorial 1 AWS mostra como copiá-las para
`~/.aws/`. Reabra o Lab e recopie quando expirarem.

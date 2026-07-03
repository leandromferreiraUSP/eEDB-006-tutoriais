# Tutoriais de Processamento em Streaming

Conjunto de tutoriais práticos sobre **processamento de dados em streaming**: um fluxo
**contínuo** de eventos de e-commerce (vendas) é **produzido** por um gerador Python e
**consumido/processado** de três formas diferentes, sempre terminando em um **data lake S3**
(local com **MiniStack**, ou real na **AWS**).

O dado que trafega é **sempre o mesmo** (um evento de venda). O que muda entre os tutoriais é
o **transporte** (fila vs. tópico) e o **tipo de agrupamento** (unitário/micro-lote vs. janela
de tempo de 30s):

```
        MESMO EVENTO (venda)                    TRANSPORTE            AGRUPAMENTO         DESTINO
 ┌────────────────────────────┐
 │ evento_id, cliente_id,     │   ── fila ──►   SQS / RabbitMQ   ──►  micro-lote     ──►  S3 (Parquet)
 │ produto_id, categoria,     │
 │ quantidade, valor_total,   │   ── tópico ─►  Kafka + Spark    ──►  janela 30s     ──►  S3 (Parquet)
 │ data_venda (event-time)    │
 └────────────────────────────┘   ── tópico ─►  Kafka + Flink    ──►  janela 30s     ──►  S3 (Parquet)
```

Cada tutorial tem **duas versões**:

- **`TUTORIAL.md`** — versão longa, didática e explicativa. Abre sempre com **"Objetivo
  técnico e lógico"** e **"Decisões de projeto (e por quê)"**, explicando *por que* cada
  escolha foi feita (event-time + watermark, micro-lote em Parquet, agregação por categoria,
  MiniStack, Kafka só local, etc.).
- **`QUICK_TUTORIAL.md`** — versão rápida, só o passo a passo (o "como").

---

## Ordem recomendada

```
1) Infraestrutura  ──►  2) Filas   ──►   3) Kafka + Spark   ──►   4) Kafka + Flink
```

Comece **sempre** pelo **Tutorial 1** (ele sobe todo o ambiente: Kafka, RabbitMQ, Spark,
Flink e o S3 local, ou provisiona a infra AWS de filas). Os Tutoriais 2, 3 e 4 são
independentes entre si e mostram **três paradigmas** de streaming sobre o **mesmo evento**.

| # | Tutorial | Local | AWS |
|---|---|---|---|
| 1 | **Infraestrutura** | [`1-infraestrutura/local`](1-infraestrutura/local/TUTORIAL.md) — Docker (Kafka, RabbitMQ, Spark, Flink, MiniStack S3) | [`1-infraestrutura/aws`](1-infraestrutura/aws/TUTORIAL.md) — Terraform (S3 + SQS + Lambda) |
| 2 | **Filas** (processamento unitário / micro-lote) | [`2-filas/local`](2-filas/local/TUTORIAL.md) — RabbitMQ + Python | [`2-filas/aws`](2-filas/aws/TUTORIAL.md) — SQS + Lambda + S3 |
| 3 | **Kafka + Spark** (janela 30s) | [`3-kafka-spark/local`](3-kafka-spark/local/TUTORIAL.md) — Spark Structured Streaming | — (só local) |
| 4 | **Kafka + Flink** (janela 30s, SQL) | [`4-kafka-flink/local`](4-kafka-flink/local/TUTORIAL.md) — Flink SQL Client | — (só local) |

> **Por que Kafka+Spark e Kafka+Flink são só locais?** Para rodar Kafka gerenciado (Amazon MSK)
> ou Spark/Flink gerenciados (EMR / Managed Flink) na nuvem é caro e frequentemente **bloqueado**
> no AWS Academy Learner Lab. Na AWS demonstramos o paradigma de **filas serverless** (SQS +
> Lambda), que é barato, sempre disponível no Lab e espelha o tutorial local de RabbitMQ.

---

## O evento que trafega

Um **evento de venda** de e-commerce, em **JSON**. É o único "dado" do fluxo — não há banco de
origem: um **producer Python** gera eventos sinteticamente, de forma contínua (~5 eventos/s até
você apertar `Ctrl+C`), amostrando um pequeno catálogo de produtos/categorias embutido no
próprio código.

```json
{
  "evento_id": "a1b2c3d4-...",        // uuid único do evento
  "cliente_id": 7,
  "produto_id": 3,
  "categoria": "Eletronicos",          // embutida no evento (facilita o GROUP BY na janela)
  "quantidade": 2,
  "valor_total": 199.80,
  "data_venda": "2026-07-02T14:23:05.123"     // EVENT-TIME (ISO sem fuso; parseia no Spark e no Flink)
}
```

- **`data_venda`** é o **event-time**: os Tutoriais 3 e 4 abrem janelas de 30s sobre **este**
  campo (não sobre a hora em que o consumer recebeu), com **watermark** para tolerar atraso.
- **`categoria`** vem embutida para que a agregação por categoria não precise de um `join` com
  um catálogo — o foco é o streaming, não a modelagem dimensional.

---

## Os dois tipos de agrupamento

| Tutorial | Transporte | Modelo de consumo | Agrupamento | Saída no S3 |
|---|---|---|---|---|
| **2 – Filas** | SQS / RabbitMQ | 1 mensagem por vez (at-least-once) | **micro-lote** (buffer de N msgs) | `filas/dt=.../lote-*.parquet` |
| **3 – Spark** | Kafka (tópico) | stream particionado | **janela event-time 30s** por categoria | `spark/dt=.../part-*.parquet` |
| **4 – Flink** | Kafka (tópico) | stream particionado | **janela event-time 30s** por categoria | `flink/dt=.../part-*.parquet` |

O contrato de saída das **janelas** (Tut. 3 e 4) é **idêntico**, de propósito, para você
comparar as duas engines:

| Coluna | Tipo | Origem |
|---|---|---|
| `categoria` | STRING | chave do `GROUP BY` |
| `window_start` | TIMESTAMP | início da janela de 30s |
| `window_end` | TIMESTAMP | fim da janela de 30s |
| `qtd_eventos` | BIGINT | `COUNT(*)` |
| `faturamento` | DOUBLE | `SUM(valor_total)` |
| `qtd_itens` | BIGINT | `SUM(quantidade)` |

---

## Convenções

- **Idioma**: português (Brasil).
- **Sistemas operacionais** cobertos no Tutorial 1: **macOS**, **Linux (Ubuntu)** e **Windows
  (PowerShell puro, sem WSL no terminal)**.
- **Sem scripts de aplicação prontos**: os tutoriais **mostram** os comandos e o código
  (producers, consumers, handler Lambda, SQL do Flink) para você **criar no seu ambiente**.
  Vêm prontos no repositório apenas: infraestrutura como código (`docker-compose.yml`,
  `Dockerfile` do Flink, Terraform) e scripts de **validação**.
- **Versões fixadas** (testadas): Apache Kafka (KRaft) 3.x, Spark 3.5.x, Flink 1.20.x,
  RabbitMQ 3.13, Python 3.12, MiniStack `latest`, Terraform provider AWS `~> 5.0`.

---

## Credenciais AWS

As credenciais do Learner Lab ficam em [`../aws_credenciais/`](../aws_credenciais/) e são
**temporárias** (expiram a cada sessão do Lab). O Tutorial 1 AWS mostra como copiá-las para
`~/.aws/`. Reabra o Lab e recopie quando expirarem.

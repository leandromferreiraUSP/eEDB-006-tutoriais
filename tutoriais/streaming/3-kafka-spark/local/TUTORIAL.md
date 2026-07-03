# Tutorial 3 (Local): Streaming com Kafka + Spark (janela de 30s)

> Versão **longa e explicativa**. Você vai publicar um fluxo contínuo de **eventos de venda** em
> um tópico **Kafka** (com um producer Python) e consumi-lo com **Spark Structured Streaming**,
> agregando em **janelas de 30 segundos por categoria** (event-time + watermark) e gravando o
> resultado em **Parquet** no data lake S3 (MiniStack).
>
> **Pré-requisito**: ter feito o `1-infraestrutura/local` e estar com os containers rodando
> (Kafka, Spark, MiniStack). Só os comandos? Veja `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Objetivo técnico e lógico](#1-objetivo-técnico-e-lógico)
2. [Decisões de projeto (e por quê)](#2-decisões-de-projeto-e-por-quê)
3. [Fundamentos: o modelo do Spark Structured Streaming](#3-fundamentos-o-modelo-do-spark-structured-streaming)
4. [O fluxo deste tutorial](#4-o-fluxo-deste-tutorial)
5. [Preparando: tópico e ambiente Python](#5-preparando-tópico-e-ambiente-python)
6. [O producer (Python → Kafka)](#6-o-producer-python--kafka)
7. [O consumer (Spark Structured Streaming)](#7-o-consumer-spark-structured-streaming)
8. [Rodando o consumer (spark-submit conf a conf)](#8-rodando-o-consumer-spark-submit-conf-a-conf)
9. [Verificando o resultado no S3](#9-verificando-o-resultado-no-s3)
10. [Teoria aprofundada: event-time, watermark, janelas e output modes](#10-teoria-aprofundada-event-time-watermark-janelas-e-output-modes)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Objetivo técnico e lógico

Aqui o transporte é um **tópico Kafka** (um log distribuído e particionado) e o processador é o
**Spark Structured Streaming**. O objetivo é **agregar em tempo real**: a cada 30 segundos de
*tempo do evento*, calcular por **categoria** quantos eventos houve, o faturamento e a
quantidade de itens — e materializar isso em **Parquet** no lake.

Diferente do Tutorial 2 (filas, micro-lote sem semântica de tempo), aqui o agrupamento é uma
**janela temporal** ancorada no **event-time** (`data_venda`), a forma correta de responder
"quanto vendemos por categoria a cada 30s".

### 1.1 O que muda de um pipeline em lote (batch) para um em streaming

No mundo **batch** clássico você tem um dataset *finito* (um arquivo, uma tabela), roda uma
query, ela **termina** e produz um resultado. No mundo **streaming** o dado **nunca acaba**: os
eventos chegam continuamente, e a "query" precisa produzir resultados **incrementalmente**,
para sempre. Essa mudança de mentalidade é a chave de tudo o que vem a seguir.

> **Fundamento:** um pipeline de streaming é uma **query que não termina**. Ela lê um fluxo
> potencialmente infinito, mantém um **estado** interno (as janelas ainda abertas) e emite
> resultados aos poucos. Todo o desafio é decidir *quando* um pedaço do resultado está "pronto"
> para ser emitido — e é exatamente para isso que existem o **event-time** e o **watermark**.

> **Por que importa:** a pergunta de negócio ("faturamento por categoria a cada 30s") é
> *contínua*. Se você a respondesse com batch (rodar um job de hora em hora), teria latência
> alta e ainda precisaria reinventar a lógica de janelas temporais. O streaming resolve os dois:
> baixa latência **e** agregação por janela nativa.

### 1.2 A pergunta de negócio, traduzida para uma pipeline

```
  Pergunta:  "Quanto cada categoria vendeu, em janelas de 30 segundos?"
                              │
                              ▼
   evento de venda  ──►  agrupa por (janela de 30s, categoria)  ──►  count, sum(valor), sum(qtd)
   (data_venda,                       ▲                                        │
    categoria,                        │                                        ▼
    valor_total,               ancorado no event-time                    grava 1 linha por
    quantidade)                (quando a venda ocorreu)                   (categoria × janela)
```

Cada linha do resultado é um **fato agregado**: "na janela `20:55:00 → 20:55:30`, a categoria
`Alimentos` teve 72 eventos, R$ 4550 de faturamento e 218 itens". Esse é o produto final que
cai no lake em Parquet, pronto para BI ou para uma camada Silver/Gold.

---

## 2. Decisões de projeto (e por quê)

| Decisão | O que escolhemos | Por quê |
|---|---|---|
| **Janela por event-time** | `window(data_venda, "30 seconds")` | Agrupa por *quando a venda ocorreu*, não por quando o Spark processou. Resultado estável e reproduzível. |
| **Watermark de 10s** | `withWatermark("data_venda", "10 seconds")` | Diz ao Spark "posso esperar até 10s por eventos atrasados"; depois disso a janela é fechada e emitida. É o que permite o **append**. |
| **Output mode `append`** | Grava a janela só quando ela fecha | O sink de arquivo (Parquet) **exige** append + watermark. Cada janela vira arquivo(s) uma única vez. |
| **`spark.sql.shuffle.partitions = 4`** | Reduz de 200 (padrão) para 4 | O padrão gera centenas de arquivos minúsculos por micro-batch. 4 é suficiente e mantém o lake limpo. |
| **Checkpoint local (`/work/...`)** | Não no S3 | O checkpoint faz muitos `rename`, algo caro/frágil no S3. Local é mais rápido e robusto. |
| **`--packages` no spark-submit** | Baixa `spark-sql-kafka` e `hadoop-aws` na hora | Não precisamos montar uma imagem Spark customizada; o Ivy resolve as dependências (cacheadas em `/tmp/.ivy2`). |

Cada linha dessa tabela é uma escolha com um **trade-off** por trás. Vamos destrinchar o
raciocínio — porque entender o "porquê" é o que separa "copiei um comando" de "sei projetar um
pipeline".

### 2.1 Por que janelar por event-time e não por processing-time

> **Teoria:** existem dois "relógios" num sistema de streaming. O **event-time** é o instante em
> que o fato aconteceu no mundo real (o campo `data_venda`, gravado pelo producer). O
> **processing-time** é o instante em que o Spark *viu* aquele registro. Eles quase nunca são
> iguais: rede, buffers do Kafka, retries e o próprio agendamento dos micro-batches introduzem
> atraso variável.
>
> **Por que importa:** se você janelasse por processing-time, o mesmo dado reprocessado amanhã
> cairia em janelas diferentes (porque o "agora" mudou) — o resultado seria **não
> determinístico**. Ao janelar por `data_venda`, a venda das `20:55:03` **sempre** cai na janela
> `20:55:00 → 20:55:30`, hoje ou num reprocessamento daqui a um mês. Corretude e
> reprodutibilidade vêm de graça. Aprofundamos isso na Seção 10.

### 2.2 Por que watermark de 10s (e por que ele é obrigatório aqui)

O watermark de 10s é a promessa: *"vou esperar no máximo 10 segundos por eventos atrasados; quem
chegar depois disso é descartado"*. Sem essa promessa, o Spark **nunca** poderia declarar uma
janela "fechada" — sempre poderia chegar um retardatário — e portanto nunca poderia usar
`append`. O watermark também é o que **limita o estado**: janelas cujo fim já ficou para trás do
watermark são removidas da memória. Sem ele, o estado cresceria para sempre.

### 2.3 Por que `append` num sink de arquivos

Arquivos Parquet são **imutáveis**: você escreve e não reescreve. O modo `append` combina com
isso perfeitamente — ele só escreve uma janela **quando ela fecha**, e apenas **uma vez**. Os
modos `update`/`complete` reescreveriam resultados, o que não faz sentido para um lake de
arquivos append-only (detalhe na Seção 10.4).

### 2.4 Por que baixar 200 → 4 shuffle partitions

Toda agregação (`groupBy`) provoca um **shuffle**, e o Spark cria `spark.sql.shuffle.partitions`
tarefas na saída (padrão **200**). Como cada micro-batch escreve arquivos, 200 partições viram
até **200 arquivos minúsculos por batch** — o clássico *small files problem*, que castiga I/O e
metadados. Com poucos dados por janela, **4** partições bastam e o lake fica limpo (detalhe na
Seção 10.6).

### 2.5 Por que checkpoint local e não no S3

O checkpoint precisa de operações atômicas de `rename`. Em sistemas de arquivos POSIX (disco
local) `rename` é atômico e barato; no S3 ele é **emulado** (copy + delete), lento e sujeito a
consistência eventual. Por isso guardamos o checkpoint em `/work/...` (disco do container) —
mais rápido e robusto (detalhe na Seção 10.5).

### 2.6 Por que `--packages` em vez de imagem customizada

O Spark 3.5.3 não traz o conector Kafka nem o `hadoop-aws` embutidos. Em vez de construir uma
imagem Docker com os JARs, deixamos o **Ivy** (gerenciador de dependências que o `spark-submit`
usa) baixá-los em runtime e cacheá-los. Menos infraestrutura, mais reprodutibilidade (detalhe na
Seção 8).

---

## 3. Fundamentos: o modelo do Spark Structured Streaming

Antes de ler o código, vale entender **como o Spark enxerga um stream**. Esse modelo mental
explica praticamente todas as decisões do consumer.

### 3.1 O stream como uma "tabela infinita" (unbounded table)

O Structured Streaming não inventa uma API nova: ele reaproveita o **DataFrame/SQL** do batch. O
truque conceitual é tratar o fluxo de entrada como uma **tabela que só cresce** (a *unbounded
table*): cada novo evento no Kafka é uma **nova linha** anexada ao fim dessa tabela.

```
              UNBOUNDED TABLE  (append-only, cresce para sempre)
  ┌────────────┬─────────────┬──────────┬────────┬───────────────────┐
  │ evento_id  │ categoria   │  valor   │  qtd   │  data_venda        │
  ├────────────┼─────────────┼──────────┼────────┼───────────────────┤
  │ a1         │ Eletronicos │  3500.00 │   1    │ 20:55:03.120       │
  │ b2         │ Moveis      │   450.00 │   2    │ 20:55:03.450       │
  │ c3         │ Alimentos   │    25.00 │   3    │ 20:55:04.001       │
  │ ...        │ ...         │   ...    │  ...   │ ...   ▼▼▼ (novas linhas chegando sem parar)
  └────────────┴─────────────┴──────────┴────────┴───────────────────┘
```

> **Fundamento:** você escreve a *mesma* transformação que escreveria no batch
> (`groupBy(...).agg(...)`). A diferença é que o Spark a executa **incrementalmente** sobre as
> linhas novas, mantendo os resultados parciais em estado. O `readStream`/`writeStream` (no lugar
> de `read`/`write`) é o que sinaliza "isto é uma tabela infinita".

### 3.2 Processamento incremental em micro-batches

O Spark **não** processa evento a evento (isso é o modelo de "streaming contínuo", experimental).
O motor padrão é o **micro-batch**: de tempos em tempos (o *trigger*), o Spark:

1. pergunta ao Kafka **quais offsets novos** existem desde o último batch;
2. lê esse lote de mensagens;
3. aplica as transformações (parse, `groupBy`, agregações);
4. atualiza o **estado** das janelas e avança o **watermark**;
5. escreve o que ficou pronto no sink;
6. **grava os offsets processados no checkpoint** (para poder retomar após falha).

```
tempo de processamento ────────────────────────────────────────────►
             │◄─ 15s ─►│◄─ 15s ─►│◄─ 15s ─►│◄─ 15s ─►│
             ▼         ▼         ▼         ▼         ▼
 Kafka    [ msgs ]  [ msgs ]  [ msgs ]  [ msgs ]  [ msgs ]
             │         │         │         │         │
 trigger   batch 1   batch 2   batch 3   batch 4   batch 5
           (lê offsets novos → agrega → atualiza estado → escreve → checkpoint)
```

> **Teoria:** o **trigger** controla a cadência dos micro-batches. Aqui usamos
> `trigger(processingTime="15 seconds")`: a cada ~15s o Spark dispara um novo batch. Se um batch
> demora mais que 15s, o próximo começa assim que o anterior termina (não há sobreposição). Outros
> modos existem (`availableNow`, `continuous`), mas `processingTime` é o pão com manteiga.
>
> **Cuidado com dois "tempos" que se confundem:** o *trigger* (15s) é **processing-time** e diz
> "de quanto em quanto tempo eu processo". A *janela* (30s) é **event-time** e diz "como eu
> agrupo os dados". São independentes: o trigger de 15s pode processar dados que caem em várias
> janelas de 30s.

### 3.3 A query "roda para sempre"

Em batch, `df.write...` termina e o programa acaba. Em streaming, `writeStream...start()` devolve
um objeto `StreamingQuery` que fica **rodando em background**, e `q.awaitTermination()` bloqueia
o programa para mantê-lo vivo. A query só para com `Ctrl+C`, erro fatal, ou `q.stop()`.

> **Por que importa:** é por isso que o terminal do `spark-submit` "trava" mostrando logs a cada
> batch — está tudo certo, a query está *viva*. Fechar o terminal mata a query.

### 3.4 O Kafka como *source* do Spark

Quando você faz `readStream.format("kafka")`, o Spark passa a **gerenciar os offsets ele mesmo**
(não usa consumer groups do Kafka para commitar; usa o **checkpoint**). Cada mensagem lida vira
uma linha com um **esquema fixo e cru**:

| Coluna | Tipo | O que é |
|---|---|---|
| `key` | binary | a chave da mensagem (aqui, o `evento_id`) — em bytes |
| `value` | binary | o **payload** da mensagem (aqui, o JSON) — em bytes |
| `topic` | string | nome do tópico |
| `partition` | int | partição de onde veio |
| `offset` | long | posição da mensagem no log daquela partição |
| `timestamp` | timestamp | timestamp **do Kafka** (ingestão), *não* o nosso event-time |
| `timestampType` | int | como o timestamp do Kafka foi definido |

> **Fundamento:** o `value` vem como **binário** porque o Kafka é *schema-agnostic* — ele
> transporta bytes e não sabe (nem liga) se aquilo é JSON, Avro ou uma foto. **Nós** é que sabemos
> que publicamos JSON. Por isso o primeiro passo do consumer é `value.cast("string")` e depois
> `from_json(..., schema)` para "abrir" esses bytes na estrutura que definimos.
>
> **Atenção ao event-time certo:** repare que existe uma coluna `timestamp` **do Kafka**. Ela é
> *processing/ingestion-time* (quando a mensagem entrou no tópico), **não** é o momento da venda.
> Nós deliberadamente ignoramos essa coluna e usamos o `data_venda` de dentro do JSON — porque é
> ele que representa quando o evento realmente ocorreu.

### 3.5 `startingOffsets`: por onde começar a ler

- `latest` (o que usamos): começa a ler **só o que chegar a partir de agora**. Ideal para
  demonstração ao vivo — você liga o producer e vê os eventos fluírem.
- `earliest`: lê **desde o começo** do tópico (todo o histórico retido). Útil para reprocessar.

> **Por que importa:** `startingOffsets` só vale na **primeira** execução (quando ainda não há
> checkpoint). Depois, o Spark **sempre** retoma do offset salvo no checkpoint, ignorando essa
> opção. Se o consumer "não lê nada", quase sempre é `latest` + producer parado (veja
> Troubleshooting).

---

## 4. O fluxo deste tutorial

```
  producer.py (host)                Kafka                 Spark (container)              MiniStack
  gera eventos de venda  ──publica──►  tópico  ──consome──►  readStream.format(kafka)  ──►  s3://datalake/
  ~5-10/s em JSON          :29092      "vendas"    :9092     janela 30s por categoria       spark/dt=.../
                                                             writeStream Parquet            *.parquet
```

Leia o diagrama como uma cadeia de responsabilidades bem separadas:

- **`producer.py` (no host)** — a *fonte de dados*. Roda na sua máquina, dentro de um venv, e
  fala com o Kafka pela porta **29092** (o listener externo/"HOST").
- **Kafka (tópico `vendas`)** — o *buffer durável*. Desacopla quem produz de quem consome: o
  producer pode ir mais rápido que o Spark, que o log absorve a diferença.
- **Spark (no container)** — o *processador*. Fala com o Kafka pela porta **9092** (o listener
  interno, container→container), agrega em janelas e escreve o resultado.
- **MiniStack (S3 local)** — o *destino*. Recebe os Parquet via protocolo `s3a://`.

> **Teoria (por que duas portas para o mesmo Kafka?):** um broker Kafka anuncia *advertised
> listeners* diferentes conforme quem se conecta. Do **host** (fora do Docker) resolvemos
> `localhost:29092`; de **dentro** da rede Docker, os containers alcançam o broker por
> `kafka:9092`. É o mesmo Kafka, dois "endereços" — daí o producer usar `29092` e o consumer usar
> `kafka:9092`.

---

## 5. Preparando: tópico e ambiente Python

Com o ambiente do Tutorial 1 no ar, crie o tópico (se ainda não criou) e um venv para o producer:

```bash
# tópico (idempotente)
docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vendas --partitions 1 --replication-factor 1

# venv do producer (na pasta deste tutorial)
cd tutoriais/streaming/3-kafka-spark/local
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install confluent-kafka
```

Entendendo o comando de criação do tópico:

- `--create --if-not-exists` — **idempotente**: pode rodar quantas vezes quiser; se o tópico já
  existe, não dá erro. Ótimo para scripts repetíveis.
- `--partitions 1` — o tópico terá **uma** partição. Simplicidade máxima para o tutorial.
- `--replication-factor 1` — **uma** cópia de cada mensagem (sem redundância). Só faz sentido em
  ambiente local com um único broker.

> **Teoria (partições = paralelismo do Kafka):** uma partição é um **log ordenado e imutável** —
> mensagens são anexadas ao fim e lidas por offset crescente. O Kafka só garante **ordem dentro de
> uma partição**. O número de partições é o teto de paralelismo de consumo: com 1 partição, no
> máximo 1 tarefa Spark lê esse tópico por vez. Para este exercício (baixo volume, foco em
> janelas), 1 partição basta e ainda nos dá **ordem total** dos eventos de graça.
>
> **Por que importa para o Spark:** o Spark mapeia (por padrão) **1 partição Kafka → 1 tarefa de
> leitura**. Se um dia você precisar de mais vazão, aumentar as partições do tópico é o caminho —
> mas aí perde a ordem total.

> **Teoria (por que um venv):** o `confluent-kafka` é uma biblioteca C compilada (librdkafka). Um
> **ambiente virtual** isola essa dependência do Python do sistema, evitando conflitos de versão.
> É a boa prática para qualquer projeto Python.

---

## 6. O producer (Python → Kafka)

Crie o arquivo **`producer.py`** (na pasta deste tutorial) com o conteúdo abaixo. Ele gera
eventos de venda sinteticamente e publica no tópico `vendas`. Repare no **`data_venda` sem
fuso** (ISO local) — é o event-time que o Spark vai usar para janelar.

```python
import json, random, time, uuid, sys
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = "localhost:29092"     # listener HOST do Kafka
TOPIC = "vendas"
EPS = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0   # eventos por segundo

# catálogo embutido: produto_id -> (nome, categoria, preço)
CATALOGO = {
    1: ("Notebook",  "Eletronicos", 3500.00), 2: ("Mouse",     "Eletronicos",   80.00),
    3: ("Cadeira",   "Moveis",       450.00), 4: ("Mesa",      "Moveis",        900.00),
    5: ("Camiseta",  "Vestuario",     60.00), 6: ("Tenis",     "Vestuario",     300.00),
    7: ("Cafe",      "Alimentos",     25.00), 8: ("Chocolate", "Alimentos",      15.00),
}

def gerar_evento():
    pid = random.choice(list(CATALOGO))
    _, categoria, preco = CATALOGO[pid]
    qtd = random.randint(1, 5)
    return {
        "evento_id": str(uuid.uuid4()),
        "cliente_id": random.randint(1, 20),
        "produto_id": pid,
        "categoria": categoria,
        "quantidade": qtd,
        "valor_total": round(preco * qtd, 2),
        # event-time em ISO SEM fuso (parseia igual no Spark e no Flink)
        "data_venda": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds"),
    }

p = Producer({"bootstrap.servers": BOOTSTRAP})
n = 0
try:
    while True:
        ev = gerar_evento()
        p.produce(TOPIC, key=ev["evento_id"], value=json.dumps(ev))
        p.poll(0)
        n += 1
        if n % 10 == 0:
            print(f"{n} eventos enviados (ultimo: {ev['categoria']} R${ev['valor_total']})", flush=True)
        time.sleep(1.0 / EPS)
except KeyboardInterrupt:
    pass
finally:
    p.flush(5)
    print(f"total enviado: {n}")
```

### 6.1 Lendo o producer por partes

- **`BOOTSTRAP = "localhost:29092"`** — endereço do listener **HOST** (Seção 4). Do seu terminal,
  o Kafka mora aqui.
- **`EPS = float(sys.argv[1]) ...`** — *events per second*, configurável na linha de comando
  (`python producer.py 8` → 8 eventos/s). Controla o `time.sleep(1.0 / EPS)` no fim do laço.
- **`CATALOGO`** — um dicionário fixo `produto_id → (nome, categoria, preço)`. Ter preço fixo por
  produto torna o `valor_total` **verificável**: você pode conferir a soma na saída.
- **`gerar_evento()`** — sorteia um produto, uma quantidade (1–5) e monta o dicionário do evento.
  O `valor_total` é `preço × quantidade` arredondado.
- **`data_venda`** — é o **coração do event-time**. Detalhes abaixo.

> **Fundamento (o event-time nasce aqui):** `datetime.now(timezone.utc).replace(tzinfo=None)` pega
> o instante **em UTC** e depois **remove a marca de fuso** (`tzinfo=None`), gerando um timestamp
> "ingênuo" (*naive*). O `.isoformat(timespec="milliseconds")` serializa como
> `"2026-07-02T20:55:03.120"`. Guardar UTC evita a bagunça de horário de verão/fuso; remover o
> `tzinfo` faz a string **parsear de forma idêntica** no Spark e no Flink (que interpretam ISO sem
> fuso como hora local do processamento — como todos os containers usam UTC, tudo bate).
>
> **Por que importa:** esse campo é o **carimbo de quando a venda ocorreu**. É ele — e não a hora
> em que o Spark leu a mensagem — que define em qual janela de 30s o evento cai. Toda a corretude
> do pipeline depende de ele ser gerado corretamente na origem.

### 6.2 O ciclo de publicação e a semântica assíncrona

- **`p = Producer({"bootstrap.servers": BOOTSTRAP})`** — cria o produtor; ele mantém um **buffer
  interno** e uma thread de I/O que envia as mensagens em lotes.
- **`p.produce(TOPIC, key=..., value=json.dumps(ev))`** — **não** envia na hora; **enfileira** a
  mensagem no buffer local. A `key` é o `evento_id`; o `value` é o JSON serializado.
- **`p.poll(0)`** — "serve" a fila de eventos internos do produtor (callbacks de entrega,
  reconexões) sem bloquear. Chamá-lo a cada envio mantém o buffer saudável.
- **`time.sleep(1.0 / EPS)`** — regula a taxa de emissão para ~`EPS` eventos por segundo.
- **`finally: p.flush(5)`** — no encerramento (inclusive `Ctrl+C`), **força o envio** do que
  ainda está no buffer, esperando até 5s. Sem isso, as últimas mensagens poderiam se perder.

> **Teoria (produção assíncrona):** produtores Kafka são assíncronos por desempenho — agrupam
> mensagens (*batching*) e as comprimem antes de mandar para o broker. `produce()` só *agenda*;
> quem realmente entrega é a thread de background, cutucada por `poll()` e drenada por `flush()`.
> Por isso a dupla `poll(0)` (no laço) + `flush(5)` (no fim) é o padrão idiomático.
>
> **Por que a `key` importa:** a chave decide **em qual partição** a mensagem cai (via hash). Como
> o `evento_id` é um UUID único, as mensagens se espalham uniformemente — e mensagens com a mesma
> chave sempre iriam para a mesma partição, preservando ordem por chave. Aqui, com 1 partição,
> isso é acadêmico, mas é um hábito importante para tópicos particionados.

Rode-o em um terminal e **deixe rodando** (é o fluxo contínuo):

```bash
python producer.py 8        # 8 eventos/s
```

**Resultado esperado**: linhas `10 eventos enviados...`, `20 eventos enviados...`.

> Verifique que chegam no Kafka (em outro terminal):
> ```bash
> docker exec streaming_kafka /opt/kafka/bin/kafka-console-consumer.sh \
>   --bootstrap-server localhost:9092 --topic vendas --max-messages 3
> ```

> **Dica de leitura do teste acima:** o `kafka-console-consumer.sh` roda **dentro** do container e
> por isso usa `localhost:9092` (lá dentro, o broker é local). Ele lê 3 mensagens e sai
> (`--max-messages 3`). Se você vir 3 JSONs de venda, a metade "produtora" do pipeline está
> 100% funcional — independente do Spark.

---

## 7. O consumer (Spark Structured Streaming)

O consumer roda **dentro do container Spark**. Por isso, crie o arquivo em
`1-infraestrutura/local/docker/work/consumer_spark.py` — essa pasta é montada como **`/work`**
dentro do container.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, to_date, to_timestamp
from pyspark.sql.functions import sum as _sum, count as _count
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = (SparkSession.builder
    .appName("vendas-janela-30s")
    .config("spark.sql.shuffle.partitions", "4")   # evita centenas de arquivos minúsculos
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# esquema do evento (o value do Kafka é um JSON)
schema = (StructType()
    .add("evento_id", StringType()).add("cliente_id", IntegerType())
    .add("produto_id", IntegerType()).add("categoria", StringType())
    .add("quantidade", IntegerType()).add("valor_total", DoubleType())
    .add("data_venda", StringType()))

# 1) lê o tópico como stream
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")   # listener INTERNAL (container→container)
    .option("subscribe", "vendas")
    .option("startingOffsets", "latest")
    .load())

# 2) desserializa o JSON e converte data_venda em timestamp (event-time)
eventos = (raw
    .select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")
    .withColumn("data_venda", to_timestamp(col("data_venda"))))

# 3) janela tumbling de 30s por categoria, com watermark de 10s
agg = (eventos
    .withWatermark("data_venda", "10 seconds")
    .groupBy(window(col("data_venda"), "30 seconds"), col("categoria"))
    .agg(_count("*").alias("qtd_eventos"),
         _sum("valor_total").alias("faturamento"),
         _sum("quantidade").alias("qtd_itens")))

# 4) achata a struct window e adiciona a partição dt
saida = agg.select(
    col("categoria"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("qtd_eventos"), col("faturamento"), col("qtd_itens"),
    to_date(col("window.start")).alias("dt"))

# 5) grava Parquet no S3 (append: cada janela fechada vira arquivo uma vez)
q = (saida.writeStream.format("parquet")
    .option("path", "s3a://datalake/spark/")
    .option("checkpointLocation", "/work/checkpoint_spark")
    .partitionBy("dt")
    .outputMode("append")
    .trigger(processingTime="15 seconds")
    .start())

q.awaitTermination()
```

Vamos ler **bloco a bloco**. Este é o cérebro do tutorial.

### 7.1 Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, to_date, to_timestamp
from pyspark.sql.functions import sum as _sum, count as _count
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
```

- `SparkSession` — o ponto de entrada de toda aplicação Spark moderna.
- `from_json` — transforma uma **string JSON** em uma coluna estruturada (dado o schema).
- `window` — a função que cria a **janela temporal** (gera uma struct `start`/`end`).
- `to_date`, `to_timestamp` — conversões de tipo (string → timestamp; timestamp → date).
- `sum as _sum, count as _count` — renomeados com `_` para **não colidir** com o `sum`/`count`
  nativos do Python. Detalhe pequeno, mas evita bugs sutis.

### 7.2 A SparkSession

```python
spark = (SparkSession.builder
    .appName("vendas-janela-30s")
    .config("spark.sql.shuffle.partitions", "4")   # evita centenas de arquivos minúsculos
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- **`.appName(...)`** — nome que aparece na Spark UI e nos logs (facilita identificar o job).
- **`.config("spark.sql.shuffle.partitions", "4")`** — reduz o número de partições pós-shuffle de
  **200** (padrão) para **4**. Como todo `groupBy` gera um shuffle e cada partição de saída pode
  virar um arquivo, esse ajuste é o que evita o *small files problem* (Seção 10.6).
- **`.getOrCreate()`** — reaproveita a sessão se já existir, senão cria.
- **`setLogLevel("WARN")`** — silencia o `INFO` verborrágico do Spark; você só vê avisos e erros —
  e as próprias mensagens de progresso dos micro-batches.

> **Por que fixar shuffle partitions logo no builder:** essa config é **global** para a sessão e
> afeta *todas* as agregações. Colocá-la no builder garante que já valha desde o primeiro batch.

### 7.3 O esquema do evento

```python
schema = (StructType()
    .add("evento_id", StringType()).add("cliente_id", IntegerType())
    .add("produto_id", IntegerType()).add("categoria", StringType())
    .add("quantidade", IntegerType()).add("valor_total", DoubleType())
    .add("data_venda", StringType()))
```

Definimos o schema **explicitamente**, campo a campo, batendo com o JSON que o producer emite.
Note que `data_venda` entra como `StringType()` — vamos convertê-la para timestamp no passo 2.

> **Fundamento (por que schema explícito em streaming):** em batch, o Spark pode *inferir* o
> schema lendo uma amostra do arquivo. Em streaming isso é impossível — o dado ainda nem chegou.
> Além disso, inferência custaria um passe extra a cada batch. Declarar o schema é obrigatório e
> saudável: torna o pipeline **determinístico** e **rápido**. Campos ausentes no JSON viram
> `null`; campos extras são ignorados.

### 7.4 Passo 1 — ler o Kafka como stream

```python
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")   # listener INTERNAL (container→container)
    .option("subscribe", "vendas")
    .option("startingOffsets", "latest")
    .load())
```

- **`spark.readStream`** — em vez de `spark.read`, sinaliza que a fonte é **infinita** (stream).
- **`.format("kafka")`** — usa o conector `spark-sql-kafka-0-10` (que trazemos via `--packages`).
- **`.option("kafka.bootstrap.servers", "kafka:9092")`** — endereço **interno** do broker; como o
  Spark roda **dentro** da rede Docker, ele alcança o Kafka por `kafka:9092` (não `29092`).
- **`.option("subscribe", "vendas")`** — assina o tópico `vendas`.
- **`.option("startingOffsets", "latest")`** — na primeira execução, começa a ler só o que chegar
  a partir de agora (Seção 3.5).
- **`.load()`** — materializa o DataFrame de streaming com as colunas cruas do Kafka
  (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`, ...).

> **Por que importa a diferença de porta:** este é o erro nº 1 de quem começa. Do **host** o Kafka
> é `localhost:29092` (producer); de **dentro do Docker** é `kafka:9092` (consumer). Trocar isso dá
> `Connection refused`.

### 7.5 Passo 2 — desserializar o JSON e criar o event-time

```python
eventos = (raw
    .select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")
    .withColumn("data_venda", to_timestamp(col("data_venda"))))
```

Três coisas acontecem aqui, em sequência:

1. **`col("value").cast("string")`** — o `value` do Kafka é **binário** (Seção 3.4); convertemos
   os bytes para texto (a string JSON).
2. **`from_json(..., schema).alias("e")` + `.select("e.*")`** — aplicamos o schema para
   "explodir" a string JSON numa struct chamada `e`, e depois **achatamos** essa struct em colunas
   de primeiro nível (`evento_id`, `categoria`, `valor_total`, ...). Agora temos um DataFrame
   tabular normal.
3. **`.withColumn("data_venda", to_timestamp(col("data_venda")))`** — convertemos a coluna de
   **string** para **timestamp**. Esse passo é **obrigatório**: `window(...)` e `withWatermark(...)`
   só operam sobre um tipo `timestamp`, não sobre texto.

> **Fundamento (bytes → estrutura):** este bloco é a ponte entre "o Kafka só transporta bytes" e "o
> Spark precisa de colunas tipadas". Se o schema não bater com o JSON (nome de campo diferente,
> tipo incompatível), os campos afetados viram `null` — silenciosamente. Um `null` inesperado em
> `data_venda` é a causa clássica de "as janelas não fecham".

### 7.6 Passo 3 — watermark + janela + agregação (o núcleo)

```python
agg = (eventos
    .withWatermark("data_venda", "10 seconds")
    .groupBy(window(col("data_venda"), "30 seconds"), col("categoria"))
    .agg(_count("*").alias("qtd_eventos"),
         _sum("valor_total").alias("faturamento"),
         _sum("quantidade").alias("qtd_itens")))
```

Este é o coração semântico do pipeline. A **ordem** das chamadas importa:

- **`.withWatermark("data_venda", "10 seconds")`** — declara que o event-time é `data_venda` e que
  toleramos **até 10s de atraso**. O Spark passa a rastrear o **maior `data_venda` já visto** e
  define `watermark = máximo_visto − 10s`. Janelas cujo `end` fica **abaixo** do watermark são
  consideradas fechadas (e emitidas), e seu estado é liberado. Precisa vir **antes** do `groupBy`
  para que a agregação saiba qual coluna é o event-time.
- **`window(col("data_venda"), "30 seconds")`** — cria janelas **tumbling** (fixas, sem
  sobreposição) de 30s alinhadas ao relógio (`...:00:00–00:30`, `...:00:30–01:00`, ...). Retorna
  uma **struct** com `window.start` e `window.end`.
- **`.groupBy(window(...), col("categoria"))`** — agrupa por **(janela, categoria)**. Cada
  combinação vira uma linha de resultado.
- **`.agg(...)`** — as três métricas de negócio:
  - `_count("*")` → `qtd_eventos` (quantas vendas na janela/categoria);
  - `_sum("valor_total")` → `faturamento`;
  - `_sum("quantidade")` → `qtd_itens`.

> **Teoria (o que o Spark guarda em estado):** para cada `(janela, categoria)` ainda **aberta**, o
> Spark mantém em memória os acumuladores parciais (contagem e somas). Quando o watermark
> ultrapassa o `window.end`, ele **emite** a linha final daquela janela e **descarta** o estado. É
> assim que o estado permanece **limitado** mesmo com um stream infinito — sem o watermark, o
> número de janelas abertas cresceria para sempre.

```
event-time ──────────────────────────────────────────────────────────►
        janela A [00:00 → 00:30)     janela B [00:30 → 01:00)
        ├───────────────────────────┤├───────────────────────────┤
   eventos: ● ● ●   ● ●  ●               ● ●  ● ● ●   ●
                                                    ▲ máximo visto = 01:12
                                    watermark = 01:12 − 10s = 01:02
                                    ⇒ janela A e B já têm end < watermark ⇒ FECHAM e são emitidas
```

### 7.7 Passo 4 — achatar a struct e criar a partição `dt`

```python
saida = agg.select(
    col("categoria"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("qtd_eventos"), col("faturamento"), col("qtd_itens"),
    to_date(col("window.start")).alias("dt"))
```

- **`col("window.start")` / `col("window.end")`** — extraímos os dois campos da struct `window`
  para colunas planas `window_start`/`window_end` (bem mais amigáveis num Parquet/BI).
- **`to_date(col("window.start")).alias("dt")`** — derivamos a **data** (só o dia) a partir do
  início da janela. Essa coluna `dt` será a **chave de particionamento** no disco.

> **Fundamento (por que uma coluna de data derivada):** o particionamento por data é o padrão
> ouro em data lakes. Ter uma coluna `dt = YYYY-MM-DD` explícita nos dá diretórios
> `dt=2026-07-02/`, permitindo *partition pruning* (o leitor pula dias que não interessam). Voltamos
> a isso na Seção 9.

### 7.8 Passo 5 — escrever em Parquet no S3 (o sink)

```python
q = (saida.writeStream.format("parquet")
    .option("path", "s3a://datalake/spark/")
    .option("checkpointLocation", "/work/checkpoint_spark")
    .partitionBy("dt")
    .outputMode("append")
    .trigger(processingTime="15 seconds")
    .start())

q.awaitTermination()
```

- **`saida.writeStream`** — inicia a definição do **sink** de streaming.
- **`.format("parquet")`** — sink de **arquivo** colunar (Seção 9).
- **`.option("path", "s3a://datalake/spark/")`** — destino no lake, via protocolo **`s3a://`**
  (implementado pelo `hadoop-aws`, Seção 8). Vira `s3://datalake/spark/dt=.../part-*.parquet`.
- **`.option("checkpointLocation", "/work/checkpoint_spark")`** — onde o Spark grava **offsets do
  Kafka + estado das janelas + metadados dos commits**. Fica em disco **local** do container (não
  no S3) de propósito (Seção 10.5).
- **`.partitionBy("dt")`** — cria a estrutura de diretórios `dt=YYYY-MM-DD/`.
- **`.outputMode("append")`** — escreve cada janela **uma única vez**, quando ela fecha. É o único
  modo compatível com sink de arquivo + agregação com watermark (Seção 10.4).
- **`.trigger(processingTime="15 seconds")`** — dispara um micro-batch a cada ~15s (Seção 3.2).
- **`.start()`** — **inicia** a query; devolve um `StreamingQuery` rodando em background.
- **`q.awaitTermination()`** — bloqueia o programa para manter a query **viva** (Seção 3.3).

> **Por que importa a combinação `append` + `watermark` + arquivo:** o sink de arquivo é
> **append-only** (não reescreve). O modo `append` só emite uma janela quando o watermark garante
> que ela não vai mais mudar. Sem o watermark, o Spark **não teria como saber** quando a janela
> "terminou" e recusaria o `append` com erro. Os três se sustentam mutuamente.

---

## 8. Rodando o consumer (spark-submit conf a conf)

Submeta o job dentro do container Spark. **Preste atenção nos `--conf`** (todos importantes):

```bash
docker exec -it streaming_spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.endpoint=http://ministack:4566 \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /work/consumer_spark.py
```

O que cada bloco faz:

| `--conf` / `--packages` | Papel |
|---|---|
| `spark.jars.ivy=/tmp/.ivy2` | pasta gravável p/ o Ivy baixar os JARs (o `user.home` do container não é gravável) |
| `--packages ...spark-sql-kafka...,...hadoop-aws...` | conector Kafka + suporte a `s3a://` |
| `fs.s3a.endpoint=http://ministack:4566` | aponta o S3A para o MiniStack (em vez da AWS real) |
| `fs.s3a.path.style.access=true` | MiniStack usa path-style (`endpoint/bucket`), não `bucket.endpoint` |
| `fs.s3a.connection.ssl.enabled=false` | o MiniStack fala HTTP, não HTTPS |
| `fs.s3a...SimpleAWSCredentialsProvider` | usa as credenciais `test/test` acima |

Agora o **porquê técnico** de cada peça — este comando parece um monstro, mas cada flag resolve um
problema concreto.

### 8.1 `docker exec -it streaming_spark /opt/spark/bin/spark-submit`

Executa o `spark-submit` **dentro** do container `streaming_spark`. O `-it` dá terminal
interativo (você vê os logs ao vivo e pode `Ctrl+C`). O `spark-submit` é o lançador oficial: ele
resolve dependências, sobe o *driver*, agenda as tarefas e inicia a aplicação.

### 8.2 `--conf spark.jars.ivy=/tmp/.ivy2` e `--packages ...` (dependências via Ivy)

```
--conf spark.jars.ivy=/tmp/.ivy2
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4
```

- **`--packages`** lista dependências no formato Maven `grupo:artefato:versão`. O `spark-submit`
  aciona o **Ivy** (resolvedor de dependências) para **baixá-las em runtime**, junto com suas
  dependências transitivas, e injetá-las no classpath do driver e dos executores.
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` — o conector Kafka. O `_2.12` é a versão do
    **Scala** (deve casar com a do Spark) e `3.5.3` é a versão do **Spark** (idem).
  - `org.apache.hadoop:hadoop-aws:3.3.4` — traz o `S3AFileSystem` (o driver do `s3a://`) e puxa o
    `aws-java-sdk-bundle`.
- **`--conf spark.jars.ivy=/tmp/.ivy2`** — redireciona o diretório de trabalho/cache do Ivy para
  `/tmp/.ivy2`.

> **Fundamento (por que apontar o Ivy para `/tmp/.ivy2`):** por padrão o Ivy escreve em
> `~/.ivy2` (o `user.home`). No container Spark, o home do usuário **não é gravável**, então o Ivy
> falharia ao tentar criar `~/.ivy2/cache/.../resolved-*.xml`. Apontando para `/tmp/.ivy2`
> (gravável, e mapeado para o volume `ivy-cache`), a resolução funciona **e** fica **cacheada**:
> a 1ª execução baixa ~200 MB; as próximas reaproveitam o cache e sobem rápido.
>
> **Por que essas versões exatas casam:** o Spark **3.5.3** é compilado contra o **Hadoop 3.3.x**.
> O `hadoop-aws` **3.3.4** pertence a essa mesma linha, então o `S3AFileSystem` e as classes
> Hadoop já presentes no Spark são **compatíveis**. Misturar versões (ex.: `hadoop-aws` 3.2 ou
> 3.4) provoca `NoSuchMethodError`/`ClassNotFoundException`. A regra de ouro: alinhe `hadoop-aws`
> à versão de Hadoop que o seu Spark embute.

### 8.3 As seis `--conf spark.hadoop.fs.s3a.*` (o conector S3A contra o MiniStack)

Tudo que começa com `spark.hadoop.` é repassado para a **configuração do Hadoop** (o `s3a://` é
um sistema de arquivos Hadoop). Estas seis linhas ensinam o S3A a falar com o **MiniStack** (um S3
emulado local) em vez da AWS de verdade.

| `--conf` | Valor | O porquê técnico |
|---|---|---|
| `fs.s3a.endpoint` | `http://ministack:4566` | Por padrão o S3A conecta na AWS (`s3.amazonaws.com`). Aqui redirecionamos para o **endpoint do MiniStack** dentro da rede Docker (host `ministack`, porta `4566`). |
| `fs.s3a.access.key` | `test` | Credencial de acesso (o MiniStack aceita qualquer par; usamos `test`). |
| `fs.s3a.secret.key` | `test` | Segredo correspondente. |
| `fs.s3a.path.style.access` | `true` | Força URL **path-style** (`http://ministack:4566/datalake/...`). O padrão AWS é **virtual-hosted** (`http://datalake.ministack:4566/...`), que **não resolve** em DNS local. |
| `fs.s3a.connection.ssl.enabled` | `false` | O MiniStack expõe **HTTP puro** (não HTTPS). Sem isso, o S3A tentaria TLS e a conexão falharia. |
| `fs.s3a.aws.credentials.provider` | `...SimpleAWSCredentialsProvider` | Diz ao S3A para usar **exatamente** as `access.key`/`secret.key` acima, em vez de caçar credenciais em variáveis de ambiente, perfis `~/.aws` ou metadata de instância EC2. |

> **Fundamento (por que `s3a://` e não `s3://`):** `s3a` é a implementação **moderna e mantida** do
> cliente S3 no Hadoop (sucede o antigo `s3n`/`s3`). É ela que o `hadoop-aws` fornece, com pool de
> conexões, leituras eficientes e suporte a endpoint customizado — essencial para apontar ao
> MiniStack.
>
> **Por que path-style é imprescindível aqui:** o estilo virtual-hosted embute o nome do bucket no
> **hostname** (`datalake.ministack`). Isso funciona na AWS (DNS wildcard), mas dentro do Docker
> `datalake.ministack` não existe. O path-style mantém o host fixo (`ministack`) e coloca o bucket
> no **caminho** — que resolve sempre.
>
> **Por que fixar o `credentials.provider`:** por padrão o S3A tenta uma **cadeia** de provedores
> (env vars, arquivo de credenciais, IAM da instância...). Num ambiente local isso pode "vazar"
> para credenciais reais da AWS ou falhar por timeout tentando o metadata endpoint. Fixar o
> `SimpleAWSCredentialsProvider` torna o comportamento **determinístico**: usa `test/test` e ponto.

### 8.4 O script

```
/work/consumer_spark.py
```

O último argumento é o script a executar. Lembre que `/work` é a pasta
`1-infraestrutura/local/docker/work/` montada no container — por isso salvamos o
`consumer_spark.py` lá.

> **Primeira execução é lenta** (~1–2 min): o Ivy baixa `hadoop-aws` + `aws-java-sdk-bundle`
> (~200 MB). Como cacheamos em `/tmp/.ivy2` (volume `ivy-cache`), as próximas são rápidas.

Deixe rodando. A cada ~15s o Spark processa um micro-batch; a cada **janela de 30s que fecha**
(após o watermark), ele grava Parquet.

> **O que você vê no terminal:** linhas de progresso a cada trigger. Nos primeiros ~40s
> (30s de janela + 10s de watermark) **nada** é escrito no S3 — é esperado: nenhuma janela fechou
> ainda. Paciência é parte do protocolo.

---

## 9. Verificando o resultado no S3

Em outro terminal, após ~60–90s (tempo de 2–3 janelas fecharem):

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/spark/ --recursive
```

**Resultado esperado**: arquivos `spark/dt=YYYY-MM-DD/part-*.snappy.parquet`.

Baixe um e leia (com o venv do producer, que já tem... ou instale `pyarrow`):

```bash
pip install pyarrow
aws --endpoint-url http://localhost:4566 s3 cp \
  "$(aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/spark/dt=$(date -u +%F)/ | awk '{print $4}' | head -1 | sed 's#^#s3://datalake/spark/dt='$(date -u +%F)'/#')" /tmp/janela.parquet
python -c "import pyarrow.parquet as pq; print(pq.read_table('/tmp/janela.parquet').to_pylist())"
```

**Resultado esperado** (uma linha por categoria por janela):

```python
[{'categoria': 'Alimentos', 'window_start': ...20:55:00, 'window_end': ...20:55:30,
  'qtd_eventos': 72, 'faturamento': 4550.0, 'qtd_itens': 218}]
```

Note que `window_end - window_start = 30s` exatos. 🎯

### 9.1 Como ler os comandos de verificação

- **`export AWS_ACCESS_KEY_ID=test ...`** — o CLI `aws` (que roda no **host**) também precisa de
  credenciais; damos as mesmas `test/test`. O host alcança o MiniStack por `localhost:4566` (não
  `ministack:4566`, que só existe dentro do Docker).
- **`--endpoint-url http://localhost:4566`** — redireciona o `aws` para o MiniStack (mesma ideia do
  `fs.s3a.endpoint`, só que do lado do host).
- **`s3 ls ... --recursive`** — lista o que o Spark escreveu; você deve ver os diretórios
  `dt=.../` com os `part-*.snappy.parquet`.
- A **linha do `cp`** apenas seleciona o **primeiro** arquivo da partição de hoje
  (`$(date -u +%F)` = data UTC) e o baixa; o `python -c ...` lê o Parquet com `pyarrow` e imprime
  as linhas como dicionários.

### 9.2 Por que Parquet (colunar) e por que particionar por `dt`

> **Fundamento (formato colunar):** o Parquet guarda os dados **por coluna**, não por linha. Isso
> traz três ganhos enormes para analytics: (1) **compressão** muito melhor (valores de uma mesma
> coluna são parecidos — repare no `.snappy` do nome, o codec padrão); (2) **projeção** — ler só as
> colunas necessárias, sem tocar no resto; (3) **estatísticas por bloco** (min/max), que permitem
> *pular* blocos irrelevantes numa consulta. É o formato de fato dos data lakes.
>
> **Fundamento (particionamento por data):** `partitionBy("dt")` grava em subpastas
> `dt=2026-07-02/`. Um engine de consulta (Spark, Athena, Trino) que filtre `WHERE dt = '...'` lê
> **apenas** aquela pasta — o *partition pruning*. Como quase toda análise de vendas é recortada por
> período, particionar por dia reduz drasticamente o volume lido. O nome no estilo `chave=valor` é o
> **Hive-style partitioning**, entendido nativamente por essas ferramentas.
>
> **Por que importa:** colunar + particionado é a diferença entre uma consulta que varre gigabytes
> e uma que lê megabytes. O Spark já entrega o dado nesse formato "pronto para BI".

---

## 10. Teoria aprofundada: event-time, watermark, janelas e output modes

Esta seção consolida a teoria que amarra o pipeline. Se você entender estes seis pontos, entende
Structured Streaming.

### 10.1 Event-time × processing-time

- **Event-time** é o campo `data_venda` (quando a venda ocorreu). A janela é calculada sobre
  ele, então o resultado **não depende** de quando o Spark rodou.
- **Processing-time** é quando o Spark *viu* o registro (afetado por rede, buffers do Kafka,
  atraso do micro-batch).

```
                       janela (event-time) [20:55:00 → 20:55:30)
 event-time:   ●────────●──────────●─────────────●          ← quando a venda OCORREU
                                                    \
 rede/kafka/buffer (atraso variável)                 \
                                                       ▼
 processing:                        ● ● ●  ●  ●  ●  ●        ← quando o Spark PROCESSOU
```

> **Teoria:** os dois relógios divergem por causa de atraso variável no transporte. Se
> janelássemos por processing-time, dois eventos que ocorreram no **mesmo** instante poderiam cair
> em janelas **diferentes** só porque um demorou mais na rede — e um reprocessamento amanhã daria
> outro resultado (o "agora" mudou).
>
> **Por que importa (corretude e reprocessamento):** ao janelar por `data_venda`, a agregação é
> uma **função pura** do dado. Reprocessar o mesmo tópico produz **exatamente** o mesmo Parquet.
> Isso é o que permite reprocessar histórico, corrigir bugs e comparar resultados com confiança.

### 10.2 Watermark

- **Watermark (10s)**: o Spark acompanha o maior `data_venda` visto e assume que não chegarão
  eventos mais antigos que `(máximo − 10s)`. Quando o watermark ultrapassa o fim de uma janela,
  ela é **finalizada** e emitida. Eventos que chegarem atrasados **além** dos 10s são
  descartados daquela janela.

```
 max event-time visto:            20:55:41
 watermark = max − 10s:           20:55:31
 janela [20:55:00 → 20:55:30):    end(30) < watermark(31)  ⇒  FECHA e emite ✔
 janela [20:55:30 → 20:56:00):    end(60) > watermark(31)  ⇒  ainda ABERTA, acumulando
 evento atrasado com data_venda = 20:55:18 chegando agora  ⇒  < watermark  ⇒  DESCARTADO ✘
```

> **Fundamento (o watermark limita o estado):** um stream é infinito; sem um critério de "fim", o
> Spark teria que manter **todas** as janelas já vistas em memória, para sempre — o estado
> explodiria. O watermark é esse critério: ele autoriza o motor a **fechar** janelas antigas,
> **emiti-las** e **liberar** a memória delas. É o mecanismo que torna a agregação por janela
> viável num fluxo sem fim.
>
> **O trade-off do valor (10s):** watermark maior = mais tolerância a atrasados, porém mais estado
> retido e maior latência para fechar a janela. Menor = fecha rápido e usa menos memória, mas
> descarta mais retardatários. 10s é um meio-termo didático para este tutorial.

### 10.3 Janelas: tumbling × sliding × session

Usamos **tumbling** (fixas, contíguas, sem sobreposição). Vale conhecer as três famílias:

| Tipo | Como é | Chamada típica | Cada evento cai em... |
|---|---|---|---|
| **Tumbling** (usada aqui) | blocos fixos de tamanho igual, sem sobreposição | `window(col, "30 seconds")` | **1** janela |
| **Sliding** | tamanho fixo, mas avança em passos menores (janelas se sobrepõem) | `window(col, "30 seconds", "10 seconds")` | **várias** janelas |
| **Session** | agrupa por atividade; fecha após um período de inatividade (*gap*) | `session_window(col, "5 minutes")` | 1 sessão (tamanho variável) |

```
 tumbling (30s):   |----A----|----B----|----C----|      ← contíguas, sem overlap
 sliding (30s/10): |----A----|
                       |----B----|
                           |----C----|                  ← se sobrepõem
```

> **Teoria:** `window(col("data_venda"), "30 seconds")` gera uma **struct** com `start` e `end`
> (por isso extraímos `window.start`/`window.end` no passo 4). As janelas tumbling são alinhadas ao
> epoch (`...:00`, `...:30`), então independem de quando o job começou — mais uma fonte de
> reprodutibilidade.
>
> **Por que tumbling aqui:** a pergunta é "faturamento **a cada** 30s", ou seja, blocos disjuntos.
> Cada venda deve ser contada **uma vez**. Sliding serviria para médias móveis; session, para
> agrupar atividade de um usuário. Cada forma responde a um tipo de pergunta.

### 10.4 Output modes: append × update × complete

- **Append**: como a janela só é escrita quando fecha, cada resultado vai ao Parquet **uma
  única vez** — perfeito para um sink de arquivos imutáveis. (Sem watermark, o Spark não saberia
  quando uma janela "terminou" e o append seria impossível.)

| Modo | O que emite a cada batch | Serve para sink de arquivo? |
|---|---|---|
| **append** (usado aqui) | apenas **linhas novas e definitivas** (janelas que fecharam) | **Sim** — escreve cada janela 1 vez |
| **update** | apenas as linhas que **mudaram** neste batch | Não para arquivo (implicaria reescrever) |
| **complete** | a **tabela inteira** de resultados, do zero, todo batch | Não — recriaria tudo e cresceria sem limite |

> **Fundamento (por que arquivo exige append):** um Parquet, uma vez escrito, é **imutável** — não
> dá para "atualizar uma linha". O `append` combina com isso porque só emite resultados
> **finalizados** (graças ao watermark), que nunca mais mudarão. O `update` precisaria reescrever
> linhas já gravadas (impossível em arquivo append-only); o `complete` reescreveria **tudo** a cada
> batch, o que num stream infinito é inviável (cresceria sem parar). Por isso o Spark **recusa**
> `complete`/`update` para o `FileSink` com agregação.
>
> **Por que importa:** é a razão pela qual `append` + `watermark` andam juntos neste tutorial —
> um habilita o outro, e ambos são exigência do sink de arquivo.

### 10.5 Checkpointing e semântica de entrega

> **Fundamento (o que o checkpoint guarda):** em `checkpointLocation` o Spark persiste, a cada
> batch: (1) os **offsets do Kafka** que serão processados (o *offset log*), (2) os que já foram
> confirmados (o *commit log*), e (3) o **estado** das agregações (as janelas abertas). Se o job
> cair e reiniciar, ele **relê** o checkpoint, retoma exatamente dos offsets pendentes e
> reconstrói o estado — sem perder nem duplicar processamento.
>
> **Teoria (semântica de entrega):** essa combinação — offsets replayáveis do Kafka + estado
> versionado + sink idempotente/transacional — dá **exactly-once** para o resultado das
> agregações. É por isso que o checkpoint é **obrigatório** e não pode ser trocado à toa: apagá-lo
> equivale a "esquecer" onde estava.
>
> **Por que local (`/work/...`) e não no S3:** o protocolo de checkpoint usa `rename` como
> operação **atômica** para "comitar" um passo. Em disco local (POSIX) `rename` é atômico e
> instantâneo. No S3 **não existe** rename nativo — é emulado por *copy + delete*, que é lento,
> caro e sujeito a consistência eventual, podendo corromper o protocolo. Daí guardarmos o
> checkpoint em disco do container.

Para **parar** o consumer, `Ctrl+C` no terminal do `spark-submit`. Para reprocessar do zero,
apague o checkpoint: `docker exec streaming_spark rm -rf /work/checkpoint_spark`.

> **Cuidado:** apagar o checkpoint faz o job recomeçar do `startingOffsets` (aqui, `latest` = só o
> novo). O estado das janelas antigas se perde. É exatamente o que você quer para "começar do zero"
> num tutorial — mas em produção, apagar checkpoint é uma decisão séria.

### 10.6 Shuffle partitions e o problema dos "small files"

> **Teoria:** todo `groupBy`/agregação provoca um **shuffle** — a rede redistribui as linhas por
> chave. O número de partições resultantes é `spark.sql.shuffle.partitions`, cujo **padrão é 200**.
> Como o sink de arquivo escreve (potencialmente) **um arquivo por partição por batch**, o padrão
> geraria **até 200 arquivos minúsculos a cada micro-batch**.
>
> **Por que isso é ruim (small files problem):** cada arquivo tem custo fixo de **metadados** e de
> **abrir/fechar** na leitura. Milhares de arquivos de poucos KB deixam qualquer consulta lenta
> (o engine gasta mais tempo listando/abrindo do que lendo dados) e incham o catálogo. Em S3, cada
> `PUT`/`LIST` ainda é uma chamada de rede.
>
> **A escolha (4):** como cada janela de 30s tem poucas categorias e poucas linhas, `4` partições
> são de sobra e mantêm o lake com **poucos arquivos, maiores e saudáveis**. É um ajuste específico
> deste volume — em produção, você dimensiona conforme o throughput real (e às vezes adiciona um
> passo de *compaction*).

---

## 11. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `FileNotFoundException ... .ivy2/cache/resolved-...xml` | Ivy sem pasta gravável | Use `--conf spark.jars.ivy=/tmp/.ivy2` |
| Producer: `Connection refused` a `localhost:29092` | Kafka não está no ar / porta errada | Suba o Tutorial 1; publique em `29092` (não `9092`) |
| Consumer não lê nada | `startingOffsets=latest` e producer parado | Deixe o producer rodando; ou use `earliest` |
| Nada aparece no S3 | Nenhuma janela fechou ainda | Aguarde > 40s com o producer ativo (janela 30s + watermark 10s) |
| `... CRC64NVME` / erro de checksum ao gravar | (não deve ocorrer com hadoop-aws) | O S3A usa o SDK v1, sem CRC64NVME; confira o endpoint/credenciais |
| Centenas de arquivos minúsculos | Faltou reduzir shuffle partitions | `.config("spark.sql.shuffle.partitions","4")` no builder |
| `No FileSystem for scheme "s3a"` | Faltou `hadoop-aws` no `--packages` | Inclua `org.apache.hadoop:hadoop-aws:3.3.4` |

> **Como diagnosticar em camadas:** o pipeline tem 4 elos (producer → Kafka → Spark → S3). Isole
> cada um. (1) O producer imprime `N eventos enviados`? Então ele está vivo. (2) O
> `kafka-console-consumer.sh` (Seção 6) mostra JSONs? Então Kafka está recebendo. (3) O terminal do
> `spark-submit` mostra progresso de batches sem exceção? Então o Spark lê e processa. (4) O
> `s3 ls` (Seção 9) mostra `part-*.parquet`? Então o sink escreveu. O primeiro elo que falhar
> aponta onde investigar — em vez de olhar o pipeline inteiro de uma vez.

> **Leitura de logs do Spark:** com `setLogLevel("WARN")` você não verá o `INFO` de cada batch, mas
> **verá** stack traces de exceções. Erros de S3A (endpoint, credencial, `s3a` scheme) e de parse
> de JSON aparecem aí. Se o job "roda mas não escreve", quase sempre é janela que ainda não fechou
> (item 4 da tabela), não erro.

---

**Pronto!** Você fez streaming de verdade com Kafka + Spark: janelas de event-time, watermark e
gravação incremental em Parquet. Compare agora com o **Tutorial 4 (Kafka + Flink SQL)**, que
produz **o mesmo resultado** usando só SQL.

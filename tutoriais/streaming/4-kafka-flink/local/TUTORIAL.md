# Tutorial 4 (Local): Streaming com Kafka + Flink SQL (janela de 30s)

> Versão **longa e explicativa**. Você vai publicar um fluxo contínuo de **eventos de venda** em
> um tópico **Kafka** (com o **mesmo** producer Python do Tutorial 3) e consumi-lo com o
> **Flink SQL Client** — **SQL puro**, sem escrever uma linha de Java/Python de aplicação.
> Vamos agregar em **janelas tumbling de 30 segundos por categoria** (event-time + watermark) e
> gravar o resultado em **Parquet** no data lake S3 (MiniStack).
>
> O objetivo pedagógico é **comparar**: este tutorial produz **exatamente o mesmo contrato de
> saída** do Tutorial 3 (Kafka + Spark), mas com **Flink** e **só SQL**. Rode os dois e olhe o
> resultado lado a lado.
>
> **Pré-requisito**: ter feito o `1-infraestrutura/local` e estar com os containers rodando
> (`streaming_kafka`, `streaming_flink_jobmanager`, `streaming_flink_taskmanager`,
> `streaming_ministack`), com o tópico `vendas` criado. Só os comandos? Veja `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Objetivo técnico e lógico](#1-objetivo-técnico-e-lógico)
2. [Decisões de projeto (e por quê)](#2-decisões-de-projeto-e-por-quê)
3. [O fluxo deste tutorial](#3-o-fluxo-deste-tutorial)
4. [Preparando: tópico e ambiente Python](#4-preparando-tópico-e-ambiente-python)
5. [O producer (Python → Kafka)](#5-o-producer-python--kafka)
6. [O consumer em Flink SQL (explicado parte a parte)](#6-o-consumer-em-flink-sql-explicado-parte-a-parte)
7. [Rodando o job (SQL Client)](#7-rodando-o-job-sql-client)
8. [Verificando o resultado no S3 (mesmo contrato do Spark)](#8-verificando-o-resultado-no-s3-mesmo-contrato-do-spark)
9. [Parando e cancelando o job](#9-parando-e-cancelando-o-job)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Objetivo técnico e lógico

Aqui o transporte é **o mesmo** do Tutorial 3 — um **tópico Kafka** — mas o processador é o
**Apache Flink**, operado por **SQL puro** através do **SQL Client**. O objetivo é **agregar em
tempo real**: a cada 30 segundos de *tempo do evento*, calcular por **categoria** quantos
eventos houve, o faturamento e a quantidade de itens — e materializar isso em **Parquet** no
lake.

A diferença para o Tutorial 3 é **como** você expressa esse processamento:

- No **Spark** (Tut. 3) você escreveu **PySpark** (`readStream`, `withWatermark`, `groupBy`,
  `writeStream`).
- No **Flink** (Tut. 4) você declara **tabelas com SQL** (`CREATE TABLE ... WITH (...)`) e roda
  uma consulta `INSERT INTO ... SELECT ...`. O conector Kafka vira uma *tabela source*, o
  destino S3 vira uma *tabela sink*, e o `INSERT` dispara um **job de streaming contínuo** no
  cluster Flink.

O motor Flink processa **evento a evento** (stream nativo), enquanto o Spark processa em
micro-batches — mas, com **event-time + watermark** dos dois lados, o **resultado é idêntico**.
É exatamente esse ponto que torna a comparação interessante: **mesma pergunta de negócio, duas
engines, mesma resposta**.

> **Fundamento — streaming nativo × micro-batch.** O Spark Structured Streaming executa em
> **micro-batches**: ele acumula os eventos que chegaram em um pequeno intervalo, processa esse
> lote como se fosse um "mini job batch", grava, e repete. O Flink é um motor de **streaming
> nativo** (*record-at-a-time*): cada evento é processado assim que entra no operador, sem esperar
> formar um lote. Na prática isso dá ao Flink latência potencialmente **menor** (não há a espera do
> próximo micro-batch) e um modelo de estado contínuo, atualizado registro a registro.
>
> **Por que importa — mesmo resultado.** Apesar de os *modelos de execução* serem diferentes, os
> dois usam **event-time + watermark** para decidir *quando uma janela está completa*. Como a
> pergunta de negócio ("some por categoria a cada 30s de **tempo do evento**") depende de *quando a
> venda ocorreu* — e não de *quando* a engine processou —, o **resultado converge**: as mesmas
> janelas, as mesmas somas. A engine muda a *forma de executar*, não a *semântica do resultado*.

| | Micro-batch (Spark) | Streaming nativo (Flink) |
|---|---|---|
| Unidade de processamento | um lote a cada intervalo | um evento por vez |
| Latência típica | do tamanho do micro-batch | por evento (tende a ser menor) |
| Estado das janelas | recalculado por batch | contínuo, atualizado a cada evento |
| Quando a janela fecha | pelo **watermark** | pelo **watermark** |
| Resultado da nossa agregação | **idêntico** | **idêntico** |

---

## 2. Decisões de projeto (e por quê)

| Decisão | O que escolhemos | Por quê |
|---|---|---|
| **SQL puro (sem código)** | Flink **SQL Client**, `CREATE TABLE` + `INSERT INTO ... SELECT` | O mesmo pipeline do Tutorial 3, mas declarado como SQL. Menos código, mais legível — e ótimo para comparar paradigmas (PySpark × SQL). |
| **Janela por event-time** | `TUMBLE(TABLE ..., DESCRIPTOR(data_venda), INTERVAL '30' SECOND)` | Agrupa por *quando a venda ocorreu* (`data_venda`), não por quando o Flink processou. Resultado estável e reproduzível — igual ao Spark. |
| **Watermark de 10s** | `WATERMARK FOR data_venda AS data_venda - INTERVAL '10' SECOND` | Diz ao Flink "posso esperar até 10s por eventos atrasados". Quando o watermark ultrapassa o fim da janela, ela **fecha** e é emitida. É o que permite escrever no arquivo uma única vez. |
| **Checkpointing obrigatório** | `SET 'execution.checkpointing.interval' = '10 s'` | **Ponto crítico**: o sink `filesystem` só **commita** os Parquet quando há um **checkpoint**. Sem isso os arquivos ficam eternamente `.inprogress` e **nada** aparece no S3. |
| **`flink-shaded-hadoop-2-uber` no classpath** | JAR já incluído na imagem (Tut. 1) | O escritor Parquet (`parquet-hadoop`) exige a classe `org.apache.hadoop.conf.Configuration`. Sem o uber jar, o job falha com `ClassNotFoundException`. (O acesso a `s3://` é feito por outro caminho: o plugin isolado `s3-fs-hadoop`.) |
| **Config de S3 no compose** | `FLINK_PROPERTIES` dos serviços `flink-*` (endpoint, path-style, `test/test`) | O aluno **não configura S3 no SQL**: endpoint `http://ministack:4566`, path-style e credenciais dummy já vêm prontos do Tutorial 1. O SQL só diz `'path' = 's3://datalake/flink/'`. |
| **`parallelism.default = 1`** | Uma tarefa por operador | Ambiente single-node e didático; evita centenas de arquivos minúsculos e deixa a Web UI fácil de ler. |
| **Contrato idêntico ao Spark** | `categoria, window_start, window_end, qtd_eventos, faturamento, qtd_itens` | De propósito: você compara a saída deste tutorial com a do Tutorial 3 e confirma que são **a mesma coisa**. |

Duas dessas decisões merecem um aprofundamento, porque são as que mais quebram o pipeline quando
faltam — o **checkpointing** (que discutimos em detalhe na §6.1) e as **dependências de classpath**
herdadas do Tutorial 1:

> **Teoria — por que o Parquet arrasta o Hadoop.** O formato Parquet, no Flink, é escrito pela
> biblioteca `parquet-hadoop`, historicamente construída sobre as APIs do Hadoop
> (`org.apache.hadoop.conf.Configuration` e afins). Mesmo **sem** um cluster Hadoop, essas classes
> precisam estar no classpath do TaskManager para o *encoder* Parquet inicializar. É por isso que a
> imagem do Tutorial 1 traz o `flink-shaded-hadoop-2-uber` em `/opt/flink/lib`. Sem ele, o job cai
> com `ClassNotFoundException: org.apache.hadoop.conf.Configuration` (veja o Troubleshooting, §10).
>
> **Por que importa — o `commons-cli` foi removido do uber jar.** O próprio comando `flink` (usado
> em `flink list` / `flink cancel`, §9) depende da biblioteca `commons-cli` para interpretar seus
> argumentos. Se o uber jar do Hadoop **reexportar** uma versão conflitante de `commons-cli`, o CLI
> do Flink quebra na hora do *parse* — e você não consegue nem **cancelar** o job. Por isso a
> imagem do Tutorial 1 **remove** o `commons-cli` de dentro do uber jar: mantém o Parquet
> funcionando **e** o `flink cancel` operante. É um detalhe pequeno com consequência grande.

---

## 3. O fluxo deste tutorial

```
  producer.py (host)            Kafka            Flink SQL (cluster em containers)         MiniStack
  gera eventos de venda ─publica─► tópico ─consome─► CREATE TABLE vendas_kafka (source) ──►  s3://datalake/
  ~8/s em JSON            :29092   "vendas"  :9092    TUMBLE 30s por categoria (event-time)    flink/dt=.../
                                                      INSERT INTO agg_s3 (sink parquet)        part-*  +  _SUCCESS
```

- O **producer** roda na **sua máquina** e publica em `localhost:29092` (listener HOST).
- O **Flink** roda em **containers** (`streaming_flink_jobmanager` + `streaming_flink_taskmanager`)
  e alcança o Kafka via `kafka:9092` (listener INTERNAL).
- O `INSERT` cria um **job de streaming contínuo** no cluster; você o acompanha na Web UI em
  <http://localhost:8081>.

### 3.1 — A arquitetura do Flink em 1 minuto (JobManager × TaskManager)

O Flink é um sistema distribuído **mestre/trabalhador**. Dois papéis bastam para entender este
tutorial:

| Componente | Container | Papel |
|---|---|---|
| **JobManager** | `streaming_flink_jobmanager` | O "cérebro": recebe o job submetido, compila o SQL em um **grafo de dataflow**, agenda os operadores, coordena os **checkpoints** e reage a falhas. É onde vivem a **Web UI** (:8081) e o `sql-client.sh`. |
| **TaskManager** | `streaming_flink_taskmanager` | O "músculo": executa de fato os operadores (ler do Kafka, janelar/agregar, escrever Parquet). Oferece **task slots** — as unidades de paralelismo. |

> **Fundamento — task slots e paralelismo.** Cada TaskManager expõe um número fixo de **slots**. Um
> slot é uma "fatia" de recursos onde uma cadeia de tarefas roda. O `parallelism.default = 1` deste
> tutorial diz "uma instância por operador", então o job inteiro cabe em **um** slot — ideal para um
> ambiente single-node e para a Web UI ficar legível. Em produção você aumentaria o paralelismo e o
> Flink distribuiria as várias instâncias de cada operador pelos slots disponíveis, lendo várias
> partições do Kafka em paralelo.
>
> **Teoria — o job é um grafo de dataflow.** Quando você submete o `INSERT`, o Flink o compila em um
> **DAG de operadores** encadeados. No nosso caso:
>
> ```
>   [Kafka source] ──► [window aggregate: TUMBLE + GROUP BY] ──► [filesystem/parquet sink]
> ```
>
> Os dados **fluem** continuamente por esse grafo: o source lê um registro, empurra para o operador
> de janela (que mantém em **estado** as janelas abertas e suas somas parciais), e quando uma janela
> fecha o resultado desce para o sink. É esse grafo, com as taxas de registros e os checkpoints, que
> você enxerga na aba **Running Jobs** da Web UI (§7.2).

### 3.2 — Tabelas dinâmicas e consultas contínuas (a dualidade stream ↔ tabela)

O Flink SQL parece SQL "de banco", mas a semântica por baixo é diferente: você está consultando
**streams**, não tabelas estáticas.

> **Fundamento — dynamic tables.** No Flink SQL, uma tabela não é um conjunto fixo de linhas; é uma
> **tabela dinâmica** (*dynamic table*): uma tabela que **muda ao longo do tempo** conforme novos
> eventos chegam. Existe uma **dualidade stream ↔ tabela**: um stream pode ser visto como o
> *changelog* (a sequência de mudanças) de uma tabela, e uma tabela pode ser vista como o *snapshot*
> atual de um stream. Toda a expressividade do Flink SQL nasce dessa equivalência.
>
> **Teoria — `CREATE TABLE ... WITH (...)` NÃO cria dados.** Diferente de um banco, esse comando
> **não** materializa nenhuma linha. Ele apenas **declara metadados**: "existe uma tabela chamada
> `vendas_kafka`, com estas colunas, **ligada** a este sistema externo (um tópico Kafka)". É uma
> *tabela virtual* apontando para uma fonte (source) ou um destino (sink). Nada é lido do Kafka nem
> escrito no S3 até uma **consulta** rodar.
>
> **Por que importa — o `INSERT` roda para sempre.** Um `INSERT INTO sink SELECT ... FROM source`
> não é uma carga única: é uma **consulta contínua** (*continuous query*). O Flink a mantém
> "ligada" e, a cada novo evento da tabela dinâmica de origem, recomputa/emite resultados para a
> tabela dinâmica de destino. Por isso o comando **não retorna** um resultado de tabela, e sim um
> **Job ID**: você iniciou um processo que vive no cluster até ser **cancelado** (§9).

---

## 4. Preparando: tópico e ambiente Python

Com o ambiente do Tutorial 1 no ar, confira os containers do Flink e crie o tópico (se ainda não
criou). O producer é Python, então prepare também um venv.

```bash
# containers de pé? (kafka + flink jobmanager/taskmanager + ministack)
docker compose -f tutoriais/streaming/1-infraestrutura/local/docker/docker-compose.yml ps

# tópico (idempotente)
docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vendas --partitions 1 --replication-factor 1

# venv do producer (na pasta deste tutorial)
cd tutoriais/streaming/4-kafka-flink/local
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install confluent-kafka
```

> Abra a **Web UI do Flink** em <http://localhost:8081> agora e deixe aberta — é ali que o job
> aparece na aba **Running Jobs** assim que você rodar o `INSERT`.

---

## 5. O producer (Python → Kafka)

É **o mesmo producer do Tutorial 3** — se você já o criou lá, pode reutilizá-lo. Crie o arquivo
**`producer.py`** (na pasta deste tutorial) com o conteúdo abaixo. Ele gera eventos de venda
sinteticamente e publica no tópico `vendas`.

Repare no **`data_venda` sem fuso** (ISO local, ex. `2026-07-02T21:05:00.123`): é o event-time
que o Flink vai usar para janelar. **Um `Z` no fim quebraria o parse** do Flink (veja a decisão
sobre timestamp na §6) — por isso o producer **não** usa timezone.

```python
import json, random, time, uuid, sys
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = "localhost:29092"     # listener HOST do Kafka
TOPIC = "vendas"
EPS = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0   # eventos por segundo

# catálogo embutido: produto_id -> (nome, categoria, preço)
CATALOGO = {
    1: ("Notebook","Eletronicos",3500.00), 2: ("Mouse","Eletronicos",80.00),
    3: ("Cadeira","Moveis",450.00), 4: ("Mesa","Moveis",900.00),
    5: ("Camiseta","Vestuario",60.00), 6: ("Tenis","Vestuario",300.00),
    7: ("Cafe","Alimentos",25.00), 8: ("Chocolate","Alimentos",15.00),
}

def gerar_evento():
    pid = random.choice(list(CATALOGO)); _, cat, preco = CATALOGO[pid]; qtd = random.randint(1,5)
    return {"evento_id": str(uuid.uuid4()), "cliente_id": random.randint(1,20),
            "produto_id": pid, "categoria": cat, "quantidade": qtd,
            "valor_total": round(preco*qtd,2),
            # event-time em ISO SEM fuso (parseia igual no Spark e no Flink)
            "data_venda": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")}

p = Producer({"bootstrap.servers": BOOTSTRAP})
n = 0
try:
    while True:
        ev = gerar_evento()
        p.produce(TOPIC, key=ev["evento_id"], value=json.dumps(ev)); p.poll(0)
        n += 1
        if n % 10 == 0: print(f"{n} eventos enviados", flush=True)
        time.sleep(1.0/EPS)
except KeyboardInterrupt:
    pass
finally:
    p.flush(5); print(f"total: {n}")
```

> **O producer, linha a linha.** Ele é deliberadamente pequeno, mas cada parte importa:
>
> - `BOOTSTRAP = "localhost:29092"` — o listener **HOST** do Kafka. O producer roda na **sua
>   máquina**, fora do Docker, então fala com o Kafka pela porta publicada `29092`. (Dentro dos
>   containers, o endereço seria `kafka:9092` — é o que o Flink usa na §6.2.)
> - `EPS = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0` — taxa de **eventos por segundo**,
>   controlada pelo argumento de linha de comando (`python producer.py 8` → 8 ev/s). O
>   `time.sleep(1.0/EPS)` no fim do laço é o que espaça os envios no tempo.
> - `CATALOGO` — um dicionário `produto_id → (nome, categoria, preço)`. Fixar o catálogo garante que
>   as **categorias** (a chave do nosso `GROUP BY`) e os **preços** sejam consistentes entre eventos.
> - `gerar_evento()` — sorteia um produto, uma quantidade (1–5) e monta o dicionário do evento com
>   `valor_total = preço × quantidade`. O `evento_id` é um UUID, usado também como **chave** da
>   mensagem Kafka.
> - `data_venda` — **o ponto sensível**:
>   `datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")` produz um
>   ISO **sem fuso** com milissegundos, como `2026-07-02T21:05:00.123`. É o event-time que o Flink
>   vai parsear em `TIMESTAMP(3)` (§6.2). O `.replace(tzinfo=None)` remove o offset de propósito.
> - `p.produce(TOPIC, key=..., value=json.dumps(ev))` seguido de `p.poll(0)` — enfileira a mensagem
>   e deixa a biblioteca `confluent-kafka` processar entregas/callbacks de forma **não bloqueante**.
>   O `p.flush(5)` no `finally` drena o buffer (até 5 s) ao encerrar, para não perder eventos.

Rode-o em um terminal e **deixe rodando** (é o fluxo contínuo):

```bash
python producer.py 8        # 8 eventos/s
```

**Resultado esperado**: linhas `10 eventos enviados`, `20 eventos enviados`, ...

> Verifique que chegam no Kafka (em outro terminal):
> ```bash
> docker exec streaming_kafka /opt/kafka/bin/kafka-console-consumer.sh \
>   --bootstrap-server localhost:9092 --topic vendas --max-messages 3
> ```

---

## 6. O consumer em Flink SQL (explicado parte a parte)

Todo o "consumer" deste tutorial é **SQL**. São **três SET** de sessão, **duas** tabelas
(`CREATE TABLE`) e **um** `INSERT INTO ... SELECT`. Você vai **colar** esses comandos no SQL
Client (§7). Abaixo, cada bloco explicado.

> **Teoria — o "consumer" é declarativo.** Note a inversão em relação ao Tutorial 3: você **não**
> escreve um laço que puxa mensagens. Você **declara** duas tabelas (uma ligada ao Kafka, outra ao
> S3) e **uma consulta** que liga as duas; o Flink cuida de ler, janelar, agregar e escrever. Os
> `SET` ajustam a **sessão**; os `CREATE TABLE` são apenas **metadados** (§3.2); o `INSERT` é o que
> de fato **vira o job** de streaming. Colar na ordem importa: a consulta referencia as tabelas, que
> por sua vez dependem da sessão configurada.

### 6.1 — Config da sessão (streaming + checkpointing)

```sql
-- Config da sessão (streaming + checkpointing p/ o filesystem sink commitar os Parquet)
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'parallelism.default' = '1';
```

- **`execution.runtime-mode = streaming`**: processamento contínuo (não batch).
- **`execution.checkpointing.interval = 10 s`** — **o SET mais importante deste tutorial**. O
  sink `filesystem` escreve os dados em arquivos `.inprogress` e só os **renomeia para
  definitivos (commita)** quando ocorre um **checkpoint**. Sem esse SET, **nada** aparece no S3:
  os arquivos ficam eternamente pendentes. Com checkpoint a cada 10s, os Parquet são commitados
  logo após cada janela fechar.
- **`parallelism.default = 1`**: uma tarefa por operador — suficiente e limpo para o ambiente
  local.

> **Fundamento — o que é um checkpoint.** Periodicamente o JobManager injeta no fluxo umas marcas
> especiais chamadas **barreiras de checkpoint** (*checkpoint barriers*). Quando uma barreira
> atravessa um operador, ele salva seu **estado** (por exemplo, as janelas abertas e suas somas
> parciais, e os offsets já lidos do Kafka) em um armazenamento durável. Um checkpoint é o retrato
> **consistente** do estado de **todos** os operadores em um mesmo ponto lógico do stream.
>
> **Por que importa — tolerância a falhas e commit do sink.** Se um TaskManager cair, o Flink
> **reinicia** o job a partir do último checkpoint e reprocessa só o que veio depois — é assim que
> ele entrega **exactly-once** (conceitualmente: cada evento conta exatamente uma vez no resultado,
> mesmo após falhas). E há um efeito colateral **essencial** aqui: o sink `filesystem` amarra o
> **commit** dos arquivos ao checkpoint. Um arquivo só sai de `.inprogress` para definitivo **no**
> checkpoint — o único momento em que o Flink tem certeza de que aqueles dados estão "firmes" e
> reproduzíveis após falha. Por isso `execution.checkpointing.interval = '10 s'` controla, na
> prática, **de quanto em quanto tempo os Parquet aparecem no S3**. Sem checkpointing, o commit
> **nunca** acontece — os dados existem só como `.inprogress`, invisíveis como resultado.

### 6.2 — SOURCE: o tópico Kafka como tabela

```sql
-- 1) SOURCE: tópico Kafka 'vendas'
CREATE TABLE vendas_kafka (
  evento_id   STRING,
  cliente_id  INT,
  produto_id  INT,
  categoria   STRING,
  quantidade  INT,
  valor_total DOUBLE,
  data_venda  TIMESTAMP(3),
  WATERMARK FOR data_venda AS data_venda - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'vendas',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-vendas',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
```

- As colunas espelham o JSON do evento. **`data_venda` vira `TIMESTAMP(3)`** (event-time).
- **`WATERMARK FOR data_venda AS data_venda - INTERVAL '10' SECOND`**: o Flink acompanha o maior
  `data_venda` visto e assume que não chegarão eventos mais antigos que `(máximo − 10s)`. Ao
  passar do fim de uma janela, ela é **finalizada** — é o equivalente exato do `withWatermark`
  do Spark.
- **`'properties.bootstrap.servers' = 'kafka:9092'`**: listener **INTERNAL** (container →
  container). O `localhost:29092` só vale para o producer, que roda no host.
- **`'scan.startup.mode' = 'latest-offset'`**: começa a ler do fim do log — por isso o producer
  precisa estar **rodando** quando você iniciar o job (senão não há o que ler).
- **`'json.timestamp-format.standard' = 'ISO-8601'`**: casa com o `data_venda` **sem fuso** que
  o producer emite (ex. `2026-07-02T21:05:00.123`). Um `Z` no fim quebraria esse parse — é o
  motivo de o producer não usar timezone.

> **Cláusula a cláusula do `WITH (...)`.** Vale reler cada opção como uma "ligação" da tabela
> virtual ao Kafka: `'connector' = 'kafka'` escolhe o conector (empacotado no
> `flink-sql-connector-kafka-*.jar` da imagem); `'topic' = 'vendas'` diz **de qual tópico** ler;
> `'properties.bootstrap.servers' = 'kafka:9092'` é o endereço **INTERNAL** (container → container),
> não o `29092` do host; `'properties.group.id' = 'flink-vendas'` nomeia o *consumer group*;
> `'scan.startup.mode' = 'latest-offset'` diz **de onde começar** a ler (só mensagens novas); e o
> par `'format' = 'json'` + `'json.timestamp-format.standard' = 'ISO-8601'` define **como
> desserializar** o corpo da mensagem.
>
> **Fundamento — event-time × processing-time.** Existem dois "relógios" possíveis: o
> *processing-time* (o relógio da máquina no instante em que o Flink vê o evento) e o *event-time*
> (o instante em que o fato ocorreu, carimbado no próprio dado — aqui, `data_venda`). Janelar por
> **event-time** torna o resultado **determinístico e reproduzível**: não importa se o evento chegou
> atrasado por rede ou rebalanceamento, ele cai na janela do **momento em que a venda aconteceu**. É
> a mesma escolha do Tutorial 3.
>
> **Teoria — o que a WATERMARK faz.** O stream é potencialmente infinito; o Flink não pode esperar
> "para sempre" por retardatários antes de fechar uma janela. A **watermark** é uma estimativa
> corrente do tipo "já vi event-time até aqui; não espero mais nada anterior a `(máximo visto −
> 10s)`". Quando a watermark **ultrapassa** o `window_end` de uma janela, o Flink a considera
> **completa**, emite o resultado e **libera o estado** daquela janela (evitando que o estado cresça
> sem limite). Os `-10s` são a sua **tolerância a atraso**: mais folga = espera mais por
> retardatários (maior latência, menos risco de perder eventos tardios); menos folga = fecha mais
> rápido. É o equivalente exato do `withWatermark("data_venda","10 seconds")` do Spark.
>
> **Por que importa — o `TIMESTAMP(3)` e o formato ISO-8601 andam juntos.** A coluna `data_venda
> TIMESTAMP(3)` é um timestamp **sem fuso** com precisão de milissegundos (o `(3)` = 3 casas de
> segundo). A opção `'json.timestamp-format.standard' = 'ISO-8601'` faz o parser JSON ler strings
> como `2026-07-02T21:05:00.123` (com o `T`) — **exatamente** o que o producer emite. Se o producer
> escrevesse com `Z` (UTC) ou `+00:00`, o valor deixaria de casar com um `TIMESTAMP` sem fuso (para
> carimbos com offset seria preciso `TIMESTAMP_LTZ`) e o parse falharia. Producer e source são, por
> isso, **um contrato só**.

### 6.3 — SINK: Parquet no S3 como tabela

```sql
-- 2) SINK: Parquet no S3 (MiniStack), particionado por dt
CREATE TABLE agg_s3 (
  categoria     STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  qtd_eventos   BIGINT,
  faturamento   DOUBLE,
  qtd_itens     BIGINT,
  dt            STRING
) PARTITIONED BY (dt) WITH (
  'connector' = 'filesystem',
  'path' = 's3://datalake/flink/',
  'format' = 'parquet',
  'sink.partition-commit.policy.kind' = 'success-file'
);
```

- **`'connector' = 'filesystem'` + `'format' = 'parquet'`**: escreve arquivos Parquet no
  caminho dado. O acesso a `s3://` usa o **plugin `s3-fs-hadoop`**, já habilitado na imagem, com
  endpoint/credenciais do MiniStack vindos das `FLINK_PROPERTIES` (você **não** configura isso
  aqui).
- **`PARTITIONED BY (dt)`**: gera a estrutura `flink/dt=YYYY-MM-DD/...`, idêntica à partição do
  Spark.
- **`'sink.partition-commit.policy.kind' = 'success-file'`**: ao fechar uma partição, grava um
  arquivo **`_SUCCESS`** (marcador de partição pronta) — junto com os `part-*`.
- As colunas são **o mesmo contrato** do Tutorial 3.

> **Fundamento — como o sink `filesystem` escreve (o ciclo `.inprogress` → commit).** O sink não
> abre um Parquet e escreve linha a linha até o fim. Ele trabalha em duas fases: enquanto recebe
> dados, acumula em arquivos **`.inprogress`** (parciais, ainda não visíveis como resultado final);
> e **no checkpoint** ele **finaliza** (fecha e renomeia) esses arquivos para os definitivos
> `part-*`. Só então eles "existem" oficialmente no lake. É por isso que **checkpoint e sink são
> inseparáveis** aqui (§6.1): sem a barreira de checkpoint, o arquivo nunca é promovido.
>
> **Teoria — partition commit e o `_SUCCESS`.** Como a tabela é `PARTITIONED BY (dt)`, o Flink trata
> cada valor de `dt` como uma **partição** (uma subpasta `dt=YYYY-MM-DD/`). Quando decide que uma
> partição está pronta, ele aplica a *partition-commit policy*: com
> `'sink.partition-commit.policy.kind' = 'success-file'`, grava um arquivo vazio **`_SUCCESS`**
> dentro de `dt=.../`. Esse marcador é uma convenção herdada do Hadoop/Hive — ferramentas a jusante
> (Spark, Trino, etc.) leem o `_SUCCESS` como "esta partição terminou de ser escrita, pode consumir
> com segurança". É o **mesmo** significado do `_SUCCESS` que o Spark grava no Tutorial 3.
>
> **Por que importa — você NÃO configura S3 no SQL.** Repare que não há `access-key`, `endpoint` nem
> `path-style` no `WITH (...)`. O acesso a `s3://` é responsabilidade do **plugin de sistema de
> arquivos** do Flink, o **`s3-fs-hadoop`**, carregado em um **classloader isolado** (para as
> dependências dele não colidirem com as do seu job). Toda a configuração — endpoint
> `http://ministack:4566`, `path-style` (obrigatório para o LocalStack/MiniStack) e as credenciais
> dummy `test/test` — já vem pronta nas `FLINK_PROPERTIES` dos serviços `flink-*` do compose do
> Tutorial 1. O SQL só precisa dizer **onde** (`'path' = 's3://datalake/flink/'`); o **como** já
> está resolvido na infraestrutura. Separar o *o quê* (SQL) do *como* (infra) é justamente o que
> deixa o SQL do aluno limpo e portável.

### 6.4 — JOB: a janela tumbling de 30s (TVF)

```sql
-- 3) JOB: janela tumbling de 30s por categoria (event-time) -> S3
INSERT INTO agg_s3
SELECT
  categoria,
  window_start,
  window_end,
  COUNT(*)          AS qtd_eventos,
  SUM(valor_total)  AS faturamento,
  SUM(quantidade)   AS qtd_itens,
  DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt
FROM TABLE(
  TUMBLE(TABLE vendas_kafka, DESCRIPTOR(data_venda), INTERVAL '30' SECOND)
)
GROUP BY categoria, window_start, window_end;
```

- **`TUMBLE(TABLE vendas_kafka, DESCRIPTOR(data_venda), INTERVAL '30' SECOND)`** é uma **Windowing
  TVF** (Table-Valued Function): quebra o stream em janelas **fixas e não sobrepostas** de 30s
  sobre o event-time `data_venda`. Ela injeta as colunas `window_start` e `window_end`.
- O `GROUP BY categoria, window_start, window_end` agrega **uma linha por categoria por janela**.
- `COUNT(*)`, `SUM(valor_total)`, `SUM(quantidade)` são exatamente `qtd_eventos`, `faturamento`,
  `qtd_itens` — os mesmos do Spark.
- `DATE_FORMAT(window_start, 'yyyy-MM-dd')` deriva a coluna de partição `dt`.
- Como este é um `INSERT` em uma tabela de **streaming source**, ele **não termina**: vira um
  **job contínuo** no cluster (você recebe um `Job ID`).

> **Fundamento — Windowing TVF (Table-Valued Function).** A partir do Flink 1.13 as janelas em SQL
> são expressas por **funções que recebem uma tabela e devolvem uma tabela**. `TUMBLE(TABLE
> vendas_kafka, DESCRIPTOR(data_venda), INTERVAL '30' SECOND)` recebe a tabela `vendas_kafka`, diz
> **qual coluna é o event-time** (`DESCRIPTOR(data_venda)`) e **o tamanho da janela** (30s). O que
> ela devolve é a **mesma** tabela de entrada **acrescida** das colunas `window_start`,
> `window_end` e `window_time`. Por isso, no `SELECT`, `window_start` e `window_end` "existem" sem
> você tê-las declarado — foi a TVF que as injetou.
>
> **Teoria — `TUMBLE` = janelas fixas e não sobrepostas.** *Tumbling* ("tombando"): as janelas se
> sucedem sem buraco nem sobreposição — `[00:00, 00:30)`, `[00:30, 01:00)`, ... Cada evento cai em
> **exatamente uma** janela. O `GROUP BY categoria, window_start, window_end` então agrega **uma
> linha por categoria por janela** — a granularidade do nosso contrato de saída. Note que agrupar
> por `window_start`/`window_end` (e não por `data_venda`) é o que dá **uma** linha por bucket, e
> não uma por evento.
>
> **Por que importa — a equivalência com o Spark.** No Tutorial 3 você escreveu
> `groupBy(window("data_venda","30 seconds"), "categoria")`, e o Spark devolvia uma struct
> `window.start`/`window.end`. Aqui a TVF `TUMBLE` faz o papel do `window()`, e as colunas
> `window_start`/`window_end` fazem o papel de `window.start`/`window.end`. Sintaxe diferente,
> **mesma** operação de janelamento por event-time — e, por consequência, **a mesma saída**.

---

## 7. Rodando o job (SQL Client)

Com o **producer rodando** (Terminal 1), abra o **SQL Client** em outro terminal (Terminal 2):

```bash
docker exec -it streaming_flink_jobmanager /opt/flink/bin/sql-client.sh
```

Você entra no prompt `Flink SQL>`. **Cole os comandos da §6 na ordem**:

1. Os três **`SET`** (cada um retorna `[INFO] Execute statement succeeded.`).
2. O **`CREATE TABLE vendas_kafka`** e o **`CREATE TABLE agg_s3`** (cada um retorna
   `[INFO] Execute statement succeeded.`).
3. O **`INSERT INTO agg_s3 SELECT ...`** — este retorna algo como:

   ```
   [INFO] Submitting SQL update statement to the cluster...
   [INFO] SQL update statement has been successfully submitted to the cluster:
   Job ID: 3a7f1c9e0b2d4f6a8c1e2d3b4a5f6e7d
   ```

   O `INSERT` **dispara um job de streaming contínuo**. O prompt volta livre — o job segue
   rodando **no cluster**, independente da sua sessão SQL.

### 7.1 — Alternativa não-interativa (`-f`)

Em vez de colar, você pode **salvar o SQL em um arquivo** e rodar de uma vez. A pasta
`1-infraestrutura/local/docker/work/` já é montada como **`/work`** dentro dos containers Flink.
Salve os comandos da §6 (os 3 `SET` + os 2 `CREATE TABLE` + o `INSERT`) em
`1-infraestrutura/local/docker/work/flink_vendas.sql` e rode:

```bash
docker exec streaming_flink_jobmanager /opt/flink/bin/sql-client.sh -f /work/flink_vendas.sql
```

> Regra do curso: o SQL você **digita/cola** (ou salva no `flink_vendas.sql`). Não fornecemos um
> arquivo pronto — você aprende escrevendo.

### 7.2 — Acompanhando na Web UI

Abra <http://localhost:8081> → aba **Running Jobs**. Você verá o job `insert-into_...` com os
operadores (Kafka source → window aggregate → filesystem sink), a taxa de registros e os
**checkpoints** (a cada 10s). Deixe rodando **~60–90s** para 2–3 janelas fecharem.

---

## 8. Verificando o resultado no S3 (mesmo contrato do Spark)

Em outro terminal, após ~60–90s (tempo de algumas janelas de 30s fecharem + o watermark de 10s):

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/flink/ --recursive
```

**Resultado esperado**: arquivos `flink/dt=YYYY-MM-DD/part-*` e um `flink/dt=YYYY-MM-DD/_SUCCESS`
(o marcador de partition commit).

> **Nada aparece?** Quase sempre é o **checkpointing** (§6.1). Sem
> `SET 'execution.checkpointing.interval' = '10 s'`, o sink nunca commita e os arquivos ficam
> `.inprogress`. Confira também que o **producer está rodando** (`latest-offset` não lê o
> passado).

Baixe um `part` e leia com **pyarrow** (instale no venv do producer se preciso):

```bash
pip install pyarrow
aws --endpoint-url http://localhost:4566 s3 cp \
  "$(aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/flink/dt=$(date -u +%F)/ | awk '{print $4}' | grep '^part' | head -1 | sed 's#^#s3://datalake/flink/dt='$(date -u +%F)'/#')" /tmp/janela.parquet
python -c "import pyarrow.parquet as pq; t=pq.read_table('/tmp/janela.parquet'); print(t.column_names); print(t.to_pylist()[:2])"
```

**Resultado real obtido** (uma linha por categoria por janela de 30s):

```python
['categoria','window_start','window_end','qtd_eventos','faturamento','qtd_itens']
[{'categoria':'Eletronicos','window_start':'2026-07-02 21:05:00','window_end':'2026-07-02 21:05:30',
  'qtd_eventos':80,'faturamento':405100.0,'qtd_itens':233}]
```

Note que **`window_end - window_start = 30s`** exatos. E, principalmente: **é o mesmo contrato
do Tutorial 3 (Spark)**. 🎯

### 8.1 — Spark × Flink lado a lado

| | **Tutorial 3 — Spark** | **Tutorial 4 — Flink** |
|---|---|---|
| Como você escreve | **PySpark** (`readStream`/`writeStream`) | **SQL puro** (`CREATE TABLE` + `INSERT`) |
| Modelo de execução | micro-batches | stream nativo (evento a evento) |
| Janela de 30s | `window(data_venda, "30 seconds")` | `TUMBLE(..., DESCRIPTOR(data_venda), INTERVAL '30' SECOND)` |
| Watermark | `withWatermark("data_venda","10 seconds")` | `WATERMARK FOR data_venda AS data_venda - INTERVAL '10' SECOND` |
| Commit no S3 | ao fechar a janela (append) | **no checkpoint** (`checkpointing.interval`) |
| Saída | `s3a://datalake/spark/dt=.../part-*.parquet` | `s3://datalake/flink/dt=.../part-*` + `_SUCCESS` |
| **Colunas** | `categoria, window_start, window_end, qtd_eventos, faturamento, qtd_itens` | **as mesmas** |

Duas engines, duas sintaxes — **um único resultado**.

---

## 9. Parando e cancelando o job

O job de streaming **não para sozinho** ao fechar o SQL Client — ele vive no cluster. Para
cancelá-lo, use um dos caminhos:

- **Web UI**: <http://localhost:8081> → **Running Jobs** → o job → botão **Cancel Job**.
- **Linha de comando** (pega o `Job ID` e cancela):

  ```bash
  docker exec streaming_flink_jobmanager /opt/flink/bin/flink list         # lista os jobs (pegue o Job ID)
  docker exec streaming_flink_jobmanager /opt/flink/bin/flink cancel <JobID>
  ```

> **Fundamento — por que um job de streaming não termina.** Um job batch tem um **fim natural**:
> processou todo o input, terminou. Um job de streaming lê de uma fonte **potencialmente infinita**
> (um tópico Kafka nunca "acaba"), então, por definição, ele **não conclui** — fica esperando o
> próximo evento para sempre. Fechar o `sql-client.sh` **não** derruba o job: o cliente apenas
> **submeteu** o job ao JobManager e foi embora; o job continua vivo no cluster. A única forma de
> pará-lo é **cancelar** explicitamente.
>
> **Teoria — `flink list` e `flink cancel`.** O `flink list` pergunta ao JobManager quais jobs estão
> RUNNING e imprime o **Job ID** de cada um. O `flink cancel <JobID>` manda o JobManager **parar**
> aquele job: ele interrompe os operadores, libera os task slots e encerra o dataflow. A Web UI
> (**Running Jobs → Cancel Job**) dispara exatamente a mesma operação por trás. Depois de cancelar,
> o job passa de **RUNNING** para **CANCELED** na UI — e deixa de commitar novos Parquet.

Depois, `Ctrl+C` no terminal do **producer**. Os containers do Tutorial 1 podem continuar de pé
(pare-os só quando terminar todos os tutoriais).

---

## 10. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ClassNotFoundException: org.apache.hadoop.conf.Configuration` | Imagem do Flink sem o `flink-shaded-hadoop-2-uber` (o `parquet-hadoop` precisa dele) | Reconstrua a imagem do Tutorial 1 (`docker compose up -d --build`); o JAR deve estar em `/opt/flink/lib` |
| **Nada no S3** / arquivos `.inprogress` para sempre | Faltou o checkpointing — o sink `filesystem` só commita no checkpoint | Rode `SET 'execution.checkpointing.interval' = '10 s'` **antes** do `INSERT` |
| Job roda mas **não lê nada** | `scan.startup.mode = latest-offset` e o producer parado | Deixe o `producer.py` rodando; ou publique eventos antes/durante o job |
| `Could not find any factory for identifier 'kafka'` | Conector Kafka ausente do classpath | Confira `flink-sql-connector-kafka-*.jar` em `/opt/flink/lib` (rebuild do Tut. 1) |
| Erro de **parse de timestamp** em `data_venda` | `data_venda` com `Z`/fuso não casa com ISO-8601 sem fuso | Producer deve emitir ISO **sem** timezone; a source usa `'json.timestamp-format.standard' = 'ISO-8601'` |
| Producer: `Connection refused` em `localhost:29092` | Kafka fora do ar / porta errada | Suba o Tutorial 1; publique em `29092` (host), não `9092` |
| Nenhuma janela fechou ainda | Pouco tempo com o producer ativo | Aguarde > 40s (janela 30s + watermark 10s) com o producer rodando |
| Job aparece como **FAILED** na UI | Veja o *stack trace* na aba **Exceptions** do job | Costuma cair em um dos itens acima (JAR, checkpoint, timestamp) |

---

**Pronto!** Você fez streaming de verdade com **Kafka + Flink SQL**: janela de event-time,
watermark, checkpoint para commitar e gravação em Parquet — **só com SQL**. E o mais importante:
comparou com o **Tutorial 3 (Spark)** e confirmou que **a mesma pergunta de negócio, em duas
engines, dá o mesmo resultado**.

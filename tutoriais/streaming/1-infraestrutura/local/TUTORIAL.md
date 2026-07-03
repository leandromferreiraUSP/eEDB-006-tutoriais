# Tutorial 1 (Local): Infraestrutura de Streaming com Docker

> Versão **longa e explicativa**. Aqui você monta, **na sua própria máquina**, todo o ambiente
> que os Tutoriais 2 (Filas), 3 (Kafka + Spark) e 4 (Kafka + Flink) vão usar: um broker
> **Kafka**, um broker de filas **RabbitMQ**, os processadores **Spark** e **Flink**, e um
> **data lake S3** local emulado pelo **MiniStack**. Tudo em containers Docker.
>
> Quer só os comandos? Veja o `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Objetivo técnico e lógico](#1-objetivo-técnico-e-lógico)
2. [Decisões de projeto (e por quê)](#2-decisões-de-projeto-e-por-quê)
3. [O que vamos construir](#3-o-que-vamos-construir)
4. [Conceitos: fila × tópico, Spark × Flink, MiniStack](#4-conceitos-fila--tópico-spark--flink-ministack)
5. [Pré-requisitos por sistema operacional](#5-pré-requisitos-por-sistema-operacional)
6. [Os arquivos de infraestrutura](#6-os-arquivos-de-infraestrutura)
7. [Subindo o ambiente](#7-subindo-o-ambiente)
8. [Validando o ambiente](#8-validando-o-ambiente)
9. [Explorando cada serviço](#9-explorando-cada-serviço)
10. [Parando e limpando](#10-parando-e-limpando)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Objetivo técnico e lógico

**Processamento em streaming** é processar dados **em movimento**: em vez de acumular tudo e
rodar um job em lote (batch) de tempos em tempos, um fluxo **contínuo** de eventos é processado
**à medida que chega**. É o que está por trás de dashboards em tempo real, detecção de fraude,
alertas, métricas ao vivo.

> **Teoria: batch × streaming (os dois modos de processar dados).** No modelo **batch** (lote),
> os dados são acumulados em repouso (um arquivo, uma tabela, um dia inteiro de vendas) e um job
> roda **do início ao fim** sobre um conjunto **finito e delimitado** (*bounded*). Ele começa,
> processa tudo, produz um resultado e termina. No modelo **streaming**, os dados são um fluxo
> **sem fim conhecido** (*unbounded*): novos eventos podem chegar a qualquer instante, para
> sempre. O job **nunca "termina"** — ele fica de pé, consumindo e emitindo resultados
> continuamente. Essa diferença (conjunto finito × fluxo infinito) é a raiz de quase tudo que
> vem depois: janelas de tempo, marcas d'água (*watermark*), estado, tolerância a falha.

> **Fundamento: latência × throughput.** Duas métricas guiam qualquer sistema de dados em
> movimento. **Latência** é *quanto tempo* um evento leva desde que acontece até virar resultado
> (medida em ms/s); **throughput** (vazão) é *quantos* eventos por segundo o sistema consegue
> processar. Batch otimiza throughput (processa milhões de linhas de uma vez, mas o resultado só
> sai horas depois); streaming otimiza latência (resultado em segundos, ao custo de uma
> arquitetura mais complexa). Não é "um é melhor que o outro": é uma **troca**. Neste curso usamos
> **micro-lote** (Spark e o consumer de filas) e **janela de 30s** justamente para equilibrar os
> dois num cenário didático.

> **Por que importa:** entender que um stream é *unbounded* explica por que não dá para
> simplesmente fazer `GROUP BY categoria` sobre "todos os dados" — não existe "todos", os dados
> nunca acabam. Por isso o streaming agrupa eventos em **janelas** (ex.: "vendas por categoria a
> cada 30 segundos"): recortes **finitos** de um fluxo **infinito**.

O objetivo **lógico** deste conjunto de tutoriais é mostrar os **dois grandes paradigmas** de
transporte e processamento de streaming, sempre com o **mesmo dado** (um evento de venda de
e-commerce), para você comparar:

| Paradigma | Transporte | Exemplo neste curso | Agrupamento |
|---|---|---|---|
| **Filas** (message queue) | fila ponto-a-ponto | RabbitMQ (local), SQS (AWS) | **micro-lote** |
| **Tópicos** (log distribuído) | log particionado | Kafka + Spark, Kafka + Flink | **janela de 30s** |

O objetivo **técnico** deste Tutorial 1 é subir, de forma reprodutível e idêntica em qualquer
máquina, **toda a plataforma** que os próximos tutoriais consomem — sem instalar Kafka, Spark
ou Flink "no seu sistema", e sem depender da nuvem. Um único `docker compose up` entrega:

- **Kafka** (broker de tópicos) — usado pelos Tutoriais 3 e 4.
- **RabbitMQ** (broker de filas) — usado pelo Tutorial 2 (local).
- **Spark** e **Flink** (os dois processadores de stream) — Tutoriais 3 e 4.
- **MiniStack** (S3 local) — o data lake de **destino** de todos.

> **Casos de uso típicos de streaming** (por que alguém montaria tudo isto):

| Caso de uso | Por que precisa de streaming | Latência desejada |
|---|---|---|
| **Dashboards em tempo real** | métricas de negócio que envelhecem em segundos (pedidos/min, receita ao vivo) | segundos |
| **Detecção de fraude** | bloquear a transação **antes** de ela concluir | milissegundos |
| **Alertas e monitoramento** | disparar quando um limiar é cruzado (erro, temperatura, estoque) | segundos |
| **Métricas e telemetria** | agregações contínuas (p95 de latência, contagem por região) | segundos |
| **ETL "sempre ligado"** | mover/transformar dados assim que nascem, sem esperar o "lote da noite" | segundos a minutos |

> Em todos esses casos, o dado **perde valor com o tempo**: uma fraude detectada amanhã já custou
> o dinheiro; um alerta de disco cheio *depois* da queda é inútil. O streaming existe para agir
> **enquanto o dado ainda vale**.

---

## 2. Decisões de projeto (e por quê)

Estas decisões valem para **todos** os tutoriais de streaming. Entendê-las agora evita
confusão depois.

| Decisão | O que escolhemos | Por quê |
|---|---|---|
| **Um único evento** | Evento de venda em JSON (`evento_id`, `cliente_id`, `produto_id`, `categoria`, `quantidade`, `valor_total`, `data_venda`) | O foco é o *streaming*, não a modelagem. O mesmo evento trafega em fila e em tópico — só muda o processamento. |
| **`categoria` embutida no evento** | Vem dentro do JSON, não em uma tabela à parte | Assim a agregação por categoria (janela) é um `GROUP BY` direto, sem `join` com um catálogo. |
| **`data_venda` = event-time** | A hora está **no dado**, não é a hora de chegada | Streaming "de verdade" agrupa por *quando o evento aconteceu*, tolerando atraso de rede. Os Tutoriais 3 e 4 usam **event-time + watermark**. |
| **Sem `Z`/timezone no timestamp** | `2026-07-02T20:51:59.337` (ISO sem fuso) | Esse formato é parseado sem atrito **tanto no Spark** (`to_timestamp`) **quanto no Flink** (`TIMESTAMP(3)` ISO-8601). Um `Z` no fim quebraria o parse no Flink. |
| **Producers/consumers você cria** | Só a infra (compose, Dockerfile, Terraform) e scripts de validação vêm prontos | Você aprende digitando o código, não colando um `.py` pronto. |
| **MiniStack como S3 local** | Emulador S3 na porta 4566 | Paridade local ↔ AWS: o mesmo código muda só o `endpoint`/credenciais. De graça e offline. |
| **Kafka em modo KRaft** | Sem Zookeeper | Kafka moderno (3.x) dispensa o Zookeeper; um container só. |
| **Spark e Flink em containers** | Não instalamos na sua máquina | Spark 3.5 exige Java 8/11/17 (não o Java do seu SO). No container, a versão certa já vem pronta. |

Algumas dessas decisões merecem aprofundamento, porque reaparecem — e às vezes mordem — nos
Tutoriais 3 e 4:

> **Teoria: por que `data_venda` é o "event-time".** Todo evento carrega (pelo menos) dois
> tempos: o **event-time** — quando o fato aconteceu no mundo real (a venda foi feita) — e o
> **processing-time** — quando o sistema *processou* aquele evento (quando o Spark/Flink o leu).
> Eles quase nunca coincidem: a rede congestiona, um container reinicia, um lote atrasa. Se você
> agrupar "vendas por 30s" pelo *processing-time*, o resultado muda conforme a máquina esteja
> rápida ou lenta — não é reprodutível. Agrupando pelo **event-time** (`data_venda`, que está
> *dentro* do dado), a janela "20:30:00–20:30:30" sempre contém os mesmos eventos, mesmo que um
> deles chegue 5 segundos atrasado. Os Tutoriais 3 e 4 usam **event-time + watermark** (a marca
> d'água diz ao motor "até quando esperar por atrasados antes de fechar a janela").

> **Por que o timestamp vai sem `Z`/fuso.** Um `Z` no fim de um ISO-8601 significa UTC ("Zulu"). O
> parser de `TIMESTAMP(3)` do Flink SQL espera um timestamp **sem** fuso; um `Z` o transformaria em
> `TIMESTAMP_LTZ` (com fuso) e quebraria o casamento de tipos do conector. O Spark
> (`to_timestamp`) aceita as duas formas, mas para o **mesmo dado** rodar idêntico nos dois
> motores padronizamos o formato mais simples: `2026-07-02T20:51:59.337`.

> **Por que Parquet no destino.** Todos os pipelines gravam o resultado em **Parquet** (não
> JSON/CSV) — o formato **colunar** padrão dos data lakes. A seção 4 detalha; por ora guarde: ele
> comprime bem, guarda o **schema junto** e é lido de forma eficiente por Spark, Flink, Athena,
> DuckDB e afins.

---

## 3. O que vamos construir

```
        ┌──────────────────────────── sua máquina (Docker) ───────────────────────────┐
        │                                                                              │
        │   TRANSPORTE                        PROCESSADORES              DESTINO        │
        │  ┌───────────────┐                 ┌──────────────┐         ┌──────────────┐ │
 host   │  │  Kafka (KRaft)│ ── tópico ────► │  Spark 3.5   │ ──┐     │              │ │
 (você) │  │   :9092/:29092│                 │  (Tut. 3)    │   │     │  MiniStack   │ │
  ─────► │  │               │ ── tópico ────► │  Flink 1.20  │ ──┼───► │  S3  :4566   │ │
producer│  └───────────────┘                 │  (Tut. 4)    │   │     │  datalake/   │ │
        │  ┌───────────────┐                 └──────────────┘   │     │              │ │
        │  │  RabbitMQ     │ ── fila ──────► (consumer Python) ─┘     └──────────────┘ │
        │  │  :5672/:15672 │                    (Tut. 2)                               │
        │  └───────────────┘                                                           │
        └──────────────────────────────────────────────────────────────────────────────┘
```

- O **producer** roda na **sua máquina** (host) e publica no Kafka via `localhost:29092`
  (listener HOST) ou no RabbitMQ via `localhost:5672`.
- Os **consumers** Spark/Flink rodam em **containers** e alcançam o Kafka via `kafka:9092`
  (listener INTERNAL — nome de serviço na rede do Docker).
- Todos gravam Parquet no **MiniStack** (`s3://datalake/...`).

> **Fundamento: por que a seta do producer entra "de fora".** O **producer** (que você vai
> escrever em Python nos próximos tutoriais) roda no **host** — o seu sistema operacional, fora do
> Docker. Para ele, o Kafka é `localhost:29092`. Já o **Spark** e o **Flink** rodam *dentro* da
> rede do Docker; para eles, o mesmo Kafka é `kafka:9092`. É **o mesmo broker**, com **dois
> endereços** — essa dualidade (host × container) é o motivo dos dois *listeners*, detalhado na
> seção 6.1. Guarde a ideia: metade dos erros de "não conecta no Kafka" vem de usar o endereço do
> lado errado.

---

## 4. Conceitos: fila × tópico, Spark × Flink, MiniStack

### Fila (RabbitMQ / SQS) × Tópico (Kafka)

| | **Fila** (RabbitMQ, SQS) | **Tópico / log** (Kafka) |
|---|---|---|
| Modelo | uma mensagem é **consumida e some** | um **log** append-only; mensagens ficam e podem ser relidas |
| Consumo | ponto-a-ponto (cada msg vai p/ 1 consumer) | vários consumers/grupos leem o mesmo log |
| Ordenação | por fila | por **partição** |
| Uso típico | tarefas, trabalho unitário, desacoplamento | streams de eventos, reprocessamento, alta vazão |
| Neste curso | **Tutorial 2** (micro-lote) | **Tutoriais 3 e 4** (janela) |

> **Teoria: ponto-a-ponto × publish/subscribe.** Uma **fila** implementa o modelo
> *ponto-a-ponto*: cada mensagem é entregue a **um** consumidor e, depois do *ack* (confirmação),
> **some** da fila. É ideal para "trabalho a ser feito uma vez" (processar um pagamento, enviar um
> e-mail). Um **tópico/log** implementa *publish/subscribe*: o produtor *publica* e **N** grupos de
> consumidores independentes leem **a mesma** sequência de eventos, cada um no seu ritmo. É ideal
> para "um evento que interessa a várias equipes" (a venda alimenta ao mesmo tempo o faturamento,
> o antifraude e o dashboard).

> **Fundamento: log append-only e o "replay".** A diferença técnica central é *o que acontece
> depois de ler*. Na fila, ler + ack **destrói** a mensagem. No Kafka, ler **não apaga nada**: o
> tópico é um **log append-only** (só se acrescenta ao fim) que **retém** os eventos por um tempo
> configurável (**retenção** — por tempo ou por tamanho), independentemente de quem já leu. Cada
> consumidor guarda apenas a sua **posição de leitura** (o *offset*). Isso habilita o **replay**:
> reprocessar do zero — corrigir um bug no consumer, subir um novo consumidor que precisa do
> histórico — é só "voltar o offset". Numa fila isso é impossível: o que foi consumido já não
> existe. Guarde o par: **fila = destrutiva; log = relível**.

### Kafka por dentro: broker, tópico, partição, offset

O Kafka é uma **plataforma de log distribuído**. Vale fixar o vocabulário agora, porque ele
reaparece em todos os comandos dos Tutoriais 3 e 4:

| Termo | O que é |
|---|---|
| **Broker** | o processo/servidor Kafka que armazena os dados e atende produtores e consumidores. Aqui temos **um** broker (single-node). |
| **Topic** (tópico) | o "nome" de um fluxo de eventos (ex.: `vendas`). É onde se publica e de onde se lê. |
| **Partition** (partição) | cada tópico é dividido em 1..N partições — **a unidade de paralelismo e de ordem**. A ordem é garantida *dentro* de uma partição, não entre partições. Aqui usamos **1 partição** (ordem total, simplicidade). |
| **Offset** | o número sequencial (0, 1, 2, …) de cada mensagem *dentro de uma partição*. É o "endereço" do evento e o que um consumidor guarda para saber por onde parou. |
| **Producer** | quem **publica** eventos em um tópico (seu script Python no host). |
| **Consumer** | quem **lê** eventos de um tópico (Spark/Flink nos containers). |
| **Consumer group** | um conjunto de consumidores que **dividem** as partições de um tópico entre si (escala horizontal). Grupos diferentes leem o mesmo tópico **de forma independente** — cada um com seu offset. |
| **Replication factor** | quantas cópias de cada partição existem em brokers diferentes (tolerância a falha). Com 1 broker, o único valor possível é **1** — daí todo `--replication-factor 1` e os `..._REPLICATION_FACTOR: 1` do compose. |

> **Por que importa:** quando o Tutorial 3/4 pedir `--partitions 1 --replication-factor 1`, você
> já sabe o significado: 1 partição = ordem total e sem paralelismo (ótimo para aprender);
> replicação 1 = sem redundância (impossível ter mais, só há 1 broker). Em produção esses números
> crescem (dezenas de partições, replicação 3).

### KRaft: por que o Kafka não precisa mais do Zookeeper

Historicamente, o Kafka dependia de um segundo sistema, o **Apache Zookeeper**, para guardar os
**metadados** do cluster (quais tópicos existem, quais partições, quem é o líder de cada uma, quem
está vivo). Isso significava operar **dois** sistemas distribuídos. Desde o Kafka 2.8 — e
**estável a partir do 3.x** (usamos o 3.9) — o Kafka traz o **KRaft** (*Kafka Raft*): ele gerencia
os próprios metadados usando o algoritmo de consenso **Raft**, dispensando o Zookeeper.

| Conceito KRaft | O que significa aqui |
|---|---|
| **Papel `controller`** | o(s) nó(s) que mantém(êm) os metadados e coordena(m) o cluster (eleições, quem lidera cada partição). |
| **Papel `broker`** | o nó que serve dados (produção/consumo). |
| **Quórum de controllers** | os controllers elegem um líder por **votação (Raft)**; o líder replica o log de metadados para os demais. |
| Neste tutorial | **um único container** acumula os dois papéis: `KAFKA_PROCESS_ROLES: "broker,controller"`. O quórum tem 1 voto: `KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"`. |

> **Fundamento:** o KRaft usa um **listener dedicado** (aqui, a porta `9093`, protocolo
> `CONTROLLER`) só para o tráfego de metadados entre controllers — separado das portas de dados
> (`9092` interna, `29092` externa). Por isso o compose declara **três** listeners, não dois.

> **Por que importa:** menos peças = menos coisa para instalar, monitorar e quebrar. Para um
> ambiente de estudo em um container só, o KRaft é justamente o que torna "Kafka em um serviço só"
> possível.

### RabbitMQ por dentro: o modelo AMQP

O RabbitMQ implementa o protocolo **AMQP 0-9-1**. Seu modelo é um pouco diferente do de uma "fila
crua": o produtor **não** publica direto na fila — publica em um **exchange**, que **roteia** a
mensagem para uma ou mais filas segundo regras (**bindings**).

| Termo AMQP | O que é |
|---|---|
| **Connection** | uma conexão TCP com o broker (relativamente cara de abrir). |
| **Channel** | uma "sessão" leve *dentro* de uma connection; é onde a aplicação realmente publica/consome. Abrem-se vários channels sobre uma connection. |
| **Exchange** | recebe as mensagens do produtor e decide para quais filas encaminhá-las (por tipo: `direct`, `topic`, `fanout`, `headers`). |
| **Queue** (fila) | onde as mensagens ficam até serem consumidas. |
| **Binding** | a **regra** que liga um exchange a uma fila (ex.: "mensagens com routing key `vendas` vão para a fila `vendas-fila`"). |
| **Default exchange** | um exchange `direct` **sem nome** (`""`) que todo broker já tem: publicar nele usando a routing key = nome da fila entrega **direto** naquela fila. É o atalho que o Tutorial 2 usa para parecer que "publica direto na fila". |

Conceitos de **confiabilidade** que o Tutorial 2 usa:

| Mecanismo | Para que serve |
|---|---|
| **ack** (acknowledgement) | o consumidor **confirma** que processou a mensagem; só então o broker a descarta. Sem ack (ou com falha antes dele), a mensagem é **reentregue** — garante "processa pelo menos uma vez". |
| **prefetch / QoS** | limita quantas mensagens **não confirmadas** o broker manda por vez a um consumidor (`basic_qos(prefetch_count=N)`). Evita que um consumidor lento receba 10 000 mensagens de golpe; é o que viabiliza o **micro-lote** do Tutorial 2. |
| **durable / persistent** | `durable` = a **fila** sobrevive a um restart do broker; `persistent` = a **mensagem** é gravada em disco. Os dois juntos evitam perder dados numa queda. |

> **Fila × log, de novo — agora com nomes.** Repare no contraste com o Kafka: no RabbitMQ, a
> confirmação (**ack**) **remove** a mensagem; não há offset nem replay. É o modelo *destrutivo* —
> perfeito para "faça este trabalho uma vez", que é exatamente o caso do Tutorial 2.

### Spark × Flink

Ambos processam streams e fazem janelas. Neste curso:

- **Spark Structured Streaming** (Tut. 3): você escreve em **PySpark**; o motor processa em
  **micro-batches**.
- **Flink SQL** (Tut. 4): você escreve **só SQL**; o motor processa **evento a evento** (stream
  nativo).

O resultado dos dois é **o mesmo contrato** de saída — de propósito, para comparar.

> **Teoria: micro-batch × streaming nativo.** O **Spark Structured Streaming** processa o fluxo em
> **micro-lotes**: ele acumula os eventos que chegaram num pequeno intervalo, roda um "mini-job
> batch" sobre esse grupo, escreve o resultado e repete. É simples e reaproveita todo o motor
> batch do Spark; a latência é da ordem do intervalo do micro-lote (segundos). O **Flink** é um
> **streaming nativo (event-at-a-time)**: cada evento flui pelo grafo de operadores assim que
> chega, sem esperar formar um lote; a latência pode cair para milissegundos e o modelo de
> **estado** e **event-time** é de primeira classe.

> **Por que importa:** os dois chegam ao **mesmo resultado** (janela de 30s por categoria) por
> caminhos diferentes. Ver o mesmo problema resolvido nos dois paradigmas — e também no de filas —
> é o objetivo pedagógico do curso. Os detalhes de cada motor ficam nos Tutoriais 3 (Spark) e 4
> (Flink).

### MiniStack (S3 local)

Emulador open-source dos serviços da AWS (aqui, só o **S3**) que responde na porta **4566**,
falando o mesmo protocolo do S3 real. É o que dá **paridade local ↔ nuvem**: nos tutoriais, o
código muda apenas o `endpoint_url` e as credenciais.

> **Fundamento: o que é "armazenamento de objetos" (S3).** O S3 não é um sistema de arquivos com
> pastas de verdade, nem um banco de dados: é um **object store**. Você guarda **objetos** (um
> blob de bytes + metadados) dentro de **buckets** (contêineres de nome único, aqui `datalake`), e
> cada objeto é identificado por uma **key** — uma string que *parece* um caminho
> (`vendas/ano=2026/parte-0.parquet`) mas é só um nome plano. Não há "diretórios" físicos; o `/`
> é convenção visual. As operações são simples e por HTTP: `PUT` (gravar), `GET` (ler), `LIST`,
> `DELETE`. É barato, praticamente infinito e a base de quase todo **data lake**.

> **Endpoint e `path-style`.** Toda chamada S3 vai para um **endpoint** (a URL do serviço). Na
> AWS é algo como `https://s3.amazonaws.com`; no MiniStack é `http://localhost:4566` — por isso o
> `--endpoint-url` aparece em **todo** comando `aws` do tutorial. Há ainda dois jeitos de
> endereçar o bucket na URL: **virtual-hosted** (`http://datalake.s3.amazonaws.com/...`, o padrão
> na AWS) e **path-style** (`http://localhost:4566/datalake/...`, o bucket vira o **primeiro
> segmento do caminho**). Emuladores locais usam **path-style**, porque criar um subdomínio por
> bucket em `localhost` não funciona — daí o `s3.path.style.access: true` na config do Flink
> (compose) e o comportamento equivalente no AWS CLI/boto3.

> **Gotcha do checksum (importante):** o AWS CLI v2 e o boto3 recentes calculam, por padrão, o
> checksum **CRC64NVME** ao enviar objetos — e o MiniStack não suporta esse algoritmo. Ao
> **gravar** no S3 local, use `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` (CLI) ou
> `Config(request_checksum_calculation="when_required")` (boto3). No S3 **real** não precisa.

> **Por que o checksum CRC64NVME quebra (o porquê técnico).** Versões recentes do AWS CLI v2 e do
> boto3 passaram a **calcular e enviar um checksum de integridade por padrão** em cada upload,
> escolhendo o algoritmo **CRC64NVME** e mandando-o no cabeçalho `x-amz-checksum-*`. O S3 real
> entende esse cabeçalho e valida; o MiniStack **ainda não implementa** o CRC64NVME e **rejeita** a
> requisição. Ao definir `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`, você diz ao cliente:
> *"só mande checksum quando a operação realmente exigir"* — para um `PUT` simples ele deixa de
> mandar, e o MiniStack aceita. Não é gambiarra: é **alinhar o cliente ao que o servidor
> suporta**. Como o valor default (`when_supported`) é recente, esse gotcha também é recente — por
> isso aparece agora e não em tutoriais antigos.

### Parquet: o formato do data lake

Todos os tutoriais gravam o resultado em **Parquet**. Vale saber por quê, já que é o formato de
saída em todos os destinos.

> **Fundamento: colunar × por linha.** Um CSV/JSON é **orientado a linha**: os campos de um
> registro ficam juntos, um registro após o outro. O Parquet é **colunar**: ele guarda **todos os
> valores de uma mesma coluna juntos**. Isso muda tudo para análise: (1) uma consulta que lê só
> `valor_total` e `categoria` **não precisa ler** as outras colunas do disco (menos I/O); (2)
> valores do mesmo tipo lado a lado **comprimem muito melhor** (o Parquet aplica compressão por
> coluna — Snappy por padrão); (3) o arquivo carrega o **schema embutido** (nomes e tipos das
> colunas), então quem lê não precisa adivinhar o formato.

| Propriedade | Parquet | CSV/JSON |
|---|---|---|
| Organização | **colunar** | por linha |
| Schema | **embutido** no arquivo | externo/implícito |
| Compressão | por coluna (ótima) | fraca |
| Leitura seletiva de colunas | sim (lê só o que precisa) | não (lê a linha inteira) |
| Uso típico | **data lake**, analytics | intercâmbio, logs simples |

> **Por que importa:** Spark, Flink, Athena, Trino, DuckDB, pandas — todos leem Parquet
> nativamente. Gravar o lake em Parquet é o que torna o dado **consultável e barato** logo depois
> de aterrissar no `s3://datalake`.

### Evento e event-time

> **Teoria: o que é um "evento".** Nos três pipelines, a unidade que trafega é um **evento** — um
> fato **imutável** que **aconteceu** num instante: *"a venda X, do cliente Y, do produto Z,
> ocorreu às 20:51:59"*. Ele é um JSON pequeno e **autocontido** (`evento_id`, `cliente_id`,
> `produto_id`, `categoria`, `quantidade`, `valor_total`, `data_venda`). "Autocontido" importa:
> como a `categoria` já vem **dentro** do evento, agregar por categoria é um `GROUP BY` direto, sem
> consultar um catálogo externo (sem `join`).

> **Por que `data_venda` é o event-time (revisão).** Já vimos na seção 2: `data_venda` é o carimbo
> de **quando o fato ocorreu** — o **event-time**. É por esse campo que os Tutoriais 3 e 4 montam
> as janelas de 30s, tolerando eventos que chegam fora de ordem ou atrasados. É a materialização,
> no nosso dado, de todo o conceito de "streaming por tempo de evento".

### Docker e Compose: imagens, containers, rede e volumes

Toda a infraestrutura roda em **Docker**. Como o resto do tutorial é `docker ...`, vale fixar os
conceitos que sustentam cada comando.

| Conceito | O que é |
|---|---|
| **Imagem** | um "molde" imutável e versionado (ex.: `apache/kafka:3.9.0`) com o software e suas dependências já instalados. |
| **Container** | uma **instância em execução** de uma imagem — um processo isolado (sistema de arquivos, rede e PIDs próprios). Muitos containers podem nascer da mesma imagem. |
| **Isolamento** | cada container tem seu próprio ambiente; o Kafka do container não "vê" o Java do seu SO nem vice-versa. É por isso que **não precisamos instalar** Kafka/Spark/Flink na máquina. |
| **`docker compose`** | sobe **vários** containers juntos, descritos declarativamente no `docker-compose.yml` (imagem, portas, variáveis, dependências). Um `up` sobe tudo; um `down` remove tudo. |

> **Fundamento: a rede do Compose e o DNS por nome de serviço.** O Compose cria uma **rede
> virtual** só para os serviços deste arquivo e um **DNS interno**: cada serviço é alcançável pelos
> outros **pelo seu nome** (`kafka`, `ministack`, `flink-jobmanager`). Por isso o Spark acha o
> broker em `kafka:9092` e o Flink acha o S3 em `http://ministack:4566` — sem IPs fixos. Esse DNS
> existe **dentro** da rede; do seu host, você usa `localhost:<porta publicada>`.

> **Fundamento: `ports`, `healthcheck` e `volumes`.**
> - **`ports` (`"29092:29092"`)** publica uma porta do container no seu host (`host:container`).
>   Sem isso, o serviço só é acessível de **dentro** da rede do Compose.
> - **`healthcheck`** é um comando que o Docker roda periodicamente para decidir se o serviço está
>   `healthy`. Outros serviços podem **esperar** por ele (`depends_on: condition: service_healthy`)
>   — é o que garante que o Spark só suba depois de o Kafka estar realmente pronto, não só "no ar".
> - **`volumes` do tipo *bind mount* (`./work:/work`)** liga uma **pasta do host** a um caminho
>   **dentro** do container. Você edita o `consumer_spark.py` no seu editor (host) e o container o
>   enxerga na hora em `/work`. É a ponte entre o seu código e o container.

> **Por que Spark/Flink em container (a decisão-chave).** O Spark 3.5 roda em **Java 8/11/17** e o
> Flink 1.20 desta imagem usa **Java 17** — versões específicas que talvez não sejam a Java do seu
> SO. Empacotando cada motor numa imagem com a **JVM certa já embutida**, o tutorial roda igual no
> macOS, Linux e Windows, sem você mexer no `JAVA_HOME` da sua máquina.

---

## 5. Pré-requisitos por sistema operacional

Você precisa de **três** ferramentas: **Docker**, **Python 3.12** (para os producers/consumers)
e o **AWS CLI v2** (para conversar com o S3, local e na nuvem). Siga a coluna do seu SO.

### 5.1 — macOS (caso corrente)

```bash
# Docker: use o OrbStack (leve) OU o Docker Desktop
brew install --cask orbstack        # abra o OrbStack uma vez
# (alternativa) brew install --cask docker

# Python 3.12 e AWS CLI:
brew install python@3.12 awscli

docker --version
python3 --version                    # Python 3.12.x
aws --version                        # aws-cli/2.x
```

**Entendendo o código (parte a parte):**

- `brew install --cask orbstack` — instala o **OrbStack**, um runtime de containers leve para
  macOS (alternativa ao Docker Desktop). Você o abre **uma vez** para iniciar o "motor" Docker.
- O comentário sobre `brew install --cask docker` mostra a alternativa (Docker Desktop) — escolha
  **um** dos dois, não os dois.
- `brew install python@3.12 awscli` — instala, de uma vez, o **Python 3.12** (para os
  producers/consumers) e o **AWS CLI v2** (para falar com o S3).
- `docker --version`, `python3 --version`, `aws --version` — **verificações**: cada comando deve
  imprimir a versão esperada (Docker qualquer; Python 3.12.x; aws-cli/2.x). Se algum falhar, a
  ferramenta não está instalada ou não está no `PATH`.

### 5.2 — Linux (Ubuntu)

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
sudo usermod -aG docker $USER        # depois FAÇA logout/login para valer

# Python 3.12 e AWS CLI v2:
sudo apt-get install -y python3.12 python3.12-venv unzip curl
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip

docker --version && python3.12 --version && aws --version
```

**Entendendo o código (parte a parte):**

- **Bloco Docker** — segue o repositório **oficial** do Docker: `apt-get update` atualiza o índice
  de pacotes; instala `ca-certificates curl`; cria `/etc/apt/keyrings` e baixa a **chave GPG** do
  Docker (`docker.asc`) para autenticar os pacotes; o `echo ... | sudo tee .../docker.list`
  adiciona o repositório Docker à lista do APT (a linha detecta sozinha a **arquitetura** e a
  **versão** do Ubuntu); novo `apt-get update` e então instala o **engine** (`docker-ce`), o CLI, o
  `containerd` e o **plugin compose** (`docker-compose-plugin`).
- `sudo usermod -aG docker $USER` — coloca seu usuário no grupo `docker` para rodar sem `sudo`;
  **exige logout/login** para valer.
- **Bloco Python/AWS** — instala `python3.12` + `venv`, `unzip` e `curl`; baixa o instalador
  **oficial** do AWS CLI v2 (`.zip`), descompacta, roda `sudo ./aws/install` e apaga os
  temporários.
- A última linha só imprime as três versões se **todas** existirem (o `&&` encadeia: se uma
  falhar, para ali).

### 5.3 — Windows (PowerShell puro, sem WSL no terminal)

No Windows você usa o **Docker Desktop** (internamente ele roda sobre WSL2/Hyper-V — isso é só
o "motor"; você **não precisa abrir um terminal WSL**: todos os comandos abaixo são **PowerShell
nativo**).

```powershell
winget install -e --id Docker.DockerDesktop
winget install -e --id Python.Python.3.12
winget install -e --id Amazon.AWSCLI

# ABRA o Docker Desktop uma vez (espere "Engine running"). Depois, em um NOVO PowerShell:
docker --version
python --version           # Python 3.12.x
aws --version              # aws-cli/2.x
```

**Entendendo o código (parte a parte):**

- `winget install -e --id Docker.DockerDesktop` — instala o **Docker Desktop** (`-e` = casa o ID
  **exato**). É ele que fornece o "motor" Docker no Windows (por baixo, sobre WSL2/Hyper-V).
- `winget install -e --id Python.Python.3.12` e `... Amazon.AWSCLI` — instalam o Python 3.12 e o
  AWS CLI v2 pelo mesmo gerenciador de pacotes.
- Você **abre o Docker Desktop uma vez** e espera "Engine running"; só então, em um **novo**
  PowerShell (para recarregar o `PATH`), os `--version` respondem.

> **Importante (Windows)**: espere o Docker Desktop ficar verde antes de `docker compose`. Se
> `docker` não for reconhecido, feche e reabra o PowerShell (recarrega o `PATH`).

---

## 6. Os arquivos de infraestrutura

Ficam em `1-infraestrutura/local/docker/`. Já vêm prontos (são **infra como código**), mas leia
cada parte.

### 6.1 — `docker-compose.yml`

Declara os serviços. **Ponto-chave: o Kafka tem DOIS listeners** — um para os containers
(`kafka:9092`) e outro para a sua máquina (`localhost:29092`). Sem isso, o producer no host não
consegue publicar.

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: streaming_kafka
    ports:
      - "29092:29092"          # listener HOST (producer na sua máquina)
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENERS: "CONTROLLER://:9093,INTERNAL://:9092,HOST://:29092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:9092,HOST://localhost:29092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      # ... (fatores de replicação = 1, single-node)
```

**Entendendo o código (parte a parte):**

- `image: apache/kafka:3.9.0` — imagem oficial do Kafka **3.9** (já com suporte KRaft nativo).
- `ports: - "29092:29092"` — publica **só** o listener HOST no seu computador. O `9092` (INTERNAL)
  e o `9093` (CONTROLLER) **não** são publicados: são de uso interno da rede do Docker.
- `KAFKA_NODE_ID: 1` — identidade única deste nó no cluster (só há um).
- `KAFKA_PROCESS_ROLES: "broker,controller"` — este nó acumula **os dois papéis** do KRaft (serve
  dados **e** gerencia metadados).
- `KAFKA_LISTENERS: "CONTROLLER://:9093,INTERNAL://:9092,HOST://:29092"` — as **portas em que o
  broker escuta**: metadados (9093), containers (9092), host (29092).
- `KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:9092,HOST://localhost:29092"` — os endereços que o
  broker **anuncia** aos clientes (veja a caixa abaixo). O CONTROLLER não se anuncia a clientes.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` — diz o protocolo de cada listener; aqui **tudo
  PLAINTEXT** (sem TLS — é ambiente local).
- `KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"` — qual listener é o do controller (KRaft).
- `KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"` — por qual listener os brokers falam entre si.
- `KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"` — o quórum KRaft: um votante, o nó `1`,
  alcançável em `kafka:9093`.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — o tópico interno de offsets tem **1 réplica** (só
  há 1 broker). O comentário lembra que **todos** os fatores de replicação são 1 no single-node.

> **Fundamento: `advertised.listeners` e o problema host × container.** Quando um cliente se
> conecta ao Kafka, o broker responde com o **endereço que os clientes devem usar** para falar com
> ele — e esse endereço vem do `KAFKA_ADVERTISED_LISTENERS`, **não** de por onde o cliente se
> conectou. O detalhe: um producer no **host** e o Spark num **container** resolvem nomes de forma
> diferente. Dentro da rede do Docker, `kafka` resolve para o container; no seu host, `kafka` não
> significa nada — mas `localhost` sim.
>
> Por isso há **dois listeners anunciados**:
>
> | Listener | Anuncia | Quem usa | Como resolve |
> |---|---|---|---|
> | **INTERNAL** | `kafka:9092` | Spark/Flink (containers) | DNS do Compose → container |
> | **HOST** | `localhost:29092` | producer no seu PC | `localhost` → porta publicada 29092 |
>
> Se você publicasse do host em `localhost:9092`, o broker até aceitaria a conexão inicial, mas
> **responderia "fale comigo em `kafka:9092`"** — endereço que o host não resolve, e a conexão
> quebraria. É a causa nº 1 de "meu producer não conecta". Regra de bolso: **host → 29092;
> container → 9092**.

Os demais serviços: **rabbitmq** (`:5672` AMQP, `:15672` UI), **ministack** (`:4566` S3, com o
script de criação do bucket), **spark** (mantido vivo; você roda `spark-submit` nele por
`docker exec`) e **flink-jobmanager/flink-taskmanager** (`:8081` UI). Veja o arquivo completo.

### 6.2 — `flink/Dockerfile`

A imagem oficial do Flink **não** traz os conectores de SQL. Este `Dockerfile` adiciona **três
JARs** em `/opt/flink/lib` — o **conector Kafka**, o **formato Parquet** e as **classes do
Hadoop** (exigidas pelo escritor Parquet) — habilita o **plugin S3** e faz **um ajuste fino**
(remover um `commons-cli` antigo do uber jar):

```dockerfile
FROM flink:1.20.1-scala_2.12-java17
ARG FLINK_VERSION=1.20.1
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG HADOOP_UBER_VERSION=2.8.3-10.0
USER root
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends zip; \
    # conector Kafka para Flink SQL
    wget -nv -O /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
    # formato Parquet para o connector filesystem
    wget -nv -O /opt/flink/lib/flink-sql-parquet-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/${FLINK_VERSION}/flink-sql-parquet-${FLINK_VERSION}.jar; \
    # classes do Hadoop (org.apache.hadoop.conf.Configuration) que o escritor Parquet exige
    wget -nv -O /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar; \
    # remove o commons-cli ANTIGO do uber jar (senão quebra o 'flink cancel' do CLI)
    zip -q -d /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar 'org/apache/commons/cli/*'; \
    # habilita o plugin S3 (isolado) movendo o jar de /opt para a pasta de plugins
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar /opt/flink/plugins/s3-fs-hadoop/; \
    apt-get purge -y zip; apt-get autoremove -y; rm -rf /var/lib/apt/lists/*
```

**Entendendo o código (parte a parte):**

- `FROM flink:1.20.1-scala_2.12-java17` — parte da imagem **oficial** do Flink 1.20.1 (Scala 2.12,
  **Java 17**). A JVM certa já vem pronta.
- `ARG FLINK_VERSION / KAFKA_CONNECTOR_VERSION / HADOOP_UBER_VERSION` — **versões pinadas** das
  dependências, usadas para montar as URLs. O sufixo `-1.20` do conector Kafka **casa** com a
  versão do Flink.
- `USER root` — o `apt-get` precisa de root; a imagem base roda como o usuário `flink`.
- `RUN set -eux; ...` — um único `RUN` (uma camada só, imagem menor). `set -eux`: aborta ao
  primeiro erro (`e`), trata variável indefinida como erro (`u`) e ecoa cada comando (`x`).
- `apt-get install ... zip` — instala o `zip`, necessário **só** para remover uma classe de dentro
  de um jar (ver adiante).
- Os três `wget -nv -O /opt/flink/lib/... <URL do Maven Central>` — baixam os **três JARs** direto
  para a pasta de bibliotecas do Flink (`/opt/flink/lib`, que entra no classpath).
- `zip -q -d ...flink-shaded-hadoop-2-uber... 'org/apache/commons/cli/*'` — **remove** as classes
  `commons-cli` de dentro do uber jar (ver segunda caixa abaixo).
- `mkdir -p /opt/flink/plugins/s3-fs-hadoop; cp /opt/flink/opt/flink-s3-fs-hadoop-...jar ...` —
  **habilita o plugin S3**: o Flink carrega plugins de `plugins/<nome>/` de forma **isolada**
  (classloader próprio). O jar já vinha na imagem em `/opt/flink/opt`; só o movemos para a pasta
  ativa.
- `apt-get purge -y zip; autoremove; rm -rf /var/lib/apt/lists/*` — **limpeza**: remove o `zip` e
  os caches do apt para a imagem final ficar menor.

> **Por que a imagem precisa de exatamente estes 3 JARs.** A imagem oficial do Flink traz o motor,
> mas **não** os conectores de SQL — cada integração é um jar à parte que você adiciona:
>
> | JAR | Papel | Sem ele… |
> |---|---|---|
> | `flink-sql-connector-kafka` | ler o tópico Kafka como uma **TABLE** (fonte) no Flink SQL | não há como o SQL enxergar o Kafka |
> | `flink-sql-parquet` | escrever a saída em **Parquet** pelo connector `filesystem` (destino) | não há formato Parquet disponível |
> | `flink-shaded-hadoop-2-uber` | fornece `org.apache.hadoop.conf.Configuration` e afins, que o **escritor Parquet** (`parquet-hadoop`) exige | o job quebra com `ClassNotFoundException` |
>
> Em resumo: um jar para **ler** (Kafka), um para **escrever** (Parquet) e um de **dependência
> transitiva** (classes Hadoop que o escritor Parquet usa por baixo).

> **Por que o `flink-shaded-hadoop-2-uber`?** O formato Parquet do Flink (`parquet-hadoop`) exige
> a classe `org.apache.hadoop.conf.Configuration` no classpath. Sem esse jar, o job falha com
> `ClassNotFoundException`. O acesso ao `s3://` em si continua indo pelo **plugin isolado**
> `s3-fs-hadoop` (Tutorial 4 detalha).
>
> **Por que remover o `commons-cli` do uber jar?** Esse uber jar traz um `commons-cli` antigo que,
> no classpath do CLI do Flink, sobrepõe o do `flink-dist` e **quebra o comando `flink cancel`**
> (`NoSuchMethodError: CommandLine.hasOption(Option)`). O escritor Parquet não usa `commons-cli`,
> então removê-lo do jar é seguro — e o `flink cancel` (Tutorial 4) volta a funcionar.

A configuração de S3 do Flink (endpoint do MiniStack, path-style, credenciais dummy) vai nas
variáveis `FLINK_PROPERTIES` dos serviços `flink-*` no compose.

### 6.3 — `ministack-init/ready.d/01-bucket.sh`

Cria o bucket `s3://datalake` assim que o S3 sobe. **Precisa ficar em `ready.d/`** (roda
*depois* do gateway S3 no ar) e exportar credenciais dummy + `--endpoint-url` (o MiniStack não
injeta credenciais nos scripts de init).

> **Fundamento: por que `ready.d/` e não `init.d/`.** O MiniStack executa scripts de
> inicialização em duas fases: os de **`init.d/`** rodam **enquanto** o serviço ainda está subindo
> (o S3 pode ainda não responder), e os de **`ready.d/`** rodam **depois** que o gateway já está no
> ar. Criar um bucket é uma chamada S3 — logo, precisa do S3 **pronto**: por isso o script fica em
> `ready.d/`. E como o MiniStack **não injeta** credenciais nos scripts de init, o próprio script
> exporta `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` dummy e passa `--endpoint-url
> http://localhost:4566` (de dentro do container, o S3 é `localhost`). O compose monta essa pasta
> em `/docker-entrypoint-initaws.d` (linha `volumes:` do serviço `ministack`).

---

## 7. Subindo o ambiente

Na primeira vez, o Docker baixa as imagens e **constrói a imagem do Flink** (baixa 3 JARs) —
pode levar alguns minutos.

```bash
cd tutoriais/streaming/1-infraestrutura/local/docker
docker compose up -d --build
```

**Entendendo o código (parte a parte):**

- `cd .../docker` — entra na pasta onde está o `docker-compose.yml` (o `docker compose` procura o
  arquivo no diretório atual).
- `docker compose up` — sobe **todos** os serviços declarados no arquivo.
- `-d` (*detached*) — roda em segundo plano e devolve o terminal (sem `-d`, os logs tomariam a
  tela).
- `--build` — **constrói** as imagens que têm `build:` (aqui, a do Flink) antes de subir. Na
  primeira vez isso baixa os 3 JARs do Dockerfile; nas próximas, o Docker reaproveita o cache.

No **Windows (PowerShell)** os comandos são idênticos.

Acompanhe até os serviços ficarem **healthy**:

```bash
docker compose ps
```

**Entendendo o código (parte a parte):**

- `docker compose ps` — lista os serviços **deste** compose e o `STATUS` de cada um. Espere `Up
  (healthy)` para kafka/ministack/rabbitmq (têm `healthcheck`) e `Up` para spark/flink (sem
  `healthcheck`, então mostram só "no ar", não "saudável").

**Resultado esperado** (kafka/ministack/rabbitmq `healthy`; spark/flink `Up`):

```
NAME                          STATUS
streaming_kafka               Up (healthy)
streaming_ministack           Up (healthy)
streaming_rabbitmq            Up (healthy)
streaming_spark               Up
streaming_flink_jobmanager    Up
streaming_flink_taskmanager   Up
```

> Kafka/MiniStack levam ~20–30s para ficar `healthy` na primeira subida.

---

## 8. Validando o ambiente

### 8.1 — Variáveis do AWS CLI (apontando para o MiniStack)

```bash
# macOS / Linux
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required   # MiniStack não aceita CRC64NVME
```

**Entendendo o código (parte a parte):**

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` = `test`/`test` — credenciais **dummy**: o MiniStack
  não valida usuário/senha, mas o AWS CLI **exige** que existam para montar a requisição.
- `AWS_DEFAULT_REGION=us-east-1` — a região padrão (o MiniStack foi configurado com `us-east-1` no
  compose, via `MINISTACK_REGION`).
- `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` — o ajuste do **gotcha do checksum**: impede o
  CLI de mandar o checksum CRC64NVME que o MiniStack rejeita (ver seção 4).
- `export` deixa essas variáveis visíveis para os comandos `aws` seguintes **na mesma sessão** de
  terminal. (No PowerShell, o equivalente é `$env:NOME="valor"`.)

```powershell
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="test"; $env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"; $env:AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
```

> As mesmas quatro variáveis, na sintaxe do PowerShell (`$env:NOME="valor"`, comandos separados por
> `;`). Valem apenas para a sessão atual do PowerShell — abra outra e precisará defini-las de novo.

### 8.2 — Script de validação (macOS/Linux)

```bash
cd tutoriais/streaming/1-infraestrutura/local
bash validar.sh
```

**Entendendo o código (parte a parte):**

- `cd .../local` — sobe um nível em relação ao `docker/`: o `validar.sh` fica na pasta `local`.
- `bash validar.sh` — roda o script de validação. Ele faz **4 checagens** (as `==> N/4` da saída):
  (1) lista tópicos no Kafka via `docker exec`; (2) `rabbitmq-diagnostics ping`; (3) `aws s3 ls` e
  procura o bucket `datalake`; (4) confere que os containers `spark` e `flink_*` aparecem em
  `docker ps`. Ao final, imprime `OK` ou conta as falhas (e sai com código ≠ 0 se houver alguma).

**Resultado esperado**:

```
==> 1/4 Kafka: listar tópicos (broker no ar?)
    OK (Kafka respondendo em kafka:9092)
==> 2/4 RabbitMQ: ping
    OK (RabbitMQ no ar; UI em http://localhost:15672 guest/guest)
==> 3/4 MiniStack: bucket datalake existe?
    OK (s3://datalake presente)
==> 4/4 Spark e Flink: containers de pé?
    OK (spark + flink jobmanager/taskmanager rodando)

✅ Ambiente local de streaming OK.
```

> No **Windows**, rode as verificações manualmente (próxima seção) — o `.sh` é para shells Unix.

---

## 9. Explorando cada serviço

### 9.1 — Kafka (criar o tópico `vendas`)

Os Tutoriais 3 e 4 usam o tópico `vendas`. Crie-o agora (idempotente):

```bash
docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vendas --partitions 1 --replication-factor 1

docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Entendendo o código (parte a parte):**

- `docker exec streaming_kafka ...` — roda um comando **dentro** do container do Kafka; por isso o
  `--bootstrap-server localhost:9092`: do ponto de vista do container, o broker é ele mesmo,
  `localhost:9092`.
- `/opt/kafka/bin/kafka-topics.sh` — a ferramenta CLI de administração de tópicos que já vem na
  imagem.
- `--create --if-not-exists` — cria o tópico, mas **não falha** se ele já existir (idempotente —
  pode rodar de novo à vontade).
- `--topic vendas` — o nome do tópico usado pelos Tutoriais 3 e 4.
- `--partitions 1` — **uma** partição (ordem total, sem paralelismo; suficiente para aprender).
- `--replication-factor 1` — **uma** réplica (é o único valor possível com 1 broker).
- O segundo comando (`--list`) lista os tópicos existentes — deve mostrar `vendas`.

**Resultado esperado**: `vendas`.

### 9.2 — RabbitMQ (UI de gerenciamento)

Abra <http://localhost:15672> no navegador (usuário `guest`, senha `guest`). Você verá filas,
taxas de mensagens etc. O Tutorial 2 usa a fila `vendas-fila`.

### 9.3 — Flink (Web UI)

Abra <http://localhost:8081>. É onde você acompanha os **jobs** do Flink (Tutorial 4) — task
slots, throughput, checkpoints.

### 9.4 — S3 local (ainda vazio)

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/ --recursive
```

**Entendendo o código (parte a parte):**

- `aws` — o AWS CLI (as variáveis da seção 8.1 precisam estar exportadas nesta sessão).
- `--endpoint-url http://localhost:4566` — **redireciona** o CLI para o MiniStack em vez da AWS
  real. Sem isso, ele tentaria a nuvem.
- `s3 ls s3://datalake/` — lista o conteúdo do bucket `datalake`.
- `--recursive` — desce por todos os "prefixos" (as pseudo-pastas). Neste momento deve vir
  **vazio**.

Ainda não há nada — os dados chegam quando você rodar os Tutoriais 2, 3 ou 4. Para testar a
escrita (e o gotcha do checksum):

```bash
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
echo "ola lake" > /tmp/t.txt
aws --endpoint-url http://localhost:4566 s3 cp /tmp/t.txt s3://datalake/t.txt
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/
```

**Entendendo o código (parte a parte):**

- `export AWS_REQUEST_CHECKSUM_CALCULATION=when_required` — reforça o ajuste do **checksum** (a
  **gravação** é o passo que dispara o gotcha CRC64NVME).
- `echo "ola lake" > /tmp/t.txt` — cria um arquivinho de teste no host.
- `s3 cp /tmp/t.txt s3://datalake/t.txt` — **grava** (upload) esse arquivo como objeto de key
  `t.txt` no bucket. É aqui que o checksum importaria.
- `s3 ls s3://datalake/` — confirma que o objeto `t.txt` agora aparece no bucket.

---

## 10. Parando e limpando

```bash
cd tutoriais/streaming/1-infraestrutura/local/docker

docker compose stop        # pausar (sobe rápido depois)
docker compose down        # remover containers e rede
docker compose down -v     # remover também volumes/órfãos (recomeço limpo)
```

**Entendendo o código (parte a parte):**

- `docker compose stop` — **pausa** os containers sem removê-los; um `up` depois sobe rápido e
  preserva o estado (tópico, filas, bucket).
- `docker compose down` — **remove** os containers e a rede do Compose (mas mantém imagens e
  volumes nomeados). O estado em memória some.
- `docker compose down -v` — como o `down`, e ainda remove **volumes** e órfãos: o recomeço mais
  limpo possível.
- São **três alternativas** — use a que couber ao seu objetivo; não é preciso rodar as três em
  sequência.

> Não declaramos volumes persistentes de dados: `down` já zera o estado (o bucket é recriado, o
> tópico Kafka e as filas somem). Bom para recomeçar do zero.

---

## 11. Troubleshooting

A maioria dos problemas abaixo cai em três famílias já discutidas: **Docker não está de pé**,
**endereço errado do Kafka** (host × container, seção 6.1) ou **checksum do S3** (seção 4). A
tabela liga cada sintoma à causa e à solução.

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Cannot connect to the Docker daemon` | OrbStack/Docker Desktop parado | Abra o OrbStack/Docker Desktop; Linux: `sudo systemctl start docker` |
| Producer no host não conecta ao Kafka | Faltou o listener HOST | Publique em `localhost:29092` (não `9092`); confira o mapeamento `29092:29092` |
| `docker compose` falha ao construir o Flink | Sem internet p/ baixar os JARs | Rode com internet; confira as URLs do `flink/Dockerfile` |
| MiniStack fica `health: starting` p/ sempre | Healthcheck com `curl` (ausente na imagem) | Use o healthcheck com `python` deste compose |
| Bucket `datalake` não aparece | Script de init na pasta errada | Deve estar em `ministack-init/ready.d/` e exportar `AWS_*=test` + `--endpoint-url` |
| `... CRC64NVME` ao subir arquivo | Checksum padrão do AWS CLI v2/boto3 | `export AWS_REQUEST_CHECKSUM_CALCULATION=when_required` |
| `port is already allocated` (29092/5672/4566/8081) | Porta em uso | Pare o serviço conflitante ou ajuste as portas no compose |
| Spark: `FileNotFoundException ... .ivy2/cache/resolved-...xml` | `--packages` tenta escrever no `user.home` do container | Adicione `--conf spark.jars.ivy=/tmp/.ivy2` ao `spark-submit` (detalhado no Tut. 3) |

---

**Pronto!** Seu ambiente local de streaming está de pé. Agora siga para:

- **`2-filas/local/TUTORIAL.md`** — processamento por **filas** (RabbitMQ), micro-lote → Parquet.
- **`3-kafka-spark/local/TUTORIAL.md`** — **Kafka + Spark**, janela de 30s → Parquet.
- **`4-kafka-flink/local/TUTORIAL.md`** — **Kafka + Flink (SQL)**, janela de 30s → Parquet.

Deixe os containers **rodando** enquanto faz esses tutoriais.

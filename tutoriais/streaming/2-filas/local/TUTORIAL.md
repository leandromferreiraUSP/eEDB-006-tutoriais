# Tutorial 2 (Local): Streaming por Filas com RabbitMQ (micro-lote → Parquet)

> Versão **longa e explicativa**. Você vai publicar um fluxo contínuo de **eventos de venda** em
> uma **fila RabbitMQ** (com um producer Python) e consumi-lo com um consumer Python que lê
> **uma mensagem por vez** (at-least-once, com `ack`), **acumula um micro-lote** e faz **flush**
> em **Parquet** no data lake S3 (MiniStack).
>
> Este é o **espelho local** do Tutorial 2 AWS (SQS + Lambda): mesmo paradigma de **fila**,
> mesmo evento, mesmo destino. Muda só a tecnologia de transporte (RabbitMQ no lugar do SQS).
>
> **Pré-requisito**: ter feito o `1-infraestrutura/local` e estar com os containers rodando
> (`streaming_rabbitmq` e `streaming_ministack`). Só os comandos? Veja `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Objetivo técnico e lógico](#1-objetivo-técnico-e-lógico)
2. [Decisões de projeto (e por quê)](#2-decisões-de-projeto-e-por-quê)
3. [Fundamentos: filas, AMQP e garantias de entrega](#3-fundamentos-filas-amqp-e-garantias-de-entrega)
4. [O fluxo deste tutorial](#4-o-fluxo-deste-tutorial)
5. [Preparando: ambiente Python e RabbitMQ no ar](#5-preparando-ambiente-python-e-rabbitmq-no-ar)
6. [O producer (Python → RabbitMQ)](#6-o-producer-python--rabbitmq)
7. [O consumer (RabbitMQ → micro-lote → Parquet)](#7-o-consumer-rabbitmq--micro-lote--parquet)
8. [Rodando o consumer](#8-rodando-o-consumer)
9. [Verificando o resultado no S3](#9-verificando-o-resultado-no-s3)
10. [Parando e limpando](#10-parando-e-limpando)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Objetivo técnico e lógico

Aqui o transporte é uma **fila** (RabbitMQ) e o processamento é **unitário e enfileirado**: o
consumer lê **uma mensagem por vez**, confirma o recebimento (`ack`) e a mensagem **some** da
fila. Esse é o modelo clássico de **message queue** — ponto-a-ponto, ideal para trabalho
unitário e desacoplamento entre quem produz e quem consome.

O objetivo **lógico** é mostrar que, mesmo lendo mensagem a mensagem, dá para **materializar em
lote** no data lake: em vez de gravar um arquivinho por mensagem (o que geraria milhares de
objetos minúsculos no S3), o consumer **acumula um micro-lote** em memória e só grava um Parquet
quando o buffer atinge `BATCH` mensagens **ou** quando passam `FLUSH_SECS` segundos — o que vier
primeiro. Cada flush vira **um** arquivo `lote-*.parquet`.

O objetivo **técnico** é fazer isso com **entrega confiável** (at-least-once, via `ack` só
depois de bufferizar) e gravando no S3 local (MiniStack) com o **mesmo código** que rodaria na
AWS — mudando apenas `endpoint_url` e credenciais.

Diferente dos Tutoriais 3 e 4 (Kafka + Spark/Flink), aqui **não há janela temporal**: o
agrupamento é um **micro-lote por contagem/tempo de chegada**, não uma agregação por
*event-time*. É a diferença fundamental entre o paradigma de **filas** e o de **tópicos/log**.

Vamos destrinchar essas três frases, porque cada uma esconde uma ideia central de sistemas de
mensageria. Quando você entende **por que** cada peça existe, o código de `producer.py` e
`consumer.py` deixa de ser "receita" e vira uma decisão consciente que você saberá adaptar.

> **Fundamento (o que é "streaming por fila"):** *streaming* não é sinônimo de "processar tudo em
> tempo real com uma engine gigante". No sentido mais literal, é apenas **dados em movimento
> contínuo, tratados à medida que chegam** — em vez de esperar um arquivo fechado e rodar um job
> em lote. Uma fila é a forma mais simples e antiga de fazer isso: um lado **empurra** eventos, o
> outro lado os **retira e processa**, indefinidamente. Não há Spark nem Flink aqui; o "motor de
> stream" é o seu próprio `consumer.py` rodando em laço.

> **Por que importa (a intuição do dia a dia):** pense numa fila de banco. Cada cliente é uma
> **mensagem**; os caixas são **consumers**. O cliente não precisa saber qual caixa vai atendê-lo,
> e o caixa não precisa saber quem é o próximo — a **fila no meio** desacopla os dois. Se um caixa
> sai para o almoço (consumer cai), a fila **segura** os clientes; ninguém é mandado embora. Se
> chega gente demais, a fila **cresce** e serve de amortecedor. Toda a teoria abaixo é uma versão
> formal dessa cena.

O que você vai **construir e observar** ao final:

| Peça | Papel neste tutorial | Onde aparece |
|---|---|---|
| **Producer** | Gera eventos de venda sintéticos e os **publica** na fila `vendas-fila`. | `producer.py` |
| **Broker (RabbitMQ)** | Recebe, **persiste** e entrega as mensagens; guarda o que ainda não foi confirmado. | container `streaming_rabbitmq` |
| **Consumer** | Lê 1 mensagem, dá `ack`, **acumula um micro-lote** e faz **flush** para Parquet. | `consumer.py` |
| **Data lake (MiniStack)** | Recebe os arquivos `lote-*.parquet` particionados por data. | container `streaming_ministack` (S3 em `:4566`) |

---

## 2. Decisões de projeto (e por quê)

| Decisão | O que escolhemos | Por quê |
|---|---|---|
| **Fila, não tópico** | RabbitMQ (fila `vendas-fila`) | Modelo ponto-a-ponto: cada mensagem vai para **um** consumer e some após o `ack`. É o paradigma de trabalho unitário/desacoplamento (contraste com o log do Kafka nos Tut. 3/4). |
| **Micro-lote no consumer** | Buffer de `BATCH=50` **ou** `FLUSH_SECS=15` | Ler 1 a 1 e gravar 1 arquivo por mensagem geraria milhares de objetos minúsculos. Bufferizar e dar flush produz Parquets saudáveis, sem depender de janela de tempo do evento. |
| **`ack` explícito (at-least-once)** | `basic_ack` após colocar no buffer | Se o consumer cair antes do `ack`, o RabbitMQ **reentrega** a mensagem. Garante que nenhum evento se perca (podendo, no pior caso, duplicar — semântica *at-least-once*). |
| **`basic_qos(prefetch=BATCH)`** | Limita mensagens não confirmadas em voo | O broker só entrega até `BATCH` mensagens antes de exigir `ack`. Combina com o tamanho do micro-lote e evita "engolir" a fila inteira de uma vez. |
| **Fila `durable=True` + `delivery_mode=2`** | Fila e mensagens persistentes | Sobrevivem a reinício do broker. Boa prática para não perder trabalho enfileirado. |
| **Gotcha do checksum (boto3)** | `Config(request_checksum_calculation="when_required")` | O boto3 recente calcula **CRC64NVME** ao enviar objetos, e o **MiniStack não suporta**. Sem esse `Config`, o `put_object` falha. No S3 **real** não precisa. |
| **`categoria` embutida no evento** | Vem dentro do JSON | Mantém o **mesmo contrato** dos outros tutoriais; não há `join` com catálogo. Aqui só persistimos o bruto, sem agregar. |
| **Destino `filas/dt=YYYY-MM-DD/`** | Partição por data no S3 | Mesma convenção de particionamento dos demais tutoriais (`spark/dt=...`, `flink/dt=...`). |

Repare que essas oito decisões não são independentes — elas formam **duas cadeias de raciocínio**
que se cruzam:

- **Cadeia da confiabilidade:** *fila durável* → *mensagem persistente* → *`ack` só depois de
  bufferizar* → *`prefetch` casado com o lote*. Todas apontam para o mesmo objetivo: **nenhum
  evento se perde**, nem se o broker reiniciar, nem se o consumer morrer no meio de um lote.
- **Cadeia do armazenamento:** *micro-lote* → *Parquet em memória* → *partição por data* → *gotcha
  do checksum*. Todas apontam para **gravar bem no data lake**: poucos arquivos grandes, colunar,
  organizados por dia e compatíveis com o MiniStack.

> **Teoria (por que "decisões de projeto" vêm antes do código):** em sistemas de dados, quase todo
> parâmetro é um **trade-off**, não uma verdade absoluta. `BATCH=50` não é "o valor certo" — é um
> ponto escolhido na curva **latência × throughput × número de arquivos**. `at-least-once` não é
> "melhor que exactly-once" — é o que dá para garantir **de graça** com um `ack`, deixando a
> deduplicação para quem lê. Entender o eixo do trade-off é o que te deixa mudar os números com
> segurança quando o cenário mudar (mais volume, menos latência, storage diferente).

A seção seguinte aprofunda a **teoria** por trás dessas decisões antes de a gente escrever
qualquer linha de Python. Se você só quer rodar, pode pular direto para a
[seção 4](#4-o-fluxo-deste-tutorial) — mas volte aqui quando quiser entender o *porquê*.

---

## 3. Fundamentos: filas, AMQP e garantias de entrega

Esta seção é a **espinha teórica** do tutorial. Ela responde a sete perguntas, na ordem em que
elas aparecem no código: (1) por que filas existem, (2) o que é AMQP e como o RabbitMQ roteia,
(3) que garantias de entrega existem e qual usamos, (4) o que é QoS/prefetch, (5) o que é
durabilidade, (6) por que agrupar em micro-lote e (7) por que "fila" é diferente de "tópico".

### 3.1 — Por que filas existem: desacoplamento, ponto-a-ponto, backpressure

Imagine ligar o producer **diretamente** no consumer, por uma chamada de função. Três problemas
aparecem na hora: se o consumer estiver lento, o producer **trava** esperando; se o consumer
**cair**, o evento se **perde**; e se você quiser **dois** consumers, precisa reescrever o
producer. A fila resolve os três de uma vez, colocando um **intermediário durável** no meio.

> **Fundamento (desacoplamento):** uma fila **desacopla no tempo, no espaço e na taxa**.
> **No tempo:** producer e consumer não precisam estar vivos ao mesmo tempo — o producer publica
> agora, o consumer processa daqui a uma hora. **No espaço:** nenhum lado conhece o endereço do
> outro; ambos só conhecem o **broker** e o **nome da fila**. **Na taxa:** o producer pode publicar
> a 1000/s enquanto o consumer processa a 100/s; a diferença **acumula na fila** em vez de estourar.

> **Teoria (ponto-a-ponto vs. publish/subscribe):** existem dois grandes padrões de mensageria.
> No **ponto-a-ponto** (este tutorial), cada mensagem é entregue a **exatamente um** consumer e é
> **removida** da fila após o `ack` — é trabalho a ser feito *uma vez*. No **publish/subscribe**
> (Tut. 3/4, Kafka), a mensagem é um **fato** que fica no log e pode ser lido por **vários**
> consumidores independentes, cada um no seu ritmo. Fila = "faça esta tarefa"; tópico = "isto
> aconteceu".

> **Teoria (competing consumers):** se você subir **N** consumers apontando para a **mesma** fila,
> o RabbitMQ distribui as mensagens entre eles (round-robin, respeitando o `prefetch`). É o padrão
> **competing consumers**: escala horizontal *de graça* — cada mensagem ainda vai para **um só**
> consumer, mas o trabalho total se divide. Neste tutorial rodamos **um** consumer, mas o desenho
> já suporta vários sem mudar uma linha do producer. Esse é o pagamento do desacoplamento.

> **Por que importa (backpressure):** *backpressure* é o mecanismo que impede um produtor rápido de
> afogar um consumidor lento. Numa chamada direta, o produtor afoga. Numa fila, a **profundidade da
> fila** (nº de mensagens *Ready*) é o sinal de backpressure: se ela só cresce, seu consumer não dá
> conta e você precisa de mais consumers ou mais `BATCH`. Na UI do RabbitMQ, a linha **Ready**
> subindo sem parar é exatamente esse alarme. O `prefetch` (seção 3.4) é a válvula que aplica
> backpressure **de dentro** do consumer, limitando quantas mensagens ele segura sem confirmar.

### 3.2 — AMQP e RabbitMQ: connection, channel, exchange, queue, binding, routing key

O RabbitMQ fala **AMQP 0-9-1**, um protocolo com um modelo de roteamento que vale entender, porque
o `producer.py` usa uma versão "invisível" dele (o *default exchange*).

> **Fundamento (connection × channel):** uma **connection** é **uma** conexão TCP com o broker
> (cara de abrir). Um **channel** é uma sessão lógica **multiplexada dentro** dessa connection —
> barata, e é por onde passam de fato os comandos (`queue_declare`, `basic_publish`, `basic_ack`).
> A regra prática: **uma connection por processo, um channel por thread/tarefa**. No `pika`, a
> `BlockingConnection` abre a connection e `conn.channel()` abre o channel. Todo o resto do código
> conversa pelo `ch`.

> **Fundamento (o caminho de uma mensagem):** no AMQP, o producer **nunca publica direto numa
> fila**. Ele publica num **exchange**, e o exchange decide para quais filas copiar a mensagem,
> usando **bindings** (as ligações exchange→fila) e uma **routing key** (um rótulo na mensagem). O
> desenho é: `producer → exchange → (binding + routing key) → queue → consumer`. Tipos de exchange:
> **direct** (routing key tem de bater exatamente com o binding), **fanout** (copia para todas as
> filas ligadas), **topic** (padrões com curinga) e **headers**.

Então onde está o exchange no nosso `basic_publish(exchange="", routing_key=QUEUE, ...)`? A string
vazia `""` é o **default exchange**, um caso especial que o RabbitMQ mantém pronto:

> **Teoria (o truque do default exchange):** o **default exchange** é um exchange do tipo *direct*,
> sem nome (`""`), ao qual **toda fila é automaticamente ligada com um binding cuja routing key é o
> próprio nome da fila**. Consequência: publicar no default exchange com `routing_key="vendas-fila"`
> entrega **direto na fila `vendas-fila`**, sem você precisar declarar exchange nem binding. É
> "açúcar sintático" que faz o RabbitMQ *parecer* uma fila simples ponto-a-ponto — perfeito para
> este tutorial. Em produção, com roteamento de verdade, você declararia um exchange nomeado e
> bindings explícitos.

| Conceito AMQP | O que é | No nosso código |
|---|---|---|
| **Connection** | Conexão TCP com o broker | `pika.BlockingConnection(...)` |
| **Channel** | Sessão lógica dentro da connection | `conn.channel()` → `ch` |
| **Exchange** | Roteador que recebe do producer | `exchange=""` (o *default*) |
| **Queue** | Buffer nomeado que guarda mensagens | `vendas-fila` (via `queue_declare`) |
| **Binding** | Liga exchange→fila | Implícito: toda fila ↔ default exchange |
| **Routing key** | Rótulo que o exchange usa para rotear | `routing_key="vendas-fila"` |

### 3.3 — Garantias de entrega: at-most-once, at-least-once, exactly-once e o papel do `ack`

Toda a confiabilidade da fila gira em torno de **uma** decisão: **quando** o consumer diz ao
broker "pode apagar, eu já cuidei desta mensagem". Esse "avisar" é o **acknowledgement** (`ack`).

> **Fundamento (as três semânticas):**
> - **At-most-once** ("no máximo uma vez"): entrega e esquece. Se o consumer cair, a mensagem
>   **se perde**. Zero duplicatas, mas há perda. (No AMQP: *auto-ack*, o broker apaga assim que
>   entrega.)
> - **At-least-once** ("pelo menos uma vez", **o que usamos**): o broker só apaga a mensagem depois
>   do `ack` do consumer. Se o consumer cair antes do `ack`, o broker **reentrega**. Nunca perde,
>   mas **pode duplicar**.
> - **Exactly-once** ("exatamente uma vez"): nunca perde e nunca duplica. É o mais desejado e o
>   mais **caro** — exige transações ou deduplicação fim-a-fim; nenhum broker entrega isso "de
>   graça" no ponto de entrega. Na prática, aproxima-se dele com *at-least-once* + **idempotência**.

> **Por que importa (o momento do `ack` é tudo):** no `consumer.py`, o `ch.basic_ack(...)` vem
> **depois** de `buffer.append(...)`. Isso é deliberado. A mensagem só é "dada como resolvida"
> quando já está **segura no buffer** que será persistido. Se déssemos `ack` **antes** de guardar
> (ou usássemos auto-ack), uma queda entre "recebi" e "gravei" **perderia** o evento — viraria
> *at-most-once* sem querer. Dar `ack` cedo demais é o erro clássico de quem começa com filas.

> **Teoria (redelivery e o `redelivered`):** quando um consumer morre com mensagens **não
> confirmadas** em voo, o broker as devolve para a fila e as reentrega — possivelmente a **outro**
> consumer. Elas voltam marcadas com a flag `redelivered=True`, um aviso de "talvez você já tenha
> visto isto". É exatamente o que produz as **duplicatas** da semântica at-least-once.

> **Por que importa (idempotência é a sua rede de segurança):** como pode haver duplicata, o
> consumidor deveria ser **idempotente** — processar a mesma mensagem duas vezes deve dar o mesmo
> resultado que processá-la uma vez. Cada evento carrega um `evento_id` (um `uuid4`) **justamente**
> para permitir isso: um passo de dedup a jusante (no data lake ou numa tabela de destino) pode
> descartar `evento_id` repetido. Neste tutorial **não** deduplicamos (persistimos o bruto), mas o
> contrato já traz a chave que torna a dedup possível — é o que separa "at-least-once que perde
> dados" de "at-least-once que você conserta depois".

### 3.4 — QoS / prefetch: controle de fluxo do lado do consumer

> **Fundamento (`basic_qos(prefetch_count=N)`):** o **prefetch** limita quantas mensagens o broker
> pode ter **entregado mas ainda não confirmadas** (*unacked*) para um consumer, ao mesmo tempo. Com
> `prefetch_count=50`, o broker manda até 50 mensagens e **para**; só volta a mandar à medida que
> chegam `ack`s. Sem prefetch (valor 0 = ilimitado), o broker despeja **a fila inteira** no socket
> do consumer de uma vez.

> **Por que importa (casar prefetch com o lote):** usamos `prefetch_count=BATCH` **de propósito**.
> O consumer precisa juntar até `BATCH=50` mensagens para fechar um lote; deixar o broker adiantar
> ~50 mensagens mantém o pipeline **abastecido** sem estufar a memória com milhares de eventos
> pendentes. Prefetch **baixo demais** faz o consumer viver ocioso esperando a próxima entrega
> (throughput cai); **alto demais** transforma o consumer num "aspirador" que segura mensagens sem
> confirmar (memória sobe, e uma queda gera muita reentrega). `prefetch = BATCH` é o meio-termo
> natural aqui.

### 3.5 — Durabilidade: o que sobrevive a um restart do broker

Persistência no RabbitMQ tem **duas chaves independentes**, e você precisa das **duas** para não
perder nada num reinício do broker.

> **Fundamento (fila durável × mensagem persistente):**
> - **`queue_declare(durable=True)`** torna a **definição da fila** durável: após um restart, a fila
>   `vendas-fila` **ainda existe**. Sem isso, a fila (e tudo dentro) some ao reiniciar.
> - **`delivery_mode=2`** (na `BasicProperties` do publish) marca **cada mensagem** como
>   **persistente**: o broker a grava em disco, não só na memória.
>
> As duas juntas: fila durável **guardando** mensagens persistentes = o trabalho enfileirado
> **sobrevive** a um `docker restart` do broker. Fila durável com mensagem **transiente** (mode 1):
> a fila volta, mas vazia. Fila **não** durável: nem a fila volta. Por isso o `producer.py` usa
> `durable=True` **e** `delivery_mode=2`.

> **Por que importa (durabilidade não é o mesmo que `ack`):** são camadas diferentes. Durabilidade
> protege contra **o broker cair**; o `ack` protege contra **o consumer cair**. Você precisa das
> duas para uma entrega ponta-a-ponta confiável — e nenhuma delas, sozinha, protege contra perda de
> dados *depois* que o consumer já confirmou mas antes de gravar (por isso o `ack` vem **depois** do
> buffer, seção 3.3).

### 3.6 — Micro-lote: o "small files problem" e o trade-off latência × throughput

O consumer lê **1 mensagem por vez**, mas grava **em lote**. Por quê não gravar um Parquet por
mensagem, já que estamos lendo uma a uma?

> **Fundamento (o "small files problem"):** data lakes (S3, HDFS) e engines de consulta (Spark,
> Trino, Athena) sofrem muito com **muitos arquivos minúsculos**. Cada arquivo tem custo fixo: uma
> chamada de rede para listar/abrir, metadados, e — no Parquet — um **rodapé (footer)** e estruturas
> de coluna que não valem a pena para 1 linha. Mil mensagens = mil objetos de ~200 bytes = leitura
> lenta, listagens caras e overhead de metadados que humilha o volume real de dados. Agrupar 50
> eventos num arquivo troca **1000 objetos ruins por 20 objetos saudáveis**.

> **Teoria (latência × throughput):** micro-batching é o **botão** entre dois extremos.
> **Lote pequeno / flush frequente** = menor **latência** (o dado chega rápido ao lake) mas menor
> **throughput** e mais arquivos. **Lote grande / flush raro** = maior throughput e arquivos
> gordos, mas o dado demora mais a aparecer e você segura mais coisa na memória (mais a perder se
> cair). `BATCH=50` e `FLUSH_SECS=15` são um ponto **didático** nessa curva: arquivos com dezenas de
> linhas, latência de no máximo ~15s.

> **Por que importa (o "OU" entre tamanho e tempo):** o flush dispara por **contagem** (`>= BATCH`)
> **ou** por **tempo** (`>= FLUSH_SECS`), o que vier primeiro. Só por contagem, uma fila lenta
> deixaria 3 mensagens **presas para sempre** na memória, sem nunca virar arquivo. O gatilho de
> tempo é a **garantia de latência máxima**: mesmo devagar, o que estiver no buffer é gravado em até
> 15s. Só por tempo, uma rajada geraria arquivos enormes e picos de memória. Os dois gatilhos juntos
> cobrem os dois regimes — fila cheia e fila vazia.

### 3.7 — Fila × tópico: por que aqui NÃO há janela (contraste com os Tut. 3 e 4)

> **Teoria (o agrupamento aqui é por *chegada*, não por *event-time*):** neste tutorial o lote é
> definido por **quando a mensagem chegou ao consumer** (contagem/relógio de parede) — não pelo
> horário do evento (`data_venda`). Não há **janela temporal** nem agregação; o consumer só
> **acumula e despeja o bruto**. Nos Tutoriais 3 (Spark) e 4 (Flink), o Kafka guarda um **log** e a
> engine agrupa por **janelas de event-time** (ex.: "soma das vendas a cada 30s pelo horário do
> evento"), tolerando eventos fora de ordem e atrasados. Essa é a diferença de fundo entre os dois
> paradigmas — e a razão de o `data_venda` existir no evento mas **não** ser usado para janelar
> aqui: mantemos o **mesmo contrato** dos outros tutoriais, mesmo sem consumir o campo.

| Aspecto | **Fila** (este tutorial) | **Tópico / log** (Tut. 3 e 4) |
|---|---|---|
| Tecnologia | RabbitMQ | Kafka |
| Mensagem após consumo | **Some** (removida no `ack`) | **Permanece** no log (retenção) |
| Nº de leitores | Um (competing consumers dividem) | Vários grupos, cada um lê tudo |
| Agrupamento | Micro-lote por **chegada** (contagem/tempo) | **Janela** por **event-time** |
| Reprocessar do zero | Não (mensagem já sumiu) | Sim (reposiciona o *offset*) |
| Metáfora | "Faça esta tarefa" | "Isto aconteceu" |

---

## 4. O fluxo deste tutorial

```
  producer.py (host)             RabbitMQ                 consumer.py (host)                 MiniStack
  gera eventos de venda ─publica─►  fila       ─consome──►  lê 1 msg + ack (at-least-once)  ──►  s3://datalake/
  ~5/s em JSON            :5672   "vendas-fila"   1 a 1      buffer BATCH=50 / FLUSH_SECS=15      filas/dt=.../
                                                            flush → Parquet                       lote-*.parquet
```

- O **producer** roda na sua máquina e publica na fila `vendas-fila` via `localhost:5672` (AMQP).
- O **consumer** também roda na sua máquina: lê mensagem a mensagem, dá `ack`, acumula o
  micro-lote e, ao atingir `BATCH` ou `FLUSH_SECS`, grava **um** Parquet no MiniStack.
- Não há Spark nem Flink aqui — o "processamento" é o próprio consumer Python.

> **Por que importa (três processos, três papéis):** producer, broker e consumer rodam **separados**
> — dois processos Python no seu host e um broker no container. Você poderia **parar** qualquer um
> sem derrubar os outros: pare o consumer e a fila **acumula**; pare o producer e o consumer **drena
> e fica ocioso**; reinicie o broker e (graças à durabilidade) o trabalho **continua**. Essa
> independência é o desacoplamento da seção 3.1 saindo do papel. Rodar cada um em seu próprio
> terminal (seções 6 a 9) é justamente para você **ver** os três lados ao vivo.

---

## 5. Preparando: ambiente Python e RabbitMQ no ar

### 5.1 — Confira que o RabbitMQ (Tutorial 1) está no ar

Este tutorial depende dos containers do Tutorial 1. Confira que `streaming_rabbitmq` e
`streaming_ministack` estão de pé:

```bash
docker compose -f tutoriais/streaming/1-infraestrutura/local/docker/docker-compose.yml ps
```

Você deve ver `streaming_rabbitmq` e `streaming_ministack` como `Up (healthy)`. Abra também a
**UI de gerenciamento** do RabbitMQ para acompanhar a fila ao vivo:
<http://localhost:15672> (usuário `guest`, senha `guest`).

> Se algum container não estiver de pé, volte ao `1-infraestrutura/local` e rode
> `docker compose up -d`.

> **Por que importa (a UI de gerenciamento é o seu "raio-x"):** a interface em `:15672` é o painel
> do broker. As colunas que vamos observar: **Ready** (mensagens esperando um consumer),
> **Unacked** (entregues mas ainda sem `ack` — limitadas pelo `prefetch`) e **Total**. Os gráficos
> de **publish rate** e **ack rate** mostram, ao vivo, o producer enchendo e o consumer drenando a
> fila. É a forma mais direta de *ver* a teoria da seção 3 acontecendo.

### 5.2 — venv e bibliotecas

Crie um ambiente virtual **na pasta deste tutorial** e instale as três bibliotecas: `pika`
(cliente RabbitMQ/AMQP), `boto3` (cliente S3) e `pyarrow` (escrita de Parquet):

```bash
cd tutoriais/streaming/2-filas/local
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install pika boto3 pyarrow
```

> **Fundamento (por que um `venv` por tutorial):** um *virtual environment* isola as dependências
> deste tutorial das do sistema e dos outros tutoriais. `python3 -m venv .venv` cria a pasta
> `.venv/` com um Python próprio; `source .venv/bin/activate` faz o shell passar a usar **esse**
> Python e **esse** `pip`; a partir daí, `pip install` grava dentro do `.venv/` — nada vaza para o
> sistema. As três libs têm papéis bem separados: **`pika`** fala AMQP com o RabbitMQ, **`boto3`**
> fala S3 com o MiniStack e **`pyarrow`** serializa os dados em Parquet.

> No **Windows (PowerShell)**, a ativação é `.venv\Scripts\Activate.ps1`. Se aparecer erro de
> política de execução, rode antes:
> `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`.

---

## 6. O producer (Python → RabbitMQ)

Crie o arquivo **`producer.py`** (na pasta deste tutorial). Ele gera eventos de venda
sinteticamente e publica na fila `vendas-fila`. Repare em três pontos: `queue_declare` com
`durable=True` (fila persistente), `delivery_mode=2` (mensagem persistente) e o `EPS` (eventos
por segundo) que você pode passar na linha de comando.

```python
import json, random, time, uuid, sys
from datetime import datetime, timezone
import pika

QUEUE = "vendas-fila"
EPS = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0

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
            "data_venda": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")}

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
ch = conn.channel()
ch.queue_declare(queue=QUEUE, durable=True)
n = 0
try:
    while True:
        ev = gerar_evento()
        ch.basic_publish(exchange="", routing_key=QUEUE, body=json.dumps(ev),
                         properties=pika.BasicProperties(delivery_mode=2))
        n += 1
        if n % 10 == 0: print(f"{n} eventos enviados", flush=True)
        time.sleep(1.0/EPS)
except KeyboardInterrupt:
    pass
finally:
    conn.close(); print(f"total: {n}")
```

### Passo a passo, linha a linha (producer)

Vamos ler o `producer.py` **de cima para baixo**, cada linha ligada à teoria da seção 3.

- `import json, random, time, uuid, sys` — importa os utilitários da biblioteca padrão: `json`
  serializa o evento (dict → string), `random` sorteia produtos/quantidades, `time` cadencia o
  ritmo (`sleep`), `uuid` gera o `evento_id` único e `sys` lê os argumentos da linha de comando.
- `from datetime import datetime, timezone` — traz o relógio: `datetime.now(timezone.utc)` dá o
  instante **em UTC** para carimbar a `data_venda`.
- `import pika` — o cliente **AMQP**; é a lib que fala com o RabbitMQ.
- `QUEUE = "vendas-fila"` — o **nome da fila**. Precisa ser **idêntico** no producer e no consumer;
  como usamos o *default exchange*, esse nome também é a **routing key** (seção 3.2).
- `EPS = float(sys.argv[1]) if len(sys.argv) > 1 else 5.0` — *events per second*. Lê o **1º
  argumento** da linha de comando (`python producer.py 20` → 20/s); se você não passar nada, cai no
  **padrão 5.0**. É o controle da **taxa de produção** (a "vazão" da seção 3.1).
- `CATALOGO = { 1: ("Notebook","Eletronicos",3500.00), ... }` — um mini-catálogo fixo: `id →
  (nome, categoria, preço)`. Serve para gerar eventos **realistas e consistentes** (a categoria e o
  preço sempre batem com o produto), sem depender de banco.
- `def gerar_evento():` — a fábrica de um evento de venda.
  - `pid = random.choice(list(CATALOGO)); _, cat, preco = CATALOGO[pid]; qtd = random.randint(1,5)`
    — três passos numa linha (separados por `;`): sorteia um **id de produto** (`pid`); desempacota
    a tupla do catálogo, jogando fora o nome (`_`) e guardando **categoria** e **preço**; sorteia a
    **quantidade** de 1 a 5.
  - `return {"evento_id": str(uuid.uuid4()), ...}` — monta o **dict do evento** com as 7 chaves do
    contrato. O `evento_id` é um **UUID v4** (praticamente único) — é a chave que torna a
    **deduplicação idempotente** possível a jusante (seção 3.3). `valor_total` é `round(preco*qtd,
    2)`. A `data_venda` usa `datetime.now(timezone.utc).replace(tzinfo=None).isoformat(...)`:
    calcula o instante em UTC e **remove o fuso** (`replace(tzinfo=None)`) para gerar uma string ISO
    "ingênua" (sem `+00:00`), com precisão de **milissegundos** — o **mesmo formato** dos Tut. 3/4.
- `conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))` — abre **a connection**
  (a conexão TCP) com o broker em `localhost`, porta AMQP **5672** (o padrão, por isso não aparece).
  `BlockingConnection` = API síncrona, simples de ler.
- `ch = conn.channel()` — abre **o channel** dentro da connection. É por `ch` que passam todos os
  comandos AMQP (declarar fila, publicar). Connection cara, channel barato (seção 3.2).
- `ch.queue_declare(queue=QUEUE, durable=True)` — **declara** a fila. A operação é **idempotente**:
  cria se não existir, não faz nada se já existir (desde que os parâmetros batam). `durable=True`
  torna a **fila persistente** — sobrevive a restart do broker (seção 3.5). Declarar dos **dois**
  lados garante que a fila exista independentemente de quem sobe primeiro.
- `n = 0` — contador de eventos enviados (só para o log de progresso).
- `try:` / `while True:` — o **laço infinito** do fluxo contínuo. É *streaming*: publica sem parar
  até você interromper.
  - `ev = gerar_evento()` — cria o próximo evento.
  - `ch.basic_publish(exchange="", routing_key=QUEUE, body=json.dumps(ev), properties=pika.BasicProperties(delivery_mode=2))`
    — **publica** a mensagem. `exchange=""` é o **default exchange** (seção 3.2), que entrega direto
    na fila cujo nome = `routing_key` = `"vendas-fila"`. `body=json.dumps(ev)` serializa o dict em
    **string JSON** (o corpo da mensagem é bytes/texto). `delivery_mode=2` marca a mensagem como
    **persistente** — o broker a grava em disco (seção 3.5).
  - `n += 1` — soma um ao contador.
  - `if n % 10 == 0: print(f"{n} eventos enviados", flush=True)` — a cada 10 eventos, imprime o
    progresso. `flush=True` força a saída na hora (senão o Python bufferiza e você não vê o log
    fluindo).
  - `time.sleep(1.0/EPS)` — **espaça** os envios para atingir a taxa `EPS`. Com `EPS=5`, dorme
    `0.2s` entre eventos (~5/s). É o que transforma o laço num fluxo **cadenciado**, não numa rajada
    infinita.
- `except KeyboardInterrupt: pass` — captura o **Ctrl+C**; sai do laço sem stack trace.
- `finally: conn.close(); print(f"total: {n}")` — **sempre** roda ao final: fecha a connection
  (libera o socket TCP) e imprime o total enviado. Fechar a connection também fecha o channel.

Detalhes que importam:

| Trecho | Papel |
|---|---|
| `pika.ConnectionParameters("localhost")` | Conecta no RabbitMQ do Tutorial 1 (porta AMQP `5672`, o padrão). |
| `queue_declare(..., durable=True)` | Cria a fila `vendas-fila` se não existir (idempotente) e a torna **persistente**. |
| `basic_publish(exchange="", routing_key=QUEUE, ...)` | Publica no *default exchange*, que roteia direto para a fila de mesmo nome. |
| `delivery_mode=2` | Marca a mensagem como **persistente** (grava em disco). |
| `data_venda` sem fuso | Mantém o **mesmo formato** ISO dos outros tutoriais (aqui não é usado para janelar, mas o contrato é o mesmo). |

Rode-o em um terminal e **deixe rodando** (é o fluxo contínuo):

```bash
python producer.py          # 5 eventos/s (padrão)
# python producer.py 20     # ou 20 eventos/s
```

**Resultado esperado**: linhas `10 eventos enviados`, `20 eventos enviados`, ...

> Abra a UI <http://localhost:15672> → aba **Queues** → `vendas-fila`. Você verá o número de
> mensagens **Ready** subindo (ainda sem consumer) e o gráfico de **publish rate**.

---

## 7. O consumer (RabbitMQ → micro-lote → Parquet)

Crie o arquivo **`consumer.py`** (na mesma pasta). Ele lê mensagem a mensagem, dá `ack`, acumula
um micro-lote e faz flush em Parquet no MiniStack. **Leia os comentários** — cada um marca uma
decisão importante.

```python
import json, io, time, uuid
from datetime import datetime, timezone
import pika, boto3
import pyarrow as pa, pyarrow.parquet as pq
from botocore.config import Config

QUEUE = "vendas-fila"
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "datalake"
BATCH = 50           # tamanho do micro-lote
FLUSH_SECS = 15      # ou faz flush a cada 15s, o que vier primeiro

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                  aws_access_key_id="test", aws_secret_access_key="test",
                  region_name="us-east-1",
                  config=Config(request_checksum_calculation="when_required"))

buffer = []
last_flush = time.time()

def flush():
    global buffer, last_flush
    if not buffer:
        return
    table = pa.Table.from_pylist(buffer)
    sink = io.BytesIO(); pq.write_table(table, sink)
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"filas/dt={dt}/lote-{uuid.uuid4().hex[:8]}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=sink.getvalue())
    print(f"flush {len(buffer)} eventos -> s3://{BUCKET}/{key}", flush=True)
    buffer = []; last_flush = time.time()

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
ch = conn.channel()
ch.queue_declare(queue=QUEUE, durable=True)
ch.basic_qos(prefetch_count=BATCH)
try:
    for method, props, body in ch.consume(QUEUE, inactivity_timeout=1):
        if body is not None:
            buffer.append(json.loads(body))
            ch.basic_ack(method.delivery_tag)
        if len(buffer) >= BATCH or (buffer and time.time() - last_flush >= FLUSH_SECS):
            flush()
except KeyboardInterrupt:
    flush()
finally:
    conn.close()
```

### Passo a passo, linha a linha (consumer)

Este é o coração do tutorial. Leia devagar — cada bloco liga a uma parte da seção 3.

**Imports e configuração:**

- `import json, io, time, uuid` — `json` desserializa o corpo da mensagem (string → dict); `io`
  fornece o `BytesIO` (buffer de bytes **em memória**); `time` mede o relógio do flush por tempo;
  `uuid` gera o sufixo aleatório do nome do arquivo.
- `from datetime import datetime, timezone` — para montar a partição `dt=YYYY-MM-DD` em **UTC**.
- `import pika, boto3` — `pika` fala AMQP com o RabbitMQ; `boto3` fala S3 com o MiniStack.
- `import pyarrow as pa, pyarrow.parquet as pq` — `pa` monta a **tabela Arrow** (colunar em
  memória); `pq` a serializa em **Parquet**.
- `from botocore.config import Config` — traz o objeto de configuração do boto3, necessário para o
  **gotcha do checksum** (adiante).
- `QUEUE = "vendas-fila"` — mesmo nome do producer (tem de bater exatamente).
- `S3_ENDPOINT = "http://localhost:4566"` — o endereço do **MiniStack** (S3 local). Trocar esta
  linha (e as credenciais) pelo endpoint real da AWS é **tudo** que muda para rodar na nuvem.
- `BUCKET = "datalake"` — o bucket de destino (criado pelo Tutorial 1).
- `BATCH = 50` — tamanho do micro-lote: fecha o lote ao juntar 50 mensagens (seção 3.6).
- `FLUSH_SECS = 15` — teto de latência: fecha o lote se passarem 15s com algo no buffer (seção 3.6).

**Cliente S3:**

- `s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id="test",
  aws_secret_access_key="test", region_name="us-east-1", config=Config(request_checksum_calculation="when_required"))`
  — cria o **cliente S3**. `endpoint_url` **redireciona** o boto3 do endereço da AWS para o
  MiniStack local. As credenciais são **dummy** (`"test"`/`"test"`) porque o MiniStack não valida
  identidade — mas o boto3 **exige** que existam. `region_name` idem: o SDK exige uma região. O
  `config=Config(...)` é o **gotcha** explicado no bloco de teoria abaixo.

> **Fundamento (boto3, `put_object` e o gotcha do checksum):** o `boto3` é o SDK da AWS; um
> **cliente** S3 é um objeto que traduz chamadas Python em requisições HTTP para o serviço. As
> versões recentes do boto3 passaram a calcular, por padrão, um **checksum CRC64NVME** de cada
> objeto no `put_object` e a enviá-lo num header, para o S3 validar a integridade. O problema:
> **o MiniStack (S3 emulado) não implementa o CRC64NVME** e **rejeita** a requisição. O
> `Config(request_checksum_calculation="when_required")` diz ao boto3 "só calcule checksum quando a
> operação **realmente exigir**" — e `put_object` não exige. Resultado: o upload passa no MiniStack.
> **No S3 real da AWS isso não é necessário** (o serviço aceita o CRC64NVME). O mesmo ajuste
> aparece, no shell, como `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` na seção 9.

**Estado do buffer:**

- `buffer = []` — a **lista em memória** que acumula os eventos do micro-lote atual.
- `last_flush = time.time()` — marca **quando** foi o último flush; é a referência para o gatilho de
  tempo (`agora - last_flush >= FLUSH_SECS`).

**A função `flush()`:**

- `def flush():` — grava o buffer atual como **um** Parquet e zera o estado.
- `global buffer, last_flush` — declara que a função vai **reatribuir** essas variáveis do escopo de
  módulo (sem `global`, `buffer = []` criaria uma variável local e o buffer real nunca esvaziaria).
- `if not buffer: return` — **guarda**: se o buffer está vazio, não faz nada (evita gravar arquivo
  vazio quando o gatilho de tempo dispara sem mensagens).
- `table = pa.Table.from_pylist(buffer)` — converte a **lista de dicts** numa **tabela Arrow
  colunar**. O pyarrow **infere o schema** a partir das chaves (as 7 colunas do evento).
- `sink = io.BytesIO(); pq.write_table(table, sink)` — cria um **buffer de bytes em memória** e
  escreve o Parquet **nele** (não em arquivo de disco). Ao final, `sink` contém o arquivo Parquet
  inteiro em RAM.
- `dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")` — a data **UTC** de hoje, formato
  `YYYY-MM-DD`, para a partição.
- `key = f"filas/dt={dt}/lote-{uuid.uuid4().hex[:8]}.parquet"` — monta a **chave** (o "caminho") do
  objeto no S3: prefixo `filas/`, partição `dt=.../` e nome `lote-` + **8 hex aleatórios** (evita
  colisão entre flushes no mesmo segundo).
- `s3.put_object(Bucket=BUCKET, Key=key, Body=sink.getvalue())` — **envia** os bytes do Parquet
  (`sink.getvalue()`) ao S3, no bucket e na chave montados. É aqui que o dado chega ao data lake.
- `print(f"flush {len(buffer)} eventos -> s3://{BUCKET}/{key}", flush=True)` — loga quantos eventos
  foram gravados e onde. `flush=True` para ver na hora.
- `buffer = []; last_flush = time.time()` — **zera** o buffer e **reinicia** o cronômetro do flush
  por tempo. O ciclo recomeça do zero.

> **Fundamento (pyarrow e Parquet, e por que gravar em memória):** o **Parquet** é um formato
> **colunar**: em vez de guardar linha a linha, guarda **coluna a coluna**, o que permite
> compressão muito melhor (valores parecidos ficam juntos) e leitura seletiva de colunas — por isso
> é o padrão de data lake. O **pyarrow** é a lib que constrói tabelas Arrow (representação colunar
> em memória) e as escreve em Parquet. `Table.from_pylist(buffer)` transforma a lista de dicts em
> tabela; `write_table(table, sink)` a serializa. Escrever para um **`io.BytesIO`** (e não para um
> caminho de disco) mantém tudo **em RAM** e entrega os bytes direto ao `put_object` — sem arquivo
> temporário, sem I/O de disco, um único upload. É o casamento natural entre "montei um lote" e
> "subi um objeto".

**Conexão e laço de consumo:**

- `conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))` — abre a connection com o
  broker (igual ao producer).
- `ch = conn.channel()` — abre o channel.
- `ch.queue_declare(queue=QUEUE, durable=True)` — declara a mesma fila durável (idempotente). Se o
  consumer subir antes do producer, a fila já passa a existir.
- `ch.basic_qos(prefetch_count=BATCH)` — aplica o **QoS/prefetch** = 50: o broker entrega no máximo
  50 mensagens **não confirmadas** por vez (seção 3.4). Casa com o `BATCH` e aplica backpressure.
- `try:` / `for method, props, body in ch.consume(QUEUE, inactivity_timeout=1):` — o **laço de
  consumo** no modelo **pull**. `ch.consume(...)` devolve uma tupla `(method, props, body)` por
  iteração. O `inactivity_timeout=1` é a peça-chave: quando **não há mensagem** por 1 segundo, o
  laço **acorda mesmo assim** com `body=None` — é isso que permite verificar o **flush por tempo**
  mesmo com a fila vazia (seção 3.6). `method` carrega o `delivery_tag` (id da entrega, para o
  `ack`); `props` são as propriedades da mensagem; `body` é o corpo (bytes JSON) ou `None`.
  - `if body is not None:` — só processa quando **veio** uma mensagem (ignora os "despertares" por
    timeout).
    - `buffer.append(json.loads(body))` — **desserializa** o JSON de volta para dict e o coloca no
      **buffer**. A mensagem agora está **guardada em memória**.
    - `ch.basic_ack(method.delivery_tag)` — **confirma** a mensagem, **depois** de bufferizar. Aqui
      está a semântica **at-least-once** (seção 3.3): o broker só apaga a mensagem agora; se o
      processo tivesse caído entre o `append` e o `ack`, o RabbitMQ a **reentregaria**.
  - `if len(buffer) >= BATCH or (buffer and time.time() - last_flush >= FLUSH_SECS): flush()` — o
    **gatilho duplo** do micro-lote: dispara o `flush()` se o buffer atingiu `BATCH` **ou** se há
    algo no buffer e já passaram `FLUSH_SECS` desde o último flush. O `buffer and ...` garante que o
    gatilho de tempo **só** dispara com algo para gravar (não cria arquivo vazio).
- `except KeyboardInterrupt: flush()` — no **Ctrl+C**, faz um **último flush**: grava o que sobrou no
  buffer antes de sair, para não perder o lote parcial pendente.
- `finally: conn.close()` — **sempre** fecha a connection ao final, liberando o socket.

### Como o micro-lote funciona

- **`BATCH = 50` / `FLUSH_SECS = 15`**: o consumer grava quando o buffer chega a 50 mensagens
  **ou** quando passam 15 segundos desde o último flush **e** há algo no buffer. Assim, mesmo com
  a fila lenta, os dados não ficam presos indefinidamente na memória.
- **`basic_qos(prefetch_count=BATCH)`**: o broker entrega no máximo `BATCH` mensagens ainda não
  confirmadas por vez. Isso casa com o tamanho do lote e evita puxar a fila inteira de uma vez.
- **`ch.consume(QUEUE, inactivity_timeout=1)`**: é um laço *pull* que devolve `(method, props,
  body)`. Quando a fila está vazia, o `inactivity_timeout=1` faz o laço "acordar" a cada 1s com
  `body = None` — isso é o que permite o **flush por tempo** mesmo sem novas mensagens chegando.
- **`ch.basic_ack(method.delivery_tag)`**: confirma a mensagem **depois** de colocá-la no buffer.
  É o `ack` que garante a semântica **at-least-once**: se o consumer cair antes do `ack`, o
  RabbitMQ reentrega a mensagem a outro consumer.
- **`Config(request_checksum_calculation="when_required")`**: o **gotcha do checksum**. Sem ele,
  o boto3 tenta o algoritmo **CRC64NVME** no `put_object` e o **MiniStack recusa**. No S3 real
  isso não é necessário.
- **`pa.Table.from_pylist(buffer)` + `pq.write_table(..., sink)`**: monta a tabela Arrow com as
  7 colunas do evento e serializa em Parquet **em memória** (`io.BytesIO`), sem tocar o disco —
  o `put_object` envia os bytes direto ao S3.

---

## 8. Rodando o consumer

Com o **producer rodando** no primeiro terminal, abra um **segundo terminal**, ative o mesmo
venv e rode o consumer:

```bash
cd tutoriais/streaming/2-filas/local
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
python consumer.py
```

**Resultado esperado**: a cada micro-lote fechado, uma linha como:

```
flush 50 eventos -> s3://datalake/filas/dt=2026-07-02/lote-a1b2c3d4.parquet
flush 50 eventos -> s3://datalake/filas/dt=2026-07-02/lote-9f8e7d6c.parquet
```

> Se você tinha deixado o producer acumular mensagens antes de ligar o consumer, os primeiros
> flushes saem em rajada (o consumer drena a fila em lotes de 50). Depois, o ritmo acompanha o
> `EPS` do producer.
>
> Na UI <http://localhost:15672> → `vendas-fila`, veja a linha **Ready** cair para perto de 0 e
> o gráfico **ack rate** aparecer — prova visual do consumo com `ack`.

> **Por que importa (o que você está *vendo* acontecer):** quando o consumer sobe, o broker começa
> a entregar até `prefetch=50` mensagens; elas viram **Unacked** por um instante e, logo após o
> `basic_ack`, **somem** da fila (o `ack rate` sobe). Se você acumulou fila antes, os primeiros
> flushes saem **em rajada** de 50 em 50 (a curva de consumo é mais íngreme que a de produção) até
> zerar o atraso; depois, consumo e produção se equilibram no ritmo do `EPS`. É a teoria de
> desacoplamento na taxa (seção 3.1) visível em tempo real.

---

## 9. Verificando o resultado no S3

Em um **terceiro terminal**, aponte o AWS CLI para o MiniStack e liste os Parquets gravados:

```bash
# macOS / Linux
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required   # MiniStack não aceita CRC64NVME

aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/filas/ --recursive
```

```powershell
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="test"; $env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"; $env:AWS_REQUEST_CHECKSUM_CALCULATION="when_required"

aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/filas/ --recursive
```

**Resultado esperado**: um arquivo por flush, em `filas/dt=YYYY-MM-DD/`:

```
2026-07-02 20:52:10       4821 filas/dt=2026-07-02/lote-a1b2c3d4.parquet
2026-07-02 20:52:19       4830 filas/dt=2026-07-02/lote-9f8e7d6c.parquet
```

Baixe um lote e leia com o `pyarrow` (do venv). Vamos conferir as **7 colunas do evento** e o
número de linhas do lote:

```bash
# baixa o primeiro lote do dia de hoje
aws --endpoint-url http://localhost:4566 s3 cp \
  "$(aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/filas/dt=$(date -u +%F)/ | awk '{print $4}' | head -1 | sed 's#^#s3://datalake/filas/dt='$(date -u +%F)'/#')" /tmp/lote.parquet

python -c "import pyarrow.parquet as pq; t = pq.read_table('/tmp/lote.parquet'); print('colunas:', t.column_names); print('linhas:', t.num_rows); print(t.slice(0,2).to_pylist())"
```

**Resultado esperado** (7 colunas, 50 linhas por lote):

```
colunas: ['evento_id', 'cliente_id', 'produto_id', 'categoria', 'quantidade', 'valor_total', 'data_venda']
linhas: 50
[{'evento_id': 'a1b2c3d4-...', 'cliente_id': 7, 'produto_id': 3, 'categoria': 'Moveis',
  'quantidade': 2, 'valor_total': 900.0, 'data_venda': '2026-07-02T20:51:59.337'}, ...]
```

Cada lote tem exatamente as **7 colunas do evento** e **50 linhas** (o `BATCH`) — a não ser o
último lote de uma rajada, que pode sair menor por causa do flush por tempo (`FLUSH_SECS`).

> **Por que importa (lendo o resultado como prova da teoria):** os dois números confirmam duas
> decisões de projeto. **7 colunas** = o schema que o pyarrow inferiu do dict — o **contrato do
> evento** chegou intacto ao lake. **50 linhas** = o `BATCH` fechou o lote por **contagem**; um lote
> com *menos* de 50 é a assinatura do flush por **tempo** (`FLUSH_SECS`) — a garantia de latência
> máxima agindo (seção 3.6). Ver poucos arquivos gordos, e não milhares de minúsculos, é o
> "small files problem" resolvido na prática.

---

## 10. Parando e limpando

1. **Pare o consumer** com `Ctrl+C` no terminal dele. O bloco `except KeyboardInterrupt: flush()`
   garante que o **último buffer** ainda pendente seja gravado antes de sair.
2. **Pare o producer** com `Ctrl+C` no terminal dele (imprime o `total:`).
3. **Esvazie a fila** (opcional). Se sobraram mensagens não consumidas, você pode purgar a fila
   pela UI (<http://localhost:15672> → `vendas-fila` → **Purge Messages**) ou pela CLI do broker:

   ```bash
   docker exec streaming_rabbitmq rabbitmqctl purge_queue vendas-fila
   ```

4. Os containers do Tutorial 1 podem **continuar rodando** — você vai usá-los nos Tutoriais 3 e
   4. Para derrubar tudo, volte ao `1-infraestrutura/local` e rode `docker compose down -v`.

> **Por que importa (a ordem de parada não é acidental):** paramos o **consumer primeiro** de
> propósito. O `except KeyboardInterrupt: flush()` grava o **lote parcial** que ainda estava na
> memória — sem ele, você perderia os eventos já `ack`ados mas ainda não persistidos. Se sobrarem
> mensagens *Ready* na fila (o producer publicou mais do que o consumer drenou), elas continuam lá,
> **duráveis**, esperando um consumer futuro — é o desacoplamento no tempo (seção 3.1). O
> `purge_queue` só é necessário quando você quer **descartar** esse trabalho pendente e começar
> limpo. E `docker compose down -v` remove os **volumes**: aí sim a fila durável e os objetos do
> lake somem de vez.

---

## 11. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `pika ... Connection refused` a `localhost:5672` | RabbitMQ (Tutorial 1) parado | Suba o Tutorial 1 (`docker compose up -d`); confira `streaming_rabbitmq` em `docker compose ps` |
| `... CRC64NVME` / erro de checksum no `put_object` | Faltou o `Config` do boto3 | Use `Config(request_checksum_calculation="when_required")` no `boto3.client(...)` |
| `NoSuchBucket` / erro ao gravar | Bucket `datalake` não existe | Confira o init do MiniStack (Tut. 1); teste com `aws --endpoint-url http://localhost:4566 s3 ls` |
| Fila não aparece / consumer não recebe nada | `queue_declare` não rodou / nome errado | Garanta `queue_declare(queue="vendas-fila", durable=True)` nos **dois** scripts; confira o nome na UI |
| **Nada aparece no S3** | Buffer não atingiu `BATCH` nem `FLUSH_SECS` | Deixe o producer rodando; espere > 15s (flush por tempo) ou baixe o `EPS` para ver lotes fechando |
| Consumo lento / broker "segura" mensagens | `prefetch` baixo demais | Ajuste `basic_qos(prefetch_count=BATCH)` para casar com o tamanho do lote |
| Mensagens reaparecem após reiniciar o consumer | Consumer caiu **antes** do `ack` (at-least-once) | Comportamento esperado: o RabbitMQ reentrega. Pode gerar duplicatas — é a semântica *at-least-once* |
| Muitos arquivos minúsculos no S3 | `BATCH` muito pequeno ou `FLUSH_SECS` muito curto | Aumente `BATCH`/`FLUSH_SECS`; um flush = um arquivo |

---

**Pronto!** Você fez streaming por **fila**: consumo unitário com `ack` (at-least-once),
micro-lote em memória e gravação em **Parquet** no data lake. Compare agora com os Tutoriais **3
(Kafka + Spark)** e **4 (Kafka + Flink)**, que usam **tópico** e **janela de 30s por event-time**
— o outro grande paradigma de streaming, sobre o **mesmo evento**.

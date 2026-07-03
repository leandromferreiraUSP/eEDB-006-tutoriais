# Tutorial 2 (AWS): Streaming por Filas com SQS + Lambda + S3

> Versão **longa e explicativa**. Você vai rodar um **producer** na sua máquina (Python + boto3)
> que envia **eventos de venda** para uma fila **SQS**; a AWS invoca automaticamente uma função
> **Lambda** com um **micro-lote** de mensagens, e a Lambda grava esse lote em **Parquet** no
> **S3**. É o **espelho na nuvem** do Tutorial 2 local (RabbitMQ + Python).
>
> **Pré-requisito**: ter feito o `1-infraestrutura/aws` (SQS + Lambda + S3 provisionados via
> Terraform). Aqui você **não cria infra** — só roda o producer e observa o fluxo. Só os
> comandos? Veja `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Objetivo técnico e lógico](#1-objetivo-técnico-e-lógico)
2. [Decisões de projeto (e por quê)](#2-decisões-de-projeto-e-por-quê)
3. [Conceitos-chave: serverless, event-driven e event source mapping](#3-conceitos-chave-serverless-event-driven-e-event-source-mapping)
4. [Arquitetura na AWS](#4-arquitetura-na-aws)
5. [Pré-requisitos](#5-pré-requisitos)
6. [Relembrando o consumer (a Lambda)](#6-relembrando-o-consumer-a-lambda)
7. [O producer (você cria)](#7-o-producer-você-cria)
8. [Observando o fluxo](#8-observando-o-fluxo)
9. [Verificando no S3](#9-verificando-no-s3)
10. [Custos e limpeza](#10-custos-e-limpeza)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Objetivo técnico e lógico

Este é o **Tutorial 2 (Filas)**, variante **AWS**. O paradigma é o de **fila**: cada evento é
uma **mensagem** que fica na fila até um consumidor pegá-la, processá-la e removê-la. Não há
janela de tempo nem semântica de event-time (isso é dos Tutoriais 3 e 4, com Kafka); aqui o
agrupamento é **micro-lote** — o consumidor processa um punhado de mensagens de cada vez e grava
um arquivo.

Na nuvem, tudo é **serverless** (sem servidor para gerenciar): você **não** sobe Kafka, Spark
nem RabbitMQ. O transporte é o **SQS**, o consumidor é uma **Lambda** e o destino é o **S3**:

```
producer (sua máquina) ──► SQS (vendas-queue) ──► Lambda (vendas-consumer, micro-lote) ──► S3 (Parquet)
```

É o **espelho na nuvem** do Tutorial 2 local (RabbitMQ + Python): mesma ideia (fila +
processamento em micro-lote em Parquet), só que **gerenciada pela AWS**. A diferença chave é que
lá **você** escreve o loop de consumo (`basic_consume` + buffer); aqui a AWS **invoca a Lambda
por você**, já com o lote pronto, através do *event source mapping*.

> **Fundamento — pull vs. push (quem chama quem?):** No Tutorial 2 **local**, o seu consumidor faz
> *pull*: você escreve um loop (`basic_consume` + um buffer) que **puxa** mensagens do RabbitMQ,
> conta até formar um lote e só então grava. Aqui o modelo é *push*, **event-driven**: **você não
> escreve loop de consumo nenhum**. Quem puxa do SQS e decide a hora de agir é a própria AWS — o
> *event source mapping* faz o *polling*, monta o lote e **invoca** (empurra para) a sua Lambda já
> com as mensagens prontas em `event["Records"]`. Você programa só a **reação** a um lote; o
> *quando* e o *quanto* passam a ser responsabilidade da plataforma.

Neste tutorial você **não provisiona nada** — a infra já veio do Tutorial 1 (AWS). Seu trabalho
é: escrever e rodar o **producer**, e depois **observar e validar** o fluxo chegando em Parquet
no S3.

> **Por que importa:** esse é o coração da mentalidade *serverless*. Em vez de manter um processo
> vivo esperando mensagens (que **você** teria de hospedar, monitorar e reiniciar), você entrega
> uma **função** e um **gatilho**. Sem tráfego, nada roda e nada custa; com tráfego, a AWS cria
> quantas execuções forem necessárias. A unidade de escala e de faturamento deixa de ser "um
> servidor ligado 24/7" e passa a ser "uma invocação de alguns milissegundos". Guarde esse
> contraste: ele reaparece em cada passo deste tutorial.

---

## 2. Decisões de projeto (e por quê)

| Decisão | O que escolhemos | Por quê |
|---|---|---|
| **Serverless** (SQS + Lambda) | Sem EC2, sem servidor, sem broker para operar | Barato, escala sozinho e é sempre liberado no Learner Lab (Kafka gerenciado/MSK e Spark/Flink gerenciados costumam ser bloqueados ou caros). |
| **Micro-lote via *event source mapping*** | `batch_size=100` + janela (`maximum_batching_window`) de 30s | Cada invocação da Lambda recebe **até 100 mensagens** (ou o que acumulou em 30s) e grava **1 Parquet**. É o "micro-lote" — sem você escrever loop de consumo nem manter buffer. |
| **Lambda sem estado** | Cada invocação é independente e grava seu próprio arquivo | A função não guarda nada entre invocações; o "estado" (quais mensagens formam o lote) é gerenciado pelo SQS + mapping. Nomear o arquivo com o `aws_request_id` garante 1 arquivo único por invocação. |
| **Parquet via layer gerenciada** | Layer "AWS SDK for pandas" (pandas + pyarrow + awswrangler) | Empacotar pyarrow no zip da Lambda é grande e chato; a layer pública resolve e mantém o deploy pequeno. |
| **Fila na AWS espelha o RabbitMQ local** | Mesmo evento, mesmo destino (Parquet), mesmo paradigma (fila → micro-lote) | Permite comparar "fila local" vs. "fila gerenciada" com o **mesmo dado**. O que muda é só quem opera o broker e o consumidor: você (RabbitMQ) vs. a AWS (SQS + Lambda). |
| **Você cria só o producer** | O consumidor (`handler.py`) você já criou no Tutorial 1 AWS | Regra do curso: a infra e o código do consumidor vêm do Tutorial 1; aqui o novo é apenas o **producer** que alimenta a fila. |

As duas primeiras decisões merecem um aprofundamento, porque são elas que dão o "sabor" deste
tutorial (e o diferenciam do Tutorial 2 local):

> **Teoria — o trade-off do micro-lote (latência × custo × tamanho de arquivo):** `batch_size=100`
> e `maximum_batching_window=30s` são os dois botões que definem o micro-lote. Aumentar a janela
> agrupa **mais** mensagens por invocação → **menos invocações** (mais barato) e **arquivos Parquet
> maiores** (melhor para ler depois), ao custo de **mais latência** (no pior caso, um evento espera
> até 30s antes de ser gravado). Diminuir a janela faz o oposto: mais "tempo real", porém mais
> invocações e uma enxurrada de arquivinhos (o clássico *small files problem*, que deixa a leitura
> analítica lenta). Não existe valor universalmente certo — existe o ponto de equilíbrio do seu
> caso. Para um curso, `100`/`30s` produz lotes visíveis sem esperar demais.

> **Por que "Lambda sem estado" importa na prática:** como cada invocação é isolada e pode rodar
> **em paralelo** com outras, a função **não pode** assumir que "viu" mensagens anteriores nem
> escrever no mesmo arquivo que outra invocação. Por isso o nome do arquivo carrega o
> `aws_request_id` (único por invocação): duas invocações simultâneas geram `lote-<id_A>.parquet` e
> `lote-<id_B>.parquet` sem colidir. Se um estado compartilhado fosse necessário, ele teria de
> morar **fora** da função — no próprio S3, num DynamoDB, etc. — nunca em variáveis da Lambda.

---

## 3. Conceitos-chave: serverless, event-driven e event source mapping

Antes de rodar qualquer coisa, vale entender **quem faz o quê** neste desenho. No Tutorial 2 local
você era dono do consumidor inteiro; aqui, boa parte do trabalho é da AWS, e saber **onde** ela
age evita confusão na hora de observar e de depurar. Esta seção é puramente conceitual — nenhum
comando para rodar, só o modelo mental que sustenta tudo o que vem depois.

### 3.1 — Serverless: você entrega a função, a AWS entrega a execução

> **Fundamento:** "serverless" não quer dizer "sem servidor" — quer dizer "sem servidor **para
> você** operar". A Lambda ainda roda em máquinas reais, mas quem as provisiona, escala, corrige e
> desliga é a AWS. Você entrega duas coisas: (1) um **código** (o `handler.py`) e (2) um **gatilho**
> (o *event source mapping* apontando para a fila). O resto — subir a execução quando chega
> trabalho, derrubá-la quando acaba — é da plataforma.

> **Por que importa:** o contraste com o RabbitMQ local é direto. Lá, se você fechar o terminal do
> consumidor, o consumo **para** (o processo morreu). Aqui não há processo seu "ligado": a Lambda
> só existe durante os milissegundos de cada invocação. É por isso que, ao parar o producer, o
> custo cai para ~zero **sem você desligar nada**.

### 3.2 — Event source mapping por dentro

O *event source mapping* (ESM) é o componente que **liga** a fila SQS à Lambda. Ele não é código
seu — é configuração (feita no Tutorial 1 AWS, via Terraform). Por dentro, ele roda um laço
contínuo, operado pela AWS, mais ou menos assim:

1. **Polling:** o ESM chama `ReceiveMessage` no SQS repetidamente (*long polling*), buscando
   mensagens disponíveis.
2. **Formação do lote:** acumula mensagens até atingir `batch_size` (**100**) **ou** até fechar a
   janela `maximum_batching_window` (**30s**) — o que vier primeiro.
3. **Invocação:** chama a sua Lambda **uma vez**, passando o lote inteiro em `event["Records"]`
   (cada `Record` traz o `body` — o seu JSON — mais metadados como `messageId` e `receiptHandle`).
4. **Confirmação (o "ack" automático):** se o handler retorna **sem erro**, o ESM **apaga** as
   mensagens daquele lote do SQS (`DeleteMessage`). Se o handler **lança exceção**, ele **não**
   apaga — as mensagens voltam a ficar visíveis e serão **reentregues**.

> **Teoria — é o ESM que substitui o seu `basic_consume`:** no local, **você** escrevia o loop de
> receber + bufferizar + confirmar (`ack`). Os quatro passos acima são exatamente esse loop, só que
> implementado e operado pela AWS. Você "só" fornece o corpo da função que roda no passo 3.

> **Por que importa — reentrega e idempotência:** como um lote com erro é **reentregue**, a mesma
> mensagem pode ser processada mais de uma vez (garantia *at-least-once* do SQS). Neste tutorial
> isso é inofensivo (reprocessar um lote só regrava um Parquet equivalente), mas em produção é
> justamente o motivo de projetar consumidores **idempotentes** — que produzem o mesmo resultado se
> rodarem duas vezes com o mesmo dado.

**Partial batch failure (conceito).** Por padrão, se **uma** mensagem do lote falha, o handler
lança exceção e o **lote inteiro** é reentregue — inclusive as que já tinham dado certo. Para não
reprocessar tudo, a Lambda pode responder com `batchItemFailures` (o recurso
`ReportBatchItemFailures`), devolvendo **só os `messageId` que falharam**; o ESM então reentrega
apenas esses. Nosso handler não usa isso (o lote é tudo-ou-nada), mas é bom saber que a opção
existe — é a diferença entre "reprocessar 1 mensagem ruim" e "reprocessar as 100".

### 3.3 — Escala, concorrência e cold start

> **Fundamento — escala horizontal automática:** se a fila enche, o ESM **não** manda um lote
> gigante para uma única Lambda; ele abre **várias invocações concorrentes**, cada uma com seu
> próprio lote de até 100. É assim que o serverless "acompanha" picos: mais trabalho ⇒ mais
> execuções em paralelo, sem você tocar em nada. (Há um teto de concorrência por conta; no Learner
> Lab ele é mais do que suficiente para este volume.)

> **Por que 1 arquivo por invocação:** como podem existir N invocações ao mesmo tempo, cada uma
> **stateless** e sem saber das outras, cada invocação grava **o seu** arquivo, nomeado com o
> `aws_request_id`. Isso garante unicidade **sem coordenação** — nenhuma invocação precisa
> "combinar" nome com outra. É a razão de você ver **vários** `.parquet` no S3, um por lote.

> **Teoria — cold start:** a primeira invocação depois de um tempo ocioso paga um *cold start*: a
> AWS precisa criar o ambiente, baixar a *layer* (pandas/pyarrow) e inicializar o runtime — alguns
> segundos a mais. As invocações seguintes reaproveitam o ambiente "quente" e são bem mais rápidas.
> Se o **primeiro** lote demorar um pouco mais para aparecer no log, normalmente é isto.

### 3.4 — Visibility timeout × timeout da Lambda

Quando o ESM entrega um lote à Lambda, o SQS torna aquelas mensagens **invisíveis** por um período
— o *visibility timeout* — para que **nenhum outro** consumidor as pegue enquanto a invocação roda.

> **Por que importa — a regra prática:** o *visibility timeout* da fila deve ser **maior** que o
> *timeout* da Lambda (a AWS recomenda ~6×). Se a Lambda demora mais que a invisibilidade, o SQS
> "acha" que a mensagem se perdeu e a **reentrega** para outra invocação — que reprocessa o mesmo
> lote enquanto a primeira ainda trabalha (processamento duplicado). Esses valores já vêm ajustados
> no Terraform do Tutorial 1; aqui você só precisa **reconhecer o sintoma** se ele aparecer.

Esses mesmos números invisíveis são o que você vai **ver** na seção 8, no atributo
`ApproximateNumberOfMessagesNotVisible`.

---

## 4. Arquitetura na AWS

```
            ┌──────────────────────── AWS (us-east-1) — Learner Lab ────────────────────┐
            │                                                                            │
 você ─────►│  ┌───────────────┐  msg   ┌──────────────┐  trigger  ┌──────────────────┐ │
producer.py │  │  SQS          │ ─────► │  Lambda      │ ────────► │  S3 bucket       │ │
 (boto3)    │  │  vendas-queue │        │ vendas-      │  Parquet  │  <conta>-        │ │
send_message│  └───────────────┘        │ consumer     │           │  streaming-lab   │ │
            │        ▲ event source      │ (micro-lote) │           │  filas/dt=.../   │ │
            │        └── mapping ────────┘ + layer      │           │  lote-<id>.parquet│ │
            │       batch_size=100         pandas/pyarrow           └──────────────────┘ │
            │       janela=30s                                                            │
            └────────────────────────────────────────────────────────────────────────────┘
```

O `producer.py` roda na **sua máquina** e só faz uma coisa: `sqs.send_message(...)` em loop. Todo
o resto — juntar as mensagens em lote, invocar a Lambda, ela gravar o Parquet — acontece **dentro
da AWS**, automaticamente.

---

## 5. Pré-requisitos

Você precisa de **três** coisas prontas: a infra do Tutorial 1 (AWS), as credenciais do Lab no
`~/.aws/` e o **Python 3.12** (para o producer).

### 5.1 — Infra provisionada (Tutorial 1 AWS)

Confirme que o `1-infraestrutura/aws` já foi aplicado (SQS `vendas-queue`, Lambda
`vendas-consumer` com a layer pandas e o *event source mapping*, e o bucket S3). Se ainda não fez,
faça-o antes.

### 5.2 — Credenciais do Learner Lab

As credenciais ficam em `tutoriais/aws_credenciais/` e são **temporárias** (expiram a cada sessão
do Lab). Copie-as para `~/.aws/`:

```bash
# macOS / Linux
mkdir -p ~/.aws
cp tutoriais/aws_credenciais/credentials ~/.aws/credentials
cp tutoriais/aws_credenciais/config      ~/.aws/config
chmod 600 ~/.aws/credentials
```

```powershell
# Windows (PowerShell)
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.aws" | Out-Null
$proj = "C:\caminho\para\Big Data\tutoriais\aws_credenciais"   # ajuste
Copy-Item "$proj\credentials" "$env:USERPROFILE\.aws\credentials" -Force
Copy-Item "$proj\config"      "$env:USERPROFILE\.aws\config" -Force
```

Valide a identidade:

```bash
aws sts get-caller-identity
```

**Resultado esperado**: um JSON com `Account` e um `Arn` contendo `assumed-role/voclabs/...`.

> **Fundamento — credenciais temporárias (assumed-role):** o Learner Lab não te dá um usuário IAM
> fixo; ele te dá uma **role assumida** via STS, com credenciais **temporárias** (uma
> `aws_access_key_id`, uma `aws_secret_access_key` e — importante — um `aws_session_token`). Por
> isso o `Arn` aparece como `assumed-role/voclabs/...` e por isso elas **expiram** em poucas horas.
> Quando isso acontece, qualquer chamada retorna `ExpiredToken` e a solução é recopiar o
> `credentials`.

> **Por que importa — a cadeia de credenciais do boto3/AWS CLI:** nem o `producer.py` nem os
> comandos `aws` têm chave escrita no código. Ambos seguem a *default credential chain* do SDK:
> variáveis de ambiente → arquivo `~/.aws/credentials` (+ `~/.aws/config`) → metadados de
> instância, nessa ordem. Como você copiou as credenciais do Lab para `~/.aws/`, tanto o SDK
> (boto3) quanto a CLI as encontram sozinhos — é o mesmo mecanismo que o `aws sts
> get-caller-identity` usa para descobrir **quem** você é. Trocar de sessão do Lab = trocar esse
> arquivo, e todo o resto passa a agir na conta certa.

> ⚠️ As credenciais expiram em ~3–4h. Se qualquer comando `aws`/`boto3` retornar `ExpiredToken`,
> reabra o Lab e **recopie** o `credentials` para `~/.aws/`.

### 5.3 — Pegando os valores da infraestrutura

Você precisa da **URL da fila** e do **nome do bucket**. Leia-os dos outputs do Terraform do
Tutorial 1 (AWS):

```bash
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform output
```

**Resultado esperado** (valores variam):

```
lambda_function_name = "vendas-consumer"
s3_bucket            = "849967252385-streaming-lab"
sqs_queue_url        = "https://sqs.us-east-1.amazonaws.com/849967252385/vendas-queue"
```

Guarde a `sqs_queue_url` (o producer precisa dela) e o `s3_bucket` (para validar no fim). Para
pegar um valor "cru" (útil em scripts):

```bash
terraform output -raw sqs_queue_url
terraform output -raw s3_bucket
```

---

## 6. Relembrando o consumer (a Lambda)

O **consumidor** já existe: é a Lambda `vendas-consumer`, cujo código (`handler.py`) **você
criou** no Tutorial 1 (AWS). Aqui não vamos recriá-lo — só relembrar a lógica, porque é ela que
define o formato da saída no S3.

Cada vez que o SQS acumula um lote (até **100 mensagens** ou **30s** — o que vier primeiro), o
*event source mapping* **invoca** a Lambda passando esse lote em `event["Records"]`. A função:

1. Lê cada `rec["body"]` (o JSON do evento) e monta uma lista de dicionários.
2. Vira um `DataFrame` pandas e converte `data_venda` em timestamp.
3. Grava **um** Parquet em `s3://<bucket>/filas/dt=YYYY-MM-DD/lote-<request_id>.parquet` via
   `awswrangler` (`wr.s3.to_parquet`).

Ou seja: **1 invocação = 1 micro-lote = 1 arquivo Parquet**. O nome usa o
`context.aws_request_id`, que é único por invocação — por isso cada lote vira um arquivo distinto.

> **Teoria — o handler é pura reação, sem loop:** repare no que **não** existe aqui: não há
> `while True`, não há `receive`, não há `ack`. O handler recebe `event["Records"]` **já pronto** (o
> ESM formou o lote e entregou) e só precisa **transformar e gravar**. Comparado ao consumer local
> (RabbitMQ), sumiram o loop, o buffer e o controle de confirmação — tudo virou responsabilidade da
> plataforma. É a mesma lógica de negócio (ler evento → `DataFrame` → Parquet), com muito menos
> encanamento. O `return` sem erro é o que sinaliza ao ESM "pode apagar o lote" (seção 3.2).

Esqueleto do handler (o **código completo** está no **Tutorial 1 AWS, seção 7**):

```python
def lambda_handler(event, context):
    registros = [json.loads(rec["body"]) for rec in event.get("Records", [])]
    if not registros:
        return {"statusCode": 200, "processados": 0}
    df = pd.DataFrame(registros)
    df["data_venda"] = pd.to_datetime(df["data_venda"])
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{BUCKET}/filas/dt={dt}/lote-{context.aws_request_id}.parquet"
    wr.s3.to_parquet(df=df, path=path)          # pandas/pyarrow vêm da layer
    print(f"gravado {len(df)} eventos em {path}")
    return {"statusCode": 200, "processados": len(df)}
```

> `awswrangler` e `pandas` vêm da **layer** "AWS SDK for pandas" — você não os instala nem os
> empacota. O `print(...)` cai no **CloudWatch Logs**, e é por ele que vamos observar o fluxo na
> seção 8.

---

## 7. O producer (você cria)

Regra do curso: o producer você **escreve**. Ele gera eventos de venda sinteticamente (mesmo
catálogo/evento dos outros tutoriais) e os envia para o SQS com `sqs.send_message`.

### 7.1 — Ambiente Python

Na pasta deste tutorial, crie um venv e instale o `boto3`:

```bash
cd tutoriais/streaming/2-filas/aws
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install boto3
```

### 7.2 — O código

Crie o arquivo **`producer.py`** (na pasta deste tutorial) com o conteúdo abaixo. Repare no
**`data_venda` sem fuso** (ISO local) — é o mesmo contrato de evento dos Tutoriais 3 e 4, para
o dado ser idêntico entre os paradigmas.

```python
import json, random, time, uuid, sys
from datetime import datetime, timezone
import boto3

QUEUE_URL = sys.argv[1]                              # cole a sqs_queue_url do terraform output
EPS = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0
sqs = boto3.client("sqs", region_name="us-east-1")

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

n = 0
try:
    while True:
        ev = gerar_evento()
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(ev))
        n += 1
        if n % 10 == 0: print(f"{n} eventos enviados", flush=True)
        time.sleep(1.0/EPS)
except KeyboardInterrupt:
    print(f"total: {n}")
```

O que ele faz, em uma frase: monta um evento, serializa em JSON e chama `send_message` para a fila
— repetindo `EPS` vezes por segundo, até você apertar `Ctrl+C`.

**Passo a passo, linha a linha:**

- `import json, random, time, uuid, sys` / `from datetime import datetime, timezone` /
  `import boto3` — só biblioteca-padrão + o **boto3** (o SDK oficial da AWS para Python). Note a
  **ausência** de `pika`/RabbitMQ: aqui o transporte é a AWS.
- `QUEUE_URL = sys.argv[1]` — a **URL da fila** vem como **1º argumento** da linha de comando (você
  cola a `sqs_queue_url` do `terraform output`). É o único "endereço" que o producer precisa saber;
  ele não conhece Lambda nem S3 — só publica na fila e vai embora.
- `EPS = float(sys.argv[2]) if len(sys.argv) > 2 else 5.0` — **eventos por segundo**, 2º argumento
  **opcional**; sem ele, assume `5.0`. É o que controla o ritmo lá no fim, no `time.sleep`.
- `sqs = boto3.client("sqs", region_name="us-east-1")` — cria o **cliente SQS** apontando para
  `us-east-1`. Repare que **nenhuma credencial** aparece: o boto3 as pega sozinho do `~/.aws/` (a
  cadeia de credenciais da seção 5.2). É o `region_name` que faz o SDK resolver o endpoint real
  `sqs.us-east-1.amazonaws.com`.
- `CATALOGO = {...}` — o mesmo catálogo fixo (`id` → nome, categoria, preço) dos outros tutoriais,
  para o dado sair **comparável** entre os paradigmas.
- `def gerar_evento(): ...` — monta **um** evento sintético de venda:
  - `random.choice(list(CATALOGO))` sorteia um `pid`; `_, cat, preco = CATALOGO[pid]` desempacota
    categoria e preço (o nome é descartado com `_`); `qtd = random.randint(1,5)` sorteia a
    quantidade.
  - o dicionário retornado tem as **7 chaves** do contrato: `evento_id` (um UUID único por evento),
    `cliente_id`, `produto_id`, `categoria`, `quantidade`, `valor_total` (= `preco*qtd`,
    arredondado) e `data_venda`.
  - `data_venda`: pega a hora **UTC** e faz `.replace(tzinfo=None)` para gravar **sem fuso** (ISO
    local, com milissegundos). É o **mesmo contrato** dos Tutoriais 3 e 4 — de propósito, para o
    dado ser idêntico entre os paradigmas.
- `n = 0` — contador de mensagens enviadas.
- `while True:` — o **loop do producer**. Guarde a assimetria: **este** loop é seu (produzir); o
  loop de **consumo** é da AWS (o ESM). No Tutorial 2 local você escreveria os dois.
- `sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(ev))` — a **linha central**: publica
  **uma** mensagem na fila. O `json.dumps(ev)` transforma o dicionário no **texto JSON** que vira o
  `body` da mensagem — exatamente o que a Lambda relê com `json.loads(rec["body"])`. A fila
  transporta **texto**; a estrutura (dict) é responsabilidade das duas pontas.
- `if n % 10 == 0: print(..., flush=True)` — feedback a cada 10 envios; `flush=True` força a saída
  na hora (sem bufferizar o stdout), para você ver o progresso ao vivo.
- `time.sleep(1.0/EPS)` — espaça os envios para dar o ritmo de `EPS` mensagens por segundo.
- `except KeyboardInterrupt: print(f"total: {n}")` — ao apertar **Ctrl+C**, sai do loop e imprime o
  total enviado. É assim que você **para** o producer (e, sem producer, a fila esvazia e a Lambda
  para de ser invocada).

> **Teoria — cada `send_message` é uma requisição HTTP** ao SQS carregando **uma** mensagem. Daí a
> dica logo abaixo: `send_message_batch` junta **até 10** numa só requisição — mesma quantidade de
> mensagens na fila, porém **menos chamadas** (menos custo e overhead de rede). E lembre da garantia
> do SQS: entrega **at-least-once** — em raras falhas de rede a mesma mensagem pode chegar duas
> vezes, o que reforça por que o consumidor ideal é **idempotente** (seção 3.2).

> **Volume maior?** Dá para trocar `send_message` por `sqs.send_message_batch` (até **10 mensagens
> por chamada**), reduzindo o número de requisições. O custo é código um pouco mais complexo
> (montar a lista de `Entries`, cada uma com seu `Id`, e tratar falhas parciais do lote). Para o
> volume deste tutorial, o loop simples com uma mensagem por chamada é mais do que suficiente.

### 7.3 — Rodando

Passe a `sqs_queue_url` como 1º argumento e (opcional) os eventos/segundo como 2º. Rode em um
terminal e **deixe rodando** (é o fluxo contínuo):

```bash
python producer.py "https://sqs.us-east-1.amazonaws.com/849967252385/vendas-queue" 8
```

> Dica: dá para pegar a URL direto do Terraform, sem copiar e colar:
> ```bash
> python producer.py "$(cd ../../1-infraestrutura/aws/terraform && terraform output -raw sqs_queue_url)" 8
> ```

**Resultado esperado**: linhas `10 eventos enviados`, `20 eventos enviados`, ... a cada ~1–2s
(com `EPS=8`). As credenciais do `boto3` vêm do `~/.aws/` — não precisa passar chave no código.

> No S3/SQS **reais** você **não** usa `--endpoint-url` nem `AWS_REQUEST_CHECKSUM_CALCULATION`
> (aquilo era só do MiniStack local). O `boto3` fala direto com a AWS.

> **Fundamento — por que "sem `--endpoint-url`":** no MiniStack local, o SQS/S3 eram **emulados**
> num container na sua máquina, então você tinha de **redirecionar** o boto3/CLI para
> `http://localhost:...` com `--endpoint-url` (e ainda desligar o checksum novo via
> `AWS_REQUEST_CHECKSUM_CALCULATION`, que o emulador não suportava). Na AWS real, o endpoint
> **padrão** já é o certo (`sqs.us-east-1.amazonaws.com`), resolvido a partir do `region_name` do
> cliente — não há nada para redirecionar nem desligar. Menos configuração, e o **mesmo código** de
> produção funciona.

---

## 8. Observando o fluxo

Com o producer rodando, abra **outro terminal** e acompanhe os **logs da Lambda** em tempo real:

```bash
aws logs tail /aws/lambda/vendas-consumer --since 5m --follow
```

**Resultado esperado**: a cada micro-lote, uma linha como
`gravado 47 eventos em s3://849967252385-streaming-lab/filas/dt=2026-07-02/lote-<request_id>.parquet`.
O `N` varia conforme quantas mensagens acumularam na janela de 30s (ou até 100).

> **Fundamento — de onde vem esse log:** todo `print(...)` do handler vai parar no **CloudWatch
> Logs**, no *log group* `/aws/lambda/vendas-consumer` (a Lambda o cria e escreve nele
> automaticamente — desde que tenha permissão, o que o Tutorial 1 já concede via IAM). Cada
> invocação vira uma sequência de linhas ali dentro. O `aws logs tail ... --follow` fica "grudado"
> nesse grupo e imprime as linhas novas em tempo real (como um `tail -f`), e o `--since 5m` traz os
> últimos 5 minutos para você não começar do zero.

> **Por que importa:** sem um processo seu rodando, o CloudWatch é a **janela** para ver a Lambda
> trabalhando — é o primeiro lugar para onde olhar quando "nada aparece no S3". Ou não há linha
> nenhuma (a Lambda **não foi invocada** → problema no ESM ou na fila), ou há um **traceback** (a
> Lambda foi invocada, mas o handler **quebrou**). Cada caso aponta para uma causa diferente.

Para ver quantas mensagens estão **na fila** neste instante (profundidade da fila):

```bash
QURL=$(cd tutoriais/streaming/1-infraestrutura/aws/terraform && terraform output -raw sqs_queue_url)
aws sqs get-queue-attributes --queue-url "$QURL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

- `ApproximateNumberOfMessages` — mensagens esperando para serem lidas.
- `ApproximateNumberOfMessagesNotVisible` — mensagens **em processamento** (uma invocação da
  Lambda as pegou; ficam invisíveis até ela terminar).

> **Teoria — as duas métricas são estados diferentes da mesma mensagem:** uma mensagem no SQS está
> **visível** (esperando alguém pegar) ou **invisível / em voo** (alguém pegou e está processando,
> dentro do *visibility timeout* da seção 3.4). `ApproximateNumberOfMessages` conta as **visíveis**;
> `ApproximateNumberOfMessagesNotVisible` conta as **em voo**. O "Approximate" é honesto: o SQS é
> um sistema **distribuído**, então os números são uma **estimativa** quase em tempo real, não um
> saldo exato. A soma das duas ≈ tudo o que ainda **não** foi apagado (confirmado) da fila.

> **Por que importa — lendo o "termômetro" da fila:** com producer ligado e Lambda saudável, você
> verá um vaivém perto de zero: mensagens entram (sobem em *Messages*), o ESM pega um lote (elas
> migram para *NotVisible*) e somem quando a invocação confirma. Diagnóstico rápido: se **Messages**
> só cresce e **NotVisible** fica em zero, **ninguém está consumindo** (ESM desabilitado ou fila
> errada). Se **NotVisible** fica alto e preso, os lotes estão sendo pegos mas **não confirmados**
> (a Lambda erra, e o *visibility timeout* segura as mensagens até reentregá-las).

Com o producer ativo e a Lambda saudável, esses números devem oscilar perto de zero (o consumo
acompanha a produção). Se `ApproximateNumberOfMessages` só cresce, algo trava o consumo — veja o
Troubleshooting.

---

## 9. Verificando no S3

Depois de alguns lotes (dê ~1 minuto com o producer rodando), liste os Parquet gerados:

```bash
BUCKET=$(cd tutoriais/streaming/1-infraestrutura/aws/terraform && terraform output -raw s3_bucket)
aws s3 ls s3://$BUCKET/filas/ --recursive
```

**Resultado esperado**: vários objetos `filas/dt=YYYY-MM-DD/lote-<request_id>.parquet` — **um por
invocação da Lambda** (um por micro-lote).

> **Fundamento — o formato do caminho não é enfeite:** `dt=YYYY-MM-DD` é uma **partição no estilo
> Hive** — um par `chave=valor` no prefixo do objeto. Ferramentas de consulta (Athena, Spark, etc.)
> leem essa convenção para pular arquivos que não interessam ("só o dia X") sem abrir todos — é o
> chamado *partition pruning*. E o **Parquet** é um formato **colunar e comprimido**: guarda os
> dados por coluna (não por linha), o que deixa leituras analíticas (agregar uma coluna, filtrar
> por outra) muito mais rápidas e menores em disco que CSV/JSON. O nome `lote-<request_id>` fecha o
> ciclo da statelessness (seção 3.3): **1 invocação = 1 arquivo**, sem colisão entre execuções
> concorrentes.

Baixe **um** deles e leia o conteúdo. Com o venv do producer ativo, instale o `pyarrow`:

```bash
pip install pyarrow

# baixa o primeiro lote do dia de hoje
DT=$(date -u +%F)
KEY=$(aws s3 ls s3://$BUCKET/filas/dt=$DT/ | awk '{print $4}' | head -1)
aws s3 cp s3://$BUCKET/filas/dt=$DT/$KEY /tmp/lote.parquet

python -c "import pyarrow.parquet as pq; t=pq.read_table('/tmp/lote.parquet'); print(t.column_names); print(t.to_pylist()[:2])"
```

**Resultado esperado**: as **7 colunas** do evento e algumas linhas, por exemplo:

```python
['evento_id', 'cliente_id', 'produto_id', 'categoria', 'quantidade', 'valor_total', 'data_venda']
[{'evento_id': 'a1b2...', 'cliente_id': 7, 'produto_id': 1, 'categoria': 'Eletronicos',
  'quantidade': 2, 'valor_total': 7000.0, 'data_venda': ...}]
```

Diferente dos Tutoriais 3 e 4 (que gravam **janelas agregadas** por categoria), aqui cada arquivo
guarda os **eventos crus** do micro-lote — é o paradigma de fila (processamento por mensagem),
sem agregação temporal.

---

## 10. Custos e limpeza

1. **Pare o producer**: `Ctrl+C` no terminal onde ele roda (ele imprime `total: N`).
2. Sem producer, a fila esvazia e a Lambda deixa de ser invocada — **não gera mais custo**.
   SQS + Lambda no volume deste curso são praticamente free tier.
3. Os Parquet ficam no S3 (apague-os se quiser). O mais importante: **destrua a infraestrutura**
   para não deixar recursos órfãos — isso é feito no **Tutorial 1 AWS**:

```bash
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform destroy      # digite "yes"
```

> `force_destroy = true` no bucket faz o Terraform apagar os objetos do S3 junto.

> **Fundamento — por que o custo é ~zero aqui:** os três serviços cobram por **uso**, não por
> tempo ligado. **SQS**: por número de **requisições** (as primeiras ~1 milhão/mês são free tier) —
> os `send_message` do producer e os `ReceiveMessage`/`DeleteMessage` do ESM. **Lambda**: por
> **nº de invocações** e por **GB-segundo** (memória × tempo de execução), com franquia mensal
> generosa. **S3**: por **GB armazenado** e por requisições; uns poucos Parquet de kilobytes são
> desprezíveis. Sem producer → sem requisições nem invocações → praticamente **nada** é cobrado.
> Esse é o rosto financeiro do serverless: *pay-per-use*, e ocioso ≈ grátis.

> **Por que importa — o que gera custo esquecido:** filas e Lambda ociosas custam ~zero, mas
> **recursos provisionados** que ficam **ligados por hora** (um NAT Gateway, um RDS, um EC2 do
> Tutorial 1) cobram tenha ou não tráfego. Por isso o passo que mais importa **não** é apagar os
> Parquet — é o `terraform destroy` (no Tutorial 1 AWS), que remove tudo de uma vez e evita
> recursos órfãos queimando a cota do Learner Lab.

---

## 11. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ExpiredToken` / `InvalidClientTokenId` (no producer ou no `aws`) | Credenciais do Lab expiraram | Reabra o Lab, recopie `credentials` para `~/.aws/` (seção 5.2) e rode de novo |
| `botocore ... Unable to locate credentials` / erro de perfil no producer | `~/.aws/credentials` ausente ou perfil errado | Copie as credenciais (seção 5.2); confirme com `aws sts get-caller-identity` |
| Producer roda, mas **nada aparece no S3** | Ainda dentro da janela de 30s do batch, ou erro na Lambda | Aguarde ~30s; veja `aws logs tail /aws/lambda/vendas-consumer --since 5m` — um traceback ali indica erro no handler |
| `AccessDenied` ao enviar/ler | Fora da conta do Lab (credenciais de outra conta) | Confirme a conta com `aws sts get-caller-identity`; recopie as credenciais do Lab |
| Mensagens **ficam na fila** e a Lambda não é invocada (`ApproximateNumberOfMessages` só cresce) | *Event source mapping* desabilitado ou Lambda com erro que devolve o lote | No Tutorial 1 AWS confira o `aws_lambda_event_source_mapping` (`enabled`); veja os logs da Lambda |
| `NonExistentQueue` / `AWS.SimpleQueueService...` | `sqs_queue_url` errada (ou infra destruída) | Releia `terraform output -raw sqs_queue_url` na pasta do Tutorial 1 AWS |
| Parquet aparece, mas erro ao ler | Falta o `pyarrow` no venv | `pip install pyarrow` (seção 9) |

---

**Pronto!** Você fez streaming por **filas na nuvem**: producer → SQS → Lambda (micro-lote) →
Parquet no S3, tudo serverless. Compare com o **Tutorial 2 local** (RabbitMQ + Python) — mesmo
paradigma, você operando o broker — e com os **Tutoriais 3 e 4** (Kafka + Spark/Flink), que trocam
a fila por um **tópico** e o micro-lote por **janelas de event-time de 30s**.

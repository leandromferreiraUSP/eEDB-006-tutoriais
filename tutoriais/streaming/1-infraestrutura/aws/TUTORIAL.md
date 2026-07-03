# Tutorial 1 (AWS): Infraestrutura de Streaming por Filas com Terraform

> Versão **longa e explicativa**. Aqui você provisiona, na **AWS** (AWS Academy Learner Lab), a
> topologia **serverless** de streaming por filas que o Tutorial 2 (AWS) usa: uma fila **SQS**,
> uma função **Lambda** (o consumidor) e um bucket **S3** (destino). Tudo via **Terraform**.
>
> Só os comandos? Veja o `QUICK_TUTORIAL.md`.

---

## Sumário

1. [Objetivo técnico e lógico](#1-objetivo-técnico-e-lógico)
2. [Decisões de projeto (e por quê)](#2-decisões-de-projeto-e-por-quê)
3. [Arquitetura na AWS](#3-arquitetura-na-aws)
4. [Conceitos fundamentais (teoria)](#4-conceitos-fundamentais-teoria)
5. [O ambiente: AWS Academy Learner Lab](#5-o-ambiente-aws-academy-learner-lab)
6. [Pré-requisitos por sistema operacional](#6-pré-requisitos-por-sistema-operacional)
7. [Credenciais do Learner Lab](#7-credenciais-do-learner-lab)
8. [O código da Lambda (você cria)](#8-o-código-da-lambda-você-cria)
9. [Entendendo o Terraform](#9-entendendo-o-terraform)
10. [Provisionando a infraestrutura](#10-provisionando-a-infraestrutura)
11. [Validando](#11-validando)
12. [Destruindo tudo (evite custos!)](#12-destruindo-tudo-evite-custos)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Objetivo técnico e lógico

Na nuvem, demonstramos o paradigma de **filas** com serviços **serverless** (sem servidor para
gerenciar): você **não** sobe Kafka nem Spark. Um **producer** na sua máquina envia eventos para
uma fila **SQS**; a AWS **invoca automaticamente** uma função **Lambda** com um **lote** de
mensagens; a Lambda grava esse lote em **Parquet** no **S3**.

```
producer (sua máquina) ──► SQS (vendas-queue) ──► Lambda (micro-lote) ──► S3 (Parquet)
```

É o **espelho na nuvem** do Tutorial 2 local (RabbitMQ + Python): mesma ideia (fila +
processamento em micro-lote), só que gerenciada pela AWS.

**Lendo o pipeline da esquerda para a direita** (cada seta é um "salto" desacoplado):

| Etapa | Componente | Papel |
|---|---|---|
| **1. Produzir** | `producer.py` na sua máquina (boto3) | Publica cada venda como uma mensagem JSON na fila. Não conhece o consumidor. |
| **2. Enfileirar** | **SQS** `vendas-queue` | Guarda as mensagens de forma durável até alguém consumir. É o "amortecedor" entre quem produz e quem processa. |
| **3. Processar** | **Lambda** `vendas-consumer` | A AWS a invoca automaticamente com um **lote** de mensagens; ela transforma e grava. Não fica ligada esperando — só existe durante a invocação. |
| **4. Persistir** | **S3** (Parquet) | Destino final (data lake). Um arquivo Parquet por lote. |

> **Fundamento — arquitetura orientada a eventos (event-driven).** Em vez de o producer *chamar*
> diretamente o consumidor (acoplamento síncrono, em que um espera o outro), ele apenas **emite um
> evento** (a venda) numa fila. Quem processa **reage** a esse evento quando pode. Producer e
> consumer não se conhecem, não precisam estar online ao mesmo tempo e escalam independentemente.
>
> **Por que importa:** se o consumidor cair ou ficar lento, as mensagens **acumulam na fila** em
> vez de se perderem ou derrubarem o producer. A fila absorve picos de carga (o clássico problema
> de *back-pressure*) e torna o processamento assíncrono e resiliente — exatamente o que se espera
> de um sistema de streaming. A fundamentação completa de cada peça está na [seção 4](#4-conceitos-fundamentais-teoria).

> **Por que só filas na AWS (e não Kafka+Spark/Flink)?** Kafka gerenciado (MSK) e Spark/Flink
> gerenciados (EMR / Managed Flink) são caros e frequentemente **bloqueados** no Learner Lab.
> SQS + Lambda é barato, sempre disponível e ilustra bem o paradigma de filas. Os tutoriais de
> Kafka (3 e 4) ficam **só no ambiente local**.

---

## 2. Decisões de projeto (e por quê)

| Decisão | O que escolhemos | Por quê |
|---|---|---|
| **Serverless** (SQS + Lambda) | Sem EC2, sem servidor | Barato, escala sozinho, disponível no Lab. |
| **Micro-lote na Lambda** | `batch_size=100` + janela de 30s no *event source mapping* | Cada invocação da Lambda recebe **até 100 mensagens** (ou 30s acumulando) e grava **1 Parquet** — é o "micro-lote" sem precisar de estado entre invocações. |
| **Parquet via layer gerenciada** | Layer "AWS SDK for pandas" (pandas + pyarrow) | Empacotar pyarrow na Lambda é grande e chato; a layer pública resolve. |
| **`LabRole` como role da Lambda** | Role IAM pré-existente do Lab | No Learner Lab não criamos roles novas; a `LabRole` já tem acesso a S3/SQS/Logs. |
| **Você cria o `handler.py`** | O Terraform o empacota | Regra do curso: a infra vem pronta, o código do consumidor você escreve. |

Detalhando **por que** cada decisão:

> **Serverless (SQS + Lambda) em vez de EC2/Kafka.** Não há máquina para provisionar, aplicar
> patch, dimensionar ou pagar enquanto está ociosa. A AWS aloca capacidade **por invocação** e
> cobra por execução (ver seção 4.1). No Learner Lab isso também é pragmático: EC2/EMR/MSK são
> caros e frequentemente bloqueados; SQS e Lambda estão sempre liberados.

> **Micro-lote no *event source mapping*.** Em vez de uma invocação por mensagem (caro e lento), a
> AWS **agrupa** mensagens e entrega um lote. `batch_size=100` + janela de 30s significam: "invoque
> a Lambda quando juntar 100 mensagens **ou** quando passarem 30s, o que vier primeiro". Cada
> invocação vira **um** Parquet. Conseguimos o efeito de micro-lote **sem manter estado** entre
> invocações — a própria plataforma faz o agrupamento (ver seção 4.5).

> **Parquet via layer gerenciada.** `pyarrow` (o motor de escrita Parquet) é uma dependência
> nativa grande; empacotá-la manualmente no `.zip` da Lambda é trabalhoso e estoura limites com
> facilidade. A layer pública **"AWS SDK for pandas"** já traz `pandas`, `pyarrow` e `awswrangler`
> prontos — basta referenciá-la (ver seção 4.7).

> **`LabRole` como identidade da Lambda.** Toda Lambda executa "vestindo" uma **role IAM** que lhe
> dá permissões (gravar no S3, ler do SQS, escrever logs). No Learner Lab **não podemos criar
> roles**; usamos a `LabRole`, que já existe e já tem esses acessos (ver seção 4.6).

> **Você escreve o `handler.py`.** É uma regra pedagógica do curso: a **infraestrutura** vem pronta
> (Terraform), mas a **lógica do consumidor** é sua. O Terraform apenas empacota o seu arquivo em
> `.zip` e o publica como código da Lambda.

---

## 3. Arquitetura na AWS

```
            ┌──────────────────────── AWS (us-east-1) — Learner Lab ────────────────────┐
            │                                                                            │
 você ─────►│  ┌───────────────┐  put   ┌──────────────┐  trigger  ┌──────────────────┐ │
producer.py │  │  SQS          │ ─────► │  Lambda      │ ────────► │  S3 bucket       │ │
 (boto3)    │  │  vendas-queue │        │ vendas-      │  Parquet  │  <conta>-        │ │
            │  └───────────────┘        │ consumer     │           │  streaming-lab   │ │
            │        ▲ event source      │ (LabRole)    │           │  filas/dt=.../   │ │
            │        └── mapping ────────┘ + layer      │           └──────────────────┘ │
            │                              pandas/pyarrow                                 │
            └────────────────────────────────────────────────────────────────────────────┘
```

**Como ler o diagrama:** a caixa externa é a fronteira da conta AWS do Lab (região `us-east-1`).
Você, de fora, publica mensagens na **SQS**. O **event source mapping** (a seta de baixo) é o
mecanismo interno que faz o *polling* da fila e dispara a **Lambda** com um lote. A Lambda,
carregando a **LabRole** (identidade/permissões) e a **layer pandas/pyarrow** (dependências),
escreve o Parquet particionado no **S3** (`filas/dt=.../`).

> **Fundamento — serviços gerenciados.** Nenhuma das três caixas (SQS, Lambda, S3) é um servidor
> que você liga, atualiza ou mantém. São **serviços gerenciados**: a AWS opera a infraestrutura, a
> durabilidade e a escala; você só descreve *o quê* quer (via Terraform) e escreve a lógica de
> negócio (o `handler.py`). Compare com o ambiente local (RabbitMQ + Python), em que **você** sobe
> e mantém o broker e o worker.

---

## 4. Conceitos fundamentais (teoria)

Antes de provisionar, vale entender **o que** cada peça é e **por que** ela existe. Esta seção é a
fundamentação; as seções seguintes aplicam esses conceitos ao código real.

### 4.1 — Serverless e FaaS (Function as a Service)

> **Teoria:** *Serverless* **não** significa "sem servidor" literalmente — os servidores existem,
> mas **quem os gerencia é a nuvem**, não você. Você não provisiona, dimensiona nem paga por
> máquinas ociosas. **FaaS** (Function as a Service) é a forma mais pura desse modelo: você entrega
> uma **função** (aqui, `lambda_handler`) e a plataforma cuida de executá-la sob demanda.
>
> **Modelo de cobrança:** paga-se **por invocação** e por **tempo × memória** de execução
> (GB-segundo), não por hora de máquina ligada. Se ninguém invoca, o custo é **zero**.
>
> **Elasticidade automática:** se chegarem 1.000 eventos ao mesmo tempo, a AWS pode rodar **várias
> cópias** da função em paralelo (concorrência) e depois "encolher" para zero. Você não configura
> auto-scaling — ele é intrínseco.

| Característica | Servidor tradicional (EC2) | Serverless (Lambda) |
|---|---|---|
| Provisionamento | Você sobe e mantém a VM | A AWS aloca por invocação |
| Cobrança | Por hora ligada (mesmo ocioso) | Por invocação + GB-segundo |
| Escala | Você configura auto-scaling | Automática (até zero) |
| Patch / SO | Sua responsabilidade | Da AWS |
| Estado | Pode manter em disco/memória | **Efêmero** (some após a invocação) |

### 4.2 — Arquitetura orientada a eventos (event-driven)

> **Fundamento:** em vez de chamadas **síncronas** (o producer chama o consumidor e **espera** a
> resposta, ficando os dois acoplados), o event-driven usa **mensagens/eventos** intermediados por
> um **broker** (aqui, a fila SQS). O producer **emite e segue**; o consumidor **reage** quando
> pode. Isso desacopla no **tempo** (não precisam estar online juntos) e na **escala** (cada lado
> escala sozinho).
>
> **Por que importa:** é o alicerce de sistemas de streaming. A fila vira um **buffer** que absorve
> picos de carga (*back-pressure*), evita perda de dados quando o consumidor está fora do ar e
> permite adicionar/trocar consumidores sem tocar no producer.

### 4.3 — Amazon SQS (Simple Queue Service)

> **Teoria:** o SQS é uma **fila de mensagens gerenciada e distribuída**. Você não vê servidores:
> envia mensagens (`send-message`) e consome mensagens, e a AWS cuida de replicá-las por múltiplas
> zonas de disponibilidade para não perdê-las.

**Standard vs FIFO** — usamos **Standard**:

| | **Standard** (nossa escolha) | **FIFO** |
|---|---|---|
| Ordem | Melhor esforço (pode reordenar) | Ordem estrita por grupo |
| Entrega | **At-least-once** (pode duplicar) | Exactly-once |
| Throughput | Praticamente ilimitado | Limitado (com batching, milhares/s) |
| Uso | Padrão, mais simples | Requer `MessageGroupId` etc. |

> **At-least-once e duplicatas:** a fila Standard garante que a mensagem chega **pelo menos uma
> vez** — em cenários raros, a **mesma** mensagem pode ser entregue **mais de uma vez**. Sistemas
> robustos tratam isso com **idempotência** (processar a duplicata não causa efeito extra). No
> nosso curso aceitamos a duplicata eventual para manter o exemplo simples.

> **Visibility timeout (conceito central):** quando a Lambda pega um lote, o SQS **não apaga** as
> mensagens de imediato — ele as torna **invisíveis** por um período (o *visibility timeout*). Se a
> Lambda terminar com sucesso, elas são removidas; se falhar ou estourar o tempo, elas **voltam a
> ficar visíveis** e são reprocessadas. **Por isso o visibility timeout precisa ser ≥ ao timeout da
> Lambda** — senão a mensagem reapareceria e seria processada em paralelo por outra invocação
> enquanto a primeira ainda roda. Aqui usamos `visibility = 6 × timeout` (60s → 360s), uma folga
> segura.

> **Outros parâmetros:** *retention* (`message_retention_seconds = 3600`) é por quanto tempo a
> mensagem sobrevive na fila se ninguém consumir (1h aqui); *long polling* é o consumidor esperar um
> instante por mensagens novas em vez de perguntar à toa (reduz custo e chamadas vazias). Em
> produção, adiciona-se uma **DLQ (Dead-Letter Queue)**: mensagens que falham N vezes vão para uma
> fila separada para inspeção, em vez de reprocessarem para sempre. (Não usamos DLQ aqui para manter
> o exemplo enxuto, mas é a boa prática.)

### 4.4 — AWS Lambda

> **Teoria — modelo de execução:** a Lambda **não é um servidor ligado**. A AWS mantém o seu código
> empacotado e, quando chega um gatilho (aqui, um lote do SQS), **cria um ambiente de execução**,
> roda a sua função e depois o descarta (ou o reaproveita por um tempo). Você define **memória**
> (`memory_size = 512` MB — a CPU é proporcional à memória) e **timeout** (`timeout = 60`s — tempo
> máximo por invocação).
>
> **Statelessness (sem estado):** cada invocação é **independente** e não deve depender de dados
> guardados de invocações anteriores. **É por isso que o arquivo Parquet leva o
> `context.aws_request_id`** — um identificador **único por invocação** — no nome: como duas
> invocações concorrentes não compartilham estado, o nome único garante que elas **não sobrescrevam**
> o arquivo uma da outra.
>
> **Cold start:** a primeira invocação (ou uma após ociosidade) paga o custo de **inicializar** o
> ambiente (baixar código + layer, iniciar o runtime Python) — é o *cold start*, alguns segundos.
> Invocações seguintes reaproveitam o ambiente "quente" e são rápidas.
>
> **Concorrência:** se muitas mensagens chegam juntas, a AWS roda **várias instâncias** da função em
> paralelo, cada uma com um lote diferente. Isso, somado ao nome de arquivo único, é o que torna o
> consumidor naturalmente escalável.

### 4.5 — Event source mapping (o "micro-lote")

> **Fundamento:** você **não** escreve código para ficar consultando o SQS. O **event source
> mapping** é um componente **gerenciado** que a AWS mantém rodando: ele faz o *polling* da fila,
> **agrupa** mensagens em um lote e **invoca** a sua Lambda passando esse lote em `event["Records"]`.
>
> **Como o lote é formado (os dois "botões"):**
> - `batch_size` (**100**): número **máximo** de mensagens por lote.
> - `maximum_batching_window_in_seconds` (**30**): tempo **máximo** acumulando antes de disparar.
>
> A invocação ocorre quando **qualquer um** dos dois limites é atingido: juntou 100 mensagens **ou**
> passaram 30s. É exatamente isso que transforma um fluxo de eventos avulsos em **micro-lotes** — sem
> você manter nenhum estado. (Por isso, ao validar com **uma** mensagem só, você espera ~30s: quem
> dispara é o gatilho de tempo.)
>
> `function_response_types = ["ReportBatchItemFailures"]` permite à Lambda avisar **quais** mensagens
> do lote falharam, para o SQS reprocessar **só essas** (e não o lote inteiro).

### 4.6 — IAM, STS e a LabRole

> **Teoria:** o **IAM** (Identity and Access Management) controla **quem pode fazer o quê** na AWS.
> Uma **role** é uma identidade com um conjunto de permissões que um serviço pode **assumir**
> temporariamente. A Lambda **não tem senha**; ela **assume a role** indicada (`role = ...arn`) e,
> por baixo, o **STS** (Security Token Service) emite **credenciais temporárias** para aquela
> execução.
>
> **Por que a LabRole:** no Learner Lab suas próprias credenciais também são **temporárias e
> assumidas** (o `aws sts get-caller-identity` mostra `assumed-role/voclabs/...`) e você **não tem
> permissão para criar roles novas**. A `LabRole` é uma role pré-criada pela AWS Academy que já
> carrega os acessos necessários (S3, SQS, CloudWatch Logs). Por isso o Terraform apenas a
> **referencia** (`data "aws_iam_role"`), em vez de criá-la.
>
> **Princípio do menor privilégio:** o ideal, em produção, seria uma role **sob medida** com
> **apenas** as permissões que esta Lambda precisa (ler *desta* fila, gravar *neste* bucket). A
> `LabRole` é mais ampla que o necessário — um trade-off aceito **só** por limitação do Lab.

### 4.7 — Lambda Layers e a "AWS SDK for pandas"

> **Teoria:** uma **layer** é um pacote `.zip` de **dependências** que várias Lambdas podem
> compartilhar, montado no ambiente de execução junto do seu código. Assim o seu `.zip` fica
> minúsculo (só o `handler.py`) e as bibliotecas pesadas vêm da layer.
>
> **Por que esta layer:** a layer pública **"AWS SDK for pandas"** (`AWSSDKPandas-Python312`) já traz
> `pandas`, `pyarrow` **e** `awswrangler` compilados e compatíveis. Sem ela, você teria de empacotar
> o `pyarrow` (binário nativo, grande e sensível à versão do runtime) manualmente — fonte comum de
> erro.
>
> **Por que `list-layer-versions` dá AccessDenied no Lab:** a layer pertence a **outra conta AWS**
> (`336392948345`, da própria AWS). Sua conta do Lab pode **usá-la** (o ARN é público), mas **não tem
> permissão para listar** as versões dela — daí o `AccessDenied`. Por isso a versão (`29`) vem da
> **documentação oficial**, não do CLI.

### 4.8 — Terraform e Infraestrutura como Código (IaC)

> **Fundamento — declarativo × imperativo:** num script **imperativo** (bash com `aws ...`) você
> descreve **os passos** ("crie o bucket, depois a fila, depois..."). No Terraform, **declarativo**,
> você descreve o **estado final desejado** ("quero que existam este bucket, esta fila e esta
> Lambda") e o Terraform calcula **o que** precisa criar, alterar ou apagar para chegar lá.
>
> **State (estado):** o Terraform guarda um arquivo de **state** (`terraform.tfstate`) que mapeia o
> que você descreveu ↔ o que existe de fato na AWS. É como ele sabe que "já criou" um recurso e o que
> mudou desde a última vez.
>
> **Idempotência:** rodar `apply` de novo, sem mudar nada, **não recria** nada — o estado desejado já
> bate com o real. É o oposto de um script que criaria recursos duplicados a cada execução.

Peças do Terraform que você verá:

| Conceito | O que é | No projeto |
|---|---|---|
| **Provider** | Plugin que fala com uma API (AWS, etc.) | `aws ~> 5.0`, `archive ~> 2.0` |
| **`resource`** | Algo que o Terraform **cria/gerencia** | bucket S3, fila SQS, Lambda, mapping |
| **`data` source** | Algo que ele apenas **lê** (não cria) | `aws_caller_identity`, `aws_iam_role`, `archive_file` |
| **`variable`** | Entrada parametrizável | região, `batch_size`, versão da layer |
| **`output`** | Valor exposto ao final | `sqs_queue_url`, `s3_bucket` |
| **`locals`** | Valores calculados/reaproveitados | nome do bucket, ARN da layer |

> **Ciclo de vida:** `init` (baixa os providers) → `plan` (mostra o **diff** do que vai mudar, sem
> aplicar) → `apply` (executa) → `destroy` (remove tudo). O `plan` antes do `apply` é a rede de
> segurança: você **vê** o que vai acontecer antes de confirmar.

### 4.9 — Parquet e awswrangler

> **Teoria — por que Parquet:** Parquet é um formato **colunar** e **comprimido**, padrão de fato em
> data lakes. Ao contrário de CSV/JSON (orientados a linha), ele guarda cada **coluna** junta, o que
> dá **compressão** muito melhor e permite ler **só as colunas necessárias** — leituras analíticas
> (Spark, Athena) ficam mais rápidas e baratas. Ele também **carrega o schema** (tipos) embutido, por
> isso convertemos `data_venda` para timestamp antes de gravar.
>
> **`wr.s3.to_parquet`:** o `awswrangler` (a "AWS SDK for pandas") abstrai a escrita: recebe um
> `DataFrame` do pandas e um caminho `s3://...`, serializa em Parquet (via `pyarrow`) e envia ao S3
> em uma chamada — sem você lidar com buffers ou com o cliente boto3 do S3 na mão.

### 4.10 — O espelho do RabbitMQ local

> **Por que importa:** este tutorial é o **análogo gerenciado** do Tutorial 2 local (RabbitMQ + worker
> Python). A ideia é idêntica — **fila + processamento em micro-lote** — mudando apenas *quem opera*
> cada peça:

| Papel | Local (Tutorial 2) | AWS (este tutorial) |
|---|---|---|
| Broker/fila | **RabbitMQ** (você sobe) | **SQS** (gerenciado) |
| Consumidor | Worker Python que **você** mantém rodando | **Lambda** (roda sob demanda) |
| "Puxar" da fila | Seu loop `basic_consume` | **Event source mapping** (a AWS puxa) |
| Destino | Arquivo local / MiniStack | **S3** real |

> Aprender os dois lados mostra a **mesma arquitetura** com dois níveis de responsabilidade
> operacional: no local você entende as engrenagens; na AWS você delega a operação e foca na lógica.

---

## 5. O ambiente: AWS Academy Learner Lab

| Restrição | Valor | Como tratamos |
|---|---|---|
| Região | `us-east-1` | fixada nas variáveis |
| IAM | usar roles pré-criadas (`LabRole`) | a Lambda usa `data.aws_iam_role.lab` |
| Credenciais | temporárias (expiram ~3–4h) | reabra o Lab e recopie quando expirar |
| Serviços | SQS, Lambda, S3 liberados | (RDS/EMR/MSK costumam ser bloqueados — por isso não usamos) |

> **Por que as credenciais expiram:** o Learner Lab não te dá uma chave permanente. Ao abrir o lab,
> a AWS Academy emite, via **STS**, um **par de credenciais temporário** (com *session token*) válido
> por poucas horas. Quando expira, qualquer comando `aws`/`terraform` retorna `ExpiredToken` — a
> solução é reabrir o lab e **recopiar** o arquivo `credentials` (seção 7). Isso reforça o hábito de
> **destruir** a infra ao final: o ambiente é efêmero por natureza.

> ⚠️ **Custos**: SQS e Lambda são baratíssimos (praticamente free tier no volume do curso). Ainda
> assim, **destrua** ao final (seção 12) para não deixar recursos órfãos.

---

## 6. Pré-requisitos por sistema operacional

Você precisa do **Terraform** (para provisionar), do **AWS CLI v2** (para autenticar e validar) e
do **Python 3.12** (para o producer do Tutorial 2). Instale conforme o seu sistema.

### 6.1 — macOS

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform awscli python@3.12
terraform -version && aws --version && python3 --version
```

> **O que cada linha faz (macOS):** `brew tap` registra o repositório oficial da HashiCorp no
> Homebrew; `brew install` instala **Terraform**, **AWS CLI v2** e **Python 3.12** de uma vez; a
> última linha apenas **confere as versões** — se os três respondem, o ambiente está pronto.

### 6.2 — Linux (Ubuntu)

```bash
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform python3.12 python3.12-venv

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip
terraform -version && aws --version
```

> **O que cada linha faz (Ubuntu):** as duas primeiras adicionam a **chave GPG** e o **repositório
> APT** da HashiCorp (para o `apt` confiar na origem e achar o Terraform); o `apt-get install`
> instala Terraform e Python 3.12 (+ `venv`, para ambientes virtuais). O bloco `curl ... unzip ...
> install` baixa e instala o **AWS CLI v2** (que não vem no APT) e depois remove os arquivos
> temporários.

### 6.3 — Windows (PowerShell)

```powershell
winget install -e --id Hashicorp.Terraform
winget install -e --id Amazon.AWSCLI
winget install -e --id Python.Python.3.12
# feche e reabra o PowerShell, então:
terraform -version; aws --version; python --version
```

> **O que cada linha faz (Windows):** `winget install` baixa e instala Terraform, AWS CLI e Python
> 3.12 pelo gerenciador de pacotes do Windows. É preciso **fechar e reabrir** o PowerShell para o
> `PATH` atualizar antes de conferir as versões.

---

## 7. Credenciais do Learner Lab

As credenciais ficam em **`tutoriais/aws_credenciais/`** (`credentials`, `config`). Copie-as
para `~/.aws/` — a pasta padrão onde o **AWS CLI** e o **Terraform** procuram por elas
automaticamente.

### 7.1 — macOS / Linux

```bash
mkdir -p ~/.aws
cp tutoriais/aws_credenciais/credentials ~/.aws/credentials
cp tutoriais/aws_credenciais/config      ~/.aws/config
chmod 600 ~/.aws/credentials
```

> **Passo a passo (macOS/Linux):** `mkdir -p ~/.aws` cria a pasta padrão de configuração; os dois
> `cp` colocam ali o `credentials` (chaves temporárias + *session token*) e o `config` (região e
> perfil); `chmod 600` restringe o arquivo de credenciais ao seu usuário — boa prática de segurança
> (ninguém mais lê suas chaves).

### 7.2 — Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.aws" | Out-Null
$proj = "C:\caminho\para\Big Data\tutoriais\aws_credenciais"   # ajuste
Copy-Item "$proj\credentials" "$env:USERPROFILE\.aws\credentials" -Force
Copy-Item "$proj\config"      "$env:USERPROFILE\.aws\config" -Force
```

> **Passo a passo (Windows):** cria a pasta `.aws` no seu perfil e copia `credentials` e `config`
> para lá. Ajuste a variável `$proj` para o caminho real onde está a pasta `aws_credenciais` do
> curso.

Valide a identidade:

```bash
aws sts get-caller-identity
```

**Resultado esperado**: um JSON com `Account` e um `Arn` contendo `assumed-role/voclabs/...`.

> **Lendo o resultado:** o `Arn` com `assumed-role/voclabs/...` confirma que você está usando as
> **credenciais temporárias** do Lab (uma role assumida via STS), e não um usuário permanente — é
> assim que o Learner Lab funciona (seção 4.6). O campo `Account` é o número da sua conta do Lab,
> que reaparece no nome do bucket (`<conta>-streaming-lab`).

---

## 8. O código da Lambda (você cria)

O Terraform empacota o arquivo **`terraform/build/handler.py`** — que **você cria**. Ele é o
**consumidor**: recebe um lote de mensagens do SQS e grava um Parquet no S3. Crie a pasta e o
arquivo:

```bash
mkdir -p tutoriais/streaming/1-infraestrutura/aws/terraform/build
```

Conteúdo de `terraform/build/handler.py`:

```python
import json, os
from datetime import datetime, timezone
import awswrangler as wr
import pandas as pd

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "filas")


def lambda_handler(event, context):
    registros = []
    for rec in event.get("Records", []):
        registros.append(json.loads(rec["body"]))

    if not registros:
        return {"statusCode": 200, "processados": 0}

    df = pd.DataFrame(registros)
    df["data_venda"] = pd.to_datetime(df["data_venda"])

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{BUCKET}/{PREFIX}/dt={dt}/lote-{context.aws_request_id}.parquet"
    wr.s3.to_parquet(df=df, path=path)

    print(f"gravado {len(df)} eventos em {path}")
    return {"statusCode": 200, "processados": len(df)}
```

**O `handler.py` parte a parte:**

- **`import json, os` / `datetime` / `awswrangler as wr` / `pandas as pd`** — `json` decodifica o
  corpo das mensagens; `os` lê variáveis de ambiente; `datetime` monta a partição por dia;
  **`awswrangler` e `pandas` vêm da layer** (você não os instala).
- **`BUCKET = os.environ["BUCKET"]`** — lê o bucket de destino da variável de ambiente que o
  Terraform injeta. Usar `os.environ[...]` (e não `.get`) faz a função **falhar cedo** e com erro
  claro se a variável não existir — melhor que um comportamento silencioso errado.
- **`PREFIX = os.environ.get("PREFIX", "filas")`** — prefixo (pasta lógica) no bucket, com default
  `"filas"` caso não venha definido.
- **`def lambda_handler(event, context):`** — a **função de entrada**. A AWS a chama a cada
  invocação passando `event` (o lote de mensagens) e `context` (metadados da execução, como o
  `aws_request_id`). O nome bate com `handler = "handler.lambda_handler"` no Terraform (formato
  `arquivo.função`).
- **`for rec in event.get("Records", []): ... json.loads(rec["body"])`** — percorre cada mensagem
  do lote; `rec["body"]` é o **JSON que o producer enviou**, que voltamos a transformar em `dict`.
- **`if not registros: return {... "processados": 0}`** — se o lote vier vazio, encerra sem gravar
  (guarda de segurança).
- **`df = pd.DataFrame(registros)`** — converte a lista de dicionários em um **DataFrame** (tabela
  em memória).
- **`df["data_venda"] = pd.to_datetime(df["data_venda"])`** — converte a string ISO em
  **timestamp**, para o Parquet gravar a coluna com o **tipo** correto (não como texto).
- **`dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")`** — data de hoje (UTC) para a
  **partição** `dt=YYYY-MM-DD` (padrão Hive, que Athena/Spark reconhecem para "podar" partições).
- **`path = f"s3://{BUCKET}/{PREFIX}/dt={dt}/lote-{context.aws_request_id}.parquet"`** — monta o
  caminho de destino. O **`aws_request_id`** (único por invocação) no nome evita que invocações
  **concorrentes** sobrescrevam o arquivo umas das outras — reflexo direto da **statelessness** da
  Lambda (seção 4.4).
- **`wr.s3.to_parquet(df=df, path=path)`** — serializa o DataFrame em **Parquet** (via `pyarrow`) e
  grava direto no S3, numa única chamada (seção 4.9).
- **`print(...)`** — vai para o **CloudWatch Logs**; é como você depura a Lambda.
- **`return {"statusCode": 200, "processados": len(df)}`** — retorno de sucesso; sinaliza ao
  *event source mapping* que o lote foi processado (e o SQS pode **apagar** as mensagens).

> `awswrangler` e `pandas` vêm da **layer** gerenciada (você não precisa instalá-los). Cada
> invocação grava **um** Parquet com o lote inteiro — daí o "micro-lote".

---

## 9. Entendendo o Terraform

Arquivos em `1-infraestrutura/aws/terraform/`:

| Arquivo | O que define |
|---|---|
| `versions.tf` | providers `aws ~> 5.0` e `archive` (para zipar a Lambda) |
| `variables.tf` | região, nome da fila, `batch_size`, versão da layer pandas |
| `main.tf` | S3 + SQS + Lambda + *event source mapping* |
| `outputs.tf` | URL da fila, nome do bucket, nome da Lambda |

Trechos-chave do `main.tf`:

```hcl
data "aws_iam_role" "lab" { name = var.lambda_role_name }   # LabRole

data "archive_file" "lambda_zip" {          # empacota o build/handler.py que VOCÊ criou
  type        = "zip"
  source_file = "${path.module}/build/handler.py"
  output_path = "${path.module}/build/handler.zip"
}

resource "aws_lambda_function" "consumer" {
  function_name = "vendas-consumer"
  role          = data.aws_iam_role.lab.arn
  runtime       = "python3.12"
  handler       = "handler.lambda_handler"
  layers        = [local.pandas_layer_arn]     # AWS SDK for pandas (pandas + pyarrow)
  # ...
}

resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn                   = aws_sqs_queue.vendas.arn
  function_name                      = aws_lambda_function.consumer.arn
  batch_size                         = var.batch_size                  # até 100 msgs/invocação
  maximum_batching_window_in_seconds = var.batch_window_seconds        # ou 30s acumulando
}
```

**Os trechos do `main.tf` explicados:**

- **`data "aws_iam_role" "lab"`** — um **data source**: o Terraform **lê** (não cria) a `LabRole`
  pré-existente para pegar o ARN dela e usá-lo como identidade da Lambda (seção 4.6).
- **`data "archive_file" "lambda_zip"`** — usa o provider `archive` para **zipar** o seu
  `build/handler.py` em `handler.zip`. É esse `.zip` que vira o código publicado na Lambda — por
  isso o `handler.py` precisa **existir antes** do `plan`/`apply`.
- **`resource "aws_lambda_function" "consumer"`** — cria a Lambda: `function_name` (nome),
  `role` (a `LabRole`, via o data source), `runtime = "python3.12"`, `handler` no formato
  `arquivo.função` (`handler.lambda_handler`) e `layers` apontando para a **AWS SDK for pandas**.
- **`resource "aws_lambda_event_source_mapping" "sqs_to_lambda"`** — a **cola** entre fila e função:
  liga o ARN da SQS (`event_source_arn`) à Lambda (`function_name`) e define o micro-lote com
  `batch_size` (até 100) e `maximum_batching_window_in_seconds` (até 30s). É o componente que faz o
  *polling* e invoca a função (seção 4.5).

> **Fundamento — por que empacotar com `archive_file`:** a Lambda recebe código como um `.zip`. Em
> vez de você zipar na mão, o Terraform gera o `.zip` a partir do seu `handler.py` e usa o
> `source_code_hash` para **detectar mudanças**: se você editar o handler, o próximo `apply`
> republica; se não mexer, nada acontece (idempotência — seção 4.8).

> **A versão da layer pandas muda com o tempo** (o default aqui é **29**, para Python 3.12 em
> `us-east-1`). Se o `apply` falhar dizendo que a layer não existe, ajuste
> `-var="pandas_layer_version=NN"`. Para descobrir a versão atual, use a **doc oficial** (a coluna
> `us-east-1`, linha Python 3.12): <https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html>.
>
> ⚠️ No **Learner Lab**, o comando `aws lambda list-layer-versions` para essa layer costuma dar
> **AccessDenied** (a layer pertence a outra conta, `336392948345`) — por isso use a doc, não o CLI.

---

## 10. Provisionando a infraestrutura

```bash
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform init
terraform plan        # confira: ~4 recursos a criar
terraform apply       # digite "yes"
```

> **O ciclo, comando a comando:** `terraform init` baixa os providers (`aws`, `archive`) e prepara a
> pasta; `terraform plan` mostra o **diff** — os ~4 recursos que serão criados — **sem** alterar
> nada; `terraform apply` executa de fato (e pede `yes` para confirmar). O `plan` é a sua chance de
> revisar antes de mexer na conta (seção 4.8).

**Resultado esperado** (valores variam):

```
Outputs:
lambda_function_name = "vendas-consumer"
pandas_layer_arn     = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:29"
s3_bucket            = "849967252385-streaming-lab"
sqs_queue_url        = "https://sqs.us-east-1.amazonaws.com/849967252385/vendas-queue"
```

> **O que são esses outputs:** valores que o Terraform **expõe ao final** do `apply` (definidos em
> `outputs.tf`). O `lambda_function_name` você usa para ver logs; o `pandas_layer_arn` confirma a
> versão da layer aplicada; **`s3_bucket` e `sqs_queue_url` são os que o Tutorial 2 (AWS) consome** —
> daí a recomendação de guardá-los.

Guarde a `sqs_queue_url` e o `s3_bucket` — o Tutorial 2 (AWS) usa esses valores. Releia quando
quiser:

```bash
terraform output
terraform output -raw sqs_queue_url
```

> **`terraform output` vs `-raw`:** sem argumentos, lista **todos** os outputs formatados; com
> `-raw sqs_queue_url`, imprime **só o valor cru** de um output — ideal para capturar numa variável
> de shell (como fazemos na validação: `QURL=$(terraform output -raw sqs_queue_url)`).

---

## 11. Validando

Envie **uma** mensagem de teste para a fila e veja um Parquet aparecer no S3 (a Lambda leva até
~30s por causa da janela de batch):

```bash
QURL=$(terraform output -raw sqs_queue_url)
BUCKET=$(terraform output -raw s3_bucket)

aws sqs send-message --queue-url "$QURL" --message-body \
  '{"evento_id":"teste-1","cliente_id":1,"produto_id":1,"categoria":"Eletronicos","quantidade":1,"valor_total":100.0,"data_venda":"2026-07-02T12:00:00.000"}'

# aguarde ~30s e liste o bucket:
aws s3 ls s3://$BUCKET/filas/ --recursive
```

> **A validação, linha a linha:** as duas primeiras linhas capturam a URL da fila e o nome do bucket
> em variáveis (via `terraform output -raw`). O `aws sqs send-message` publica **uma** mensagem JSON
> na fila — é o papel que o producer terá no Tutorial 2. O `aws s3 ls ... --recursive` lista o bucket
> para ver o Parquet aparecer.
>
> **Por que esperar ~30s:** com **uma** mensagem só, o lote não chega às 100 do `batch_size`; quem
> dispara a Lambda é a **janela de 30s** (`maximum_batching_window_in_seconds`) — o gatilho de tempo
> do micro-lote (seção 4.5). Esse atraso é **esperado**, não é erro.

**Resultado esperado**: um objeto `filas/dt=.../lote-<id>.parquet`.

Veja os logs da Lambda:

```bash
aws logs tail /aws/lambda/vendas-consumer --since 5m
```

> **`aws logs tail`:** lê o grupo de logs do CloudWatch da Lambda (`/aws/lambda/vendas-consumer`)
> dos últimos 5 minutos. É ali que aparece o `print(...)` do handler ("gravado N eventos...") e
> qualquer erro/stack trace — o primeiro lugar para investigar se nada surgir no S3.

> No S3 **real** você **não** precisa de `--endpoint-url` nem do `AWS_REQUEST_CHECKSUM_CALCULATION`
> (aquilo era específico do MiniStack local).

A infraestrutura está pronta. Siga para `2-filas/aws/` (rodar o producer de verdade e observar o
fluxo). **Lembre de destruir no fim.**

---

## 12. Destruindo tudo (evite custos!)

```bash
cd tutoriais/streaming/1-infraestrutura/aws/terraform
terraform destroy     # digite "yes"
```

> **O que o `destroy` faz:** o Terraform lê o **state**, calcula a ordem inversa de dependências e
> **remove** todos os recursos que criou (mapping, Lambda, fila, bucket). Também pede `yes` para
> confirmar.

> `force_destroy = true` no bucket faz o Terraform apagar os objetos do S3 junto.

> **Por que destruir (Learner Lab):** ainda que SQS/Lambda/S3 sejam baratíssimos no volume do curso,
> deixar recursos órfãos consome o **orçamento limitado** do Lab e pode disparar seu encerramento.
> Além disso, as **credenciais expiram** e o ambiente pode ser resetado — destruir ao fim garante que
> você recomeça limpo. Regra de ouro do Lab: **provisione, use, destrua.**

---

## 13. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `ExpiredToken` / `InvalidClientTokenId` | Credenciais do Lab expiraram | Reabra o Lab, recopie `credentials` para `~/.aws/` |
| `no such file ... build/handler.py` no `plan` | Você não criou o `handler.py` | Crie `terraform/build/handler.py` (seção 8) antes do `apply` |
| `layer version ... does not exist` | Versão da layer pandas desatualizada | Descubra a atual e use `-var="pandas_layer_version=NN"` (seção 9) |
| `AccessDenied` ao criar Lambda/SQS | Política do Lab | Confirme que está na conta do Lab (`aws sts get-caller-identity`) |
| Enviei mensagem mas nada no S3 | Ainda dentro da janela de batch, ou erro na Lambda | Aguarde ~30s; veja `aws logs tail /aws/lambda/vendas-consumer` |
| `InvalidParameterValue ... visibility timeout` | Visibility < timeout da Lambda | Já tratado (visibility = 6× timeout) |

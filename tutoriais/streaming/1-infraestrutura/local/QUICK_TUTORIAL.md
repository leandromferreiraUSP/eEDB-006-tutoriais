# Quick Tutorial 1 (Local): Infraestrutura de Streaming com Docker

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: Kafka + RabbitMQ + Spark + Flink + MiniStack (S3, bucket `datalake`) rodando.

---

## 1. Pré-requisitos (uma vez)

- **macOS**: `brew install --cask orbstack` (abra) · `brew install python@3.12 awscli`
- **Ubuntu**: Docker Engine + `docker-compose-plugin` · `python3.12` · AWS CLI v2
- **Windows**: `winget install Docker.DockerDesktop Python.Python.3.12 Amazon.AWSCLI` (abra o Docker Desktop)

Confira: `docker --version` · `python3 --version` · `aws --version`

---

## 2. Subir o ambiente

```bash
cd tutoriais/streaming/1-infraestrutura/local/docker
docker compose up -d --build
docker compose ps          # esperar kafka/ministack/rabbitmq (healthy); spark/flink Up
```

---

## 3. Variáveis do AWS CLI (apontando para o MiniStack)

```bash
# macOS / Linux
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required   # MiniStack não aceita CRC64NVME
```

```powershell
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="test"; $env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"; $env:AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
```

---

## 4. Criar o tópico `vendas` (usado nos Tut. 3 e 4)

```bash
docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vendas --partitions 1 --replication-factor 1
```

---

## 5. Validar

```bash
# macOS / Linux (script pronto):
cd tutoriais/streaming/1-infraestrutura/local && bash validar.sh

# Manual (qualquer SO):
docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list  # vendas
aws --endpoint-url http://localhost:4566 s3 ls                                                         # datalake
```

UIs: RabbitMQ <http://localhost:15672> (guest/guest) · Flink <http://localhost:8081>

---

## 6. Parar / limpar

```bash
cd tutoriais/streaming/1-infraestrutura/local/docker
docker compose stop       # pausar
docker compose down -v    # remover tudo (recria limpo na próxima subida)
```

> Deixe rodando para fazer os Tutoriais 2 (Filas), 3 (Kafka+Spark) e 4 (Kafka+Flink).

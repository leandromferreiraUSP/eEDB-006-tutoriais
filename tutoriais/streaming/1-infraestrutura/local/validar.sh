#!/usr/bin/env bash
# validar.sh -- Verifica se o ambiente LOCAL de streaming está saudável:
#   1. Kafka respondendo (lista tópicos)
#   2. RabbitMQ respondendo (ping)
#   3. MiniStack (S3) no ar e com o bucket "datalake"
#   4. Spark e Flink de pé
#
# Uso (a partir de tutoriais/streaming/1-infraestrutura/local):
#   bash validar.sh
#
# Requer: docker em execução com o compose deste tutorial já de pé.
set -uo pipefail

ENDPOINT="http://localhost:4566"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

falhas=0

echo "==> 1/4 Kafka: listar tópicos (broker no ar?)"
if docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 ; then
  echo "    OK (Kafka respondendo em kafka:9092)"
else
  echo "    FALHA: Kafka não respondeu"; falhas=$((falhas+1))
fi

echo "==> 2/4 RabbitMQ: ping"
if docker exec streaming_rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1 ; then
  echo "    OK (RabbitMQ no ar; UI em http://localhost:15672 guest/guest)"
else
  echo "    FALHA: RabbitMQ não respondeu"; falhas=$((falhas+1))
fi

echo "==> 3/4 MiniStack: bucket datalake existe?"
if aws --endpoint-url "${ENDPOINT}" s3 ls 2>/dev/null | grep -q "datalake" ; then
  echo "    OK (s3://datalake presente)"
else
  echo "    FALHA: bucket datalake não encontrado"; falhas=$((falhas+1))
fi

echo "==> 4/4 Spark e Flink: containers de pé?"
if docker ps --format '{{.Names}}' | grep -q streaming_spark \
   && docker ps --format '{{.Names}}' | grep -q streaming_flink_jobmanager \
   && docker ps --format '{{.Names}}' | grep -q streaming_flink_taskmanager ; then
  echo "    OK (spark + flink jobmanager/taskmanager rodando)"
else
  echo "    FALHA: Spark ou Flink não estão rodando"; falhas=$((falhas+1))
fi

echo
if [ "$falhas" -eq 0 ]; then
  echo "✅ Ambiente local de streaming OK."
else
  echo "❌ ${falhas} verificação(ões) falharam."; exit 1
fi

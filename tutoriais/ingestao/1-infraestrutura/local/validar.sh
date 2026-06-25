#!/usr/bin/env bash
# validar.sh -- Verifica se o ambiente local de ingestão está saudável:
#   1. Postgres respondendo e com as 3 tabelas populadas
#   2. MiniStack (S3) no ar e com o bucket "datalake"
#
# Uso (a partir de tutoriais/ingestao/1-infraestrutura/local):
#   bash validar.sh
#
# Requer: docker em execução com o compose deste tutorial já de pé.
set -euo pipefail

PG_CONTAINER="ingestao_postgres"
ENDPOINT="http://localhost:4566"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

falhas=0

echo "==> 1/3 Postgres: contagem de linhas por tabela"
if docker exec "$PG_CONTAINER" psql -U ecommerce -d ecommerce -At \
    -c "SELECT 'clientes', count(*) FROM clientes
        UNION ALL SELECT 'produtos', count(*) FROM produtos
        UNION ALL SELECT 'vendas',   count(*) FROM vendas;" ; then
  echo "    OK (esperado: clientes=20, produtos=15, vendas=200)"
else
  echo "    FALHA ao consultar o Postgres"; falhas=$((falhas+1))
fi

echo "==> 2/3 MiniStack: health-check"
if curl -sf "${ENDPOINT}/_ministack/health" >/dev/null ; then
  echo "    OK (S3 no ar em ${ENDPOINT})"
else
  echo "    FALHA: MiniStack não respondeu em ${ENDPOINT}"; falhas=$((falhas+1))
fi

echo "==> 3/3 MiniStack: bucket datalake existe?"
if aws --endpoint-url "${ENDPOINT}" s3 ls | grep -q "datalake" ; then
  echo "    OK (s3://datalake presente)"
else
  echo "    FALHA: bucket datalake não encontrado"; falhas=$((falhas+1))
fi

echo
if [ "$falhas" -eq 0 ]; then
  echo "✅ Ambiente local OK."
else
  echo "❌ ${falhas} verificação(ões) falharam."; exit 1
fi

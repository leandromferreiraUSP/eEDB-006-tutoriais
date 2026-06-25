#!/usr/bin/env bash
# ready.d/01-bucket.sh -- Init script do MiniStack executado na fase "ready" (após o
# gateway S3 já estar ouvindo na porta 4566). Cria o bucket de destino "datalake".
# O MiniStack NÃO injeta credenciais nos init scripts, então passamos creds dummy
# (test/test) e o --endpoint-url explicitamente.
set -euo pipefail

export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"
EP="http://localhost:4566"

echo "[ready] criando bucket s3://datalake ..."
aws --endpoint-url "$EP" s3 mb s3://datalake 2>/dev/null || echo "[ready] bucket já existe (ok)"

echo "[ready] buckets disponíveis:"
aws --endpoint-url "$EP" s3 ls
echo "[ready] pronto."

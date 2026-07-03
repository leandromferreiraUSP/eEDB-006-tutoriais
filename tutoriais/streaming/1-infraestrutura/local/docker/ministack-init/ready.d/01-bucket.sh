#!/usr/bin/env bash
# Cria o bucket s3://datalake assim que o S3 do MiniStack fica pronto.
# Roda em ready.d/ (DEPOIS do gateway S3 subir). O MiniStack não injeta credenciais,
# então passamos credenciais dummy (test/test) e o --endpoint-url explicitamente.
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

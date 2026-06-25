# Quick Tutorial 1 (Local): Infraestrutura com Docker

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: Postgres (origem, populado) + MiniStack S3 (destino, bucket `datalake`) rodando.

---

## 1. Pré-requisitos (uma vez)

- **macOS**: `brew install --cask docker` (abra o app) · `brew install python@3.12 awscli`
- **Ubuntu**: Docker Engine + `docker-compose-plugin` · `python3.12` · AWS CLI v2
- **Windows**: `winget install Docker.DockerDesktop Python.Python.3.12 Amazon.AWSCLI` (abra o Docker Desktop)

Confira: `docker --version` · `python3 --version` · `aws --version`

---

## 2. Subir o ambiente

```bash
cd tutoriais/ingestao/1-infraestrutura/local/docker
docker compose up -d
docker compose ps          # esperar os 2 ficarem (healthy)
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

## 4. Validar

```bash
# macOS / Linux (script pronto):
cd tutoriais/ingestao/1-infraestrutura/local && bash validar.sh

# Manual (qualquer SO):
docker exec ingestao_postgres psql -U ecommerce -d ecommerce -c "SELECT count(*) FROM vendas;"   # 200
aws --endpoint-url http://localhost:4566 s3 ls                                                     # datalake
```

**Resultado esperado**: `vendas = 200`, bucket `datalake` listado.

---

## 5. Parar / limpar

```bash
cd tutoriais/ingestao/1-infraestrutura/local/docker
docker compose stop      # pausar
docker compose down -v   # remover tudo (recria limpo na próxima subida)
```

> Deixe rodando para fazer os Tutoriais 2 (Meltano) e 3 (DLTHub).

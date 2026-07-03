# Quick Tutorial 2 (Local): Filas com RabbitMQ (micro-lote → Parquet)

> Só os comandos. Explicações: `TUTORIAL.md`.
> Fluxo: `producer.py → RabbitMQ (vendas-fila) → consumer.py (micro-lote) → s3://datalake/filas/`.
> **Pré-requisito**: Tutorial 1 Local no ar (`streaming_rabbitmq` + `streaming_ministack`).

---

## 1. Conferir o Tutorial 1 no ar

```bash
docker compose -f tutoriais/streaming/1-infraestrutura/local/docker/docker-compose.yml ps
# streaming_rabbitmq e streaming_ministack -> Up (healthy)
```

UI do RabbitMQ: <http://localhost:15672> (guest/guest).

---

## 2. Ambiente Python (venv + libs)

```bash
cd tutoriais/streaming/2-filas/local
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install pika boto3 pyarrow
```

---

## 3. producer.py (Python → RabbitMQ)

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

Rodar (deixe rodando em um terminal):

```bash
python producer.py          # 5 eventos/s (ou: python producer.py 20)
```

---

## 4. consumer.py (RabbitMQ → micro-lote → Parquet)

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

Rodar (em outro terminal, mesmo venv):

```bash
python consumer.py
# flush 50 eventos -> s3://datalake/filas/dt=2026-07-02/lote-a1b2c3d4.parquet
```

---

## 5. Verificar no S3 (MiniStack)

```bash
# macOS / Linux
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required

aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/filas/ --recursive
```

```powershell
# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="test"; $env:AWS_SECRET_ACCESS_KEY="test"
$env:AWS_DEFAULT_REGION="us-east-1"; $env:AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/filas/ --recursive
```

Baixar e ler um lote (7 colunas, 50 linhas):

```bash
aws --endpoint-url http://localhost:4566 s3 cp \
  "$(aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/filas/dt=$(date -u +%F)/ | awk '{print $4}' | head -1 | sed 's#^#s3://datalake/filas/dt='$(date -u +%F)'/#')" /tmp/lote.parquet
python -c "import pyarrow.parquet as pq; t=pq.read_table('/tmp/lote.parquet'); print(t.column_names); print(t.num_rows)"
# ['evento_id','cliente_id','produto_id','categoria','quantidade','valor_total','data_venda']
# 50
```

---

## 6. Parar / limpar

```bash
# Ctrl+C no consumer (faz flush final) e no producer.
docker exec streaming_rabbitmq rabbitmqctl purge_queue vendas-fila   # esvaziar a fila (opcional)
```

> Deixe os containers do Tutorial 1 rodando para seguir aos Tutoriais 3 (Kafka+Spark) e 4 (Kafka+Flink).

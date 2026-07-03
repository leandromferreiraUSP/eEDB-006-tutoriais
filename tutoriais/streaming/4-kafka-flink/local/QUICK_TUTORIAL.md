# Quick Tutorial 4 (Local): Kafka + Flink SQL (janela 30s → Parquet)

> Só os comandos. Explicações: `TUTORIAL.md`.
> Fluxo: `producer.py → Kafka (vendas) → Flink SQL (TUMBLE 30s) → s3://datalake/flink/`.
> Mesmo contrato de saída do Tutorial 3 (Spark) — de propósito, para comparar.
> **Pré-requisito**: Tutorial 1 Local no ar (`streaming_kafka`, `streaming_flink_jobmanager`,
> `streaming_flink_taskmanager`, `streaming_ministack`) e tópico `vendas` criado.

---

## 1. Conferir o Tutorial 1 no ar + tópico

```bash
docker compose -f tutoriais/streaming/1-infraestrutura/local/docker/docker-compose.yml ps
# kafka + flink jobmanager/taskmanager + ministack -> Up

docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vendas --partitions 1 --replication-factor 1
```

Web UI do Flink: <http://localhost:8081> (aba **Running Jobs**).

---

## 2. Ambiente Python (venv + lib)

```bash
cd tutoriais/streaming/4-kafka-flink/local
python3 -m venv .venv
source .venv/bin/activate                 # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install confluent-kafka
```

---

## 3. producer.py (Python → Kafka) — o MESMO do Tutorial 3

```python
import json, random, time, uuid, sys
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = "localhost:29092"
TOPIC = "vendas"
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

p = Producer({"bootstrap.servers": BOOTSTRAP})
n = 0
try:
    while True:
        ev = gerar_evento()
        p.produce(TOPIC, key=ev["evento_id"], value=json.dumps(ev)); p.poll(0)
        n += 1
        if n % 10 == 0: print(f"{n} eventos enviados", flush=True)
        time.sleep(1.0/EPS)
except KeyboardInterrupt:
    pass
finally:
    p.flush(5); print(f"total: {n}")
```

Rodar (deixe rodando em um terminal):

```bash
python producer.py 8        # 8 eventos/s
```

---

## 4. Flink SQL (cole no SQL Client)

Abra o SQL Client (Terminal 2, com o producer rodando):

```bash
docker exec -it streaming_flink_jobmanager /opt/flink/bin/sql-client.sh
```

Cole os `SET`, os dois `CREATE TABLE` e o `INSERT` (nesta ordem):

```sql
-- Config da sessão (checkpointing é OBRIGATÓRIO p/ o filesystem sink commitar os Parquet)
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'parallelism.default' = '1';

-- 1) SOURCE: tópico Kafka 'vendas'
CREATE TABLE vendas_kafka (
  evento_id   STRING,
  cliente_id  INT,
  produto_id  INT,
  categoria   STRING,
  quantidade  INT,
  valor_total DOUBLE,
  data_venda  TIMESTAMP(3),
  WATERMARK FOR data_venda AS data_venda - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'vendas',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-vendas',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- 2) SINK: Parquet no S3 (MiniStack), particionado por dt
CREATE TABLE agg_s3 (
  categoria     STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  qtd_eventos   BIGINT,
  faturamento   DOUBLE,
  qtd_itens     BIGINT,
  dt            STRING
) PARTITIONED BY (dt) WITH (
  'connector' = 'filesystem',
  'path' = 's3://datalake/flink/',
  'format' = 'parquet',
  'sink.partition-commit.policy.kind' = 'success-file'
);

-- 3) JOB: janela tumbling de 30s por categoria (event-time) -> S3
INSERT INTO agg_s3
SELECT
  categoria,
  window_start,
  window_end,
  COUNT(*)          AS qtd_eventos,
  SUM(valor_total)  AS faturamento,
  SUM(quantidade)   AS qtd_itens,
  DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt
FROM TABLE(
  TUMBLE(TABLE vendas_kafka, DESCRIPTOR(data_venda), INTERVAL '30' SECOND)
)
GROUP BY categoria, window_start, window_end;
```

- Cada `SET`/`CREATE` retorna `[INFO] Execute statement succeeded.`
- O `INSERT` retorna um `Job ID` e dispara um **job de streaming contínuo** (veja em
  <http://localhost:8081>).

**Alternativa não-interativa** (salve o SQL acima em
`1-infraestrutura/local/docker/work/flink_vendas.sql`, que é a pasta montada como `/work`):

```bash
docker exec streaming_flink_jobmanager /opt/flink/bin/sql-client.sh -f /work/flink_vendas.sql
```

---

## 5. Verificar no S3 (MiniStack)

Após ~60–90s (janelas fechando):

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/flink/ --recursive
# flink/dt=YYYY-MM-DD/part-*   e   flink/dt=YYYY-MM-DD/_SUCCESS
```

Baixar e ler um `part` (6 colunas, uma linha por categoria por janela de 30s):

```bash
pip install pyarrow
aws --endpoint-url http://localhost:4566 s3 cp \
  "$(aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/flink/dt=$(date -u +%F)/ | awk '{print $4}' | grep '^part' | head -1 | sed 's#^#s3://datalake/flink/dt='$(date -u +%F)'/#')" /tmp/janela.parquet
python -c "import pyarrow.parquet as pq; t=pq.read_table('/tmp/janela.parquet'); print(t.column_names); print(t.to_pylist()[:2])"
# ['categoria','window_start','window_end','qtd_eventos','faturamento','qtd_itens']
# [{'categoria':'Eletronicos','window_start':'2026-07-02 21:05:00','window_end':'2026-07-02 21:05:30', ...}]
```

`window_end - window_start = 30s`. **Mesmo contrato do Tutorial 3 (Spark).**

---

## 6. Parar / cancelar

```bash
docker exec streaming_flink_jobmanager /opt/flink/bin/flink list          # pegue o Job ID
docker exec streaming_flink_jobmanager /opt/flink/bin/flink cancel <JobID>
# (ou botão Cancel na UI http://localhost:8081). Depois Ctrl+C no producer.
```

> **Nada no S3?** Faltou `SET 'execution.checkpointing.interval' = '10 s'` (o sink só commita no
> checkpoint) ou o producer não estava rodando (`scan.startup.mode = latest-offset`).

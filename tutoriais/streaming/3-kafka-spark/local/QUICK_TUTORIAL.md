# Quick Tutorial 3 (Local): Kafka + Spark (janela de 30s)

> Só os comandos. Explicações: `TUTORIAL.md`.
> Resultado: eventos do tópico `vendas` agregados em janelas de 30s por categoria → Parquet em
> `s3://datalake/spark/`.

Pré-requisito: Tutorial 1 Local no ar (Kafka, Spark, MiniStack).

---

## 1. Tópico + venv do producer

```bash
docker exec streaming_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic vendas --partitions 1 --replication-factor 1

cd tutoriais/streaming/3-kafka-spark/local
python3 -m venv .venv && source .venv/bin/activate   # Win: .venv\Scripts\Activate.ps1
pip install confluent-kafka
```

---

## 2. Producer (crie `producer.py` — código no TUTORIAL.md §5) e rode

```bash
python producer.py 8        # deixe rodando
```

---

## 3. Consumer Spark

Crie `1-infraestrutura/local/docker/work/consumer_spark.py` (código no TUTORIAL.md §6; use
`.config("spark.sql.shuffle.partitions","4")`, janela 30s + watermark 10s, sink Parquet em
`s3a://datalake/spark/`, checkpoint em `/work/checkpoint_spark`).

Submeta (em outro terminal):

```bash
docker exec -it streaming_spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.endpoint=http://ministack:4566 \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /work/consumer_spark.py
```

> 1ª execução baixa `hadoop-aws` + `aws-java-sdk-bundle` (~200 MB, ~1–2 min). Cacheado em `/tmp/.ivy2`.

---

## 4. Verificar no S3 (após ~60–90s)

```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/spark/ --recursive
# -> spark/dt=YYYY-MM-DD/part-*.snappy.parquet
```

Ler um arquivo:

```bash
pip install pyarrow
aws --endpoint-url http://localhost:4566 s3 cp \
  "$(aws --endpoint-url http://localhost:4566 s3 ls s3://datalake/spark/dt=$(date -u +%F)/ | awk '{print $4}' | head -1 | sed 's#^#s3://datalake/spark/dt='$(date -u +%F)'/#')" /tmp/janela.parquet
python -c "import pyarrow.parquet as pq; print(pq.read_table('/tmp/janela.parquet').to_pylist())"
# -> [{'categoria':'Alimentos','window_start':..:00,'window_end':..:30,'qtd_eventos':72,'faturamento':4550.0,'qtd_itens':218}]
```

---

## 5. Parar

`Ctrl+C` no `spark-submit` e no producer. Reprocessar do zero:
`docker exec streaming_spark rm -rf /work/checkpoint_spark`.

| Problema | Solução |
|---|---|
| `FileNotFoundException ...ivy2/cache` | `--conf spark.jars.ivy=/tmp/.ivy2` |
| `No FileSystem for scheme "s3a"` | inclua `hadoop-aws:3.3.4` no `--packages` |
| Nada no S3 | producer rodando? espere > 40s (janela 30s + watermark 10s) |

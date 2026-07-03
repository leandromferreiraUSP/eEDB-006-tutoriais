# Dockerfile -- Flink 1.20 com os conectores necessários para o Tutorial 4 (Kafka + Flink SQL).
#
# A imagem oficial do Flink NÃO traz os conectores de SQL. Adicionamos, em /opt/flink/lib:
#   - flink-sql-connector-kafka   : ler o tópico Kafka como TABLE (source)
#   - flink-sql-parquet           : formato Parquet para o connector 'filesystem' (sink)
#   - flink-shaded-hadoop-2-uber  : classes do Hadoop (org.apache.hadoop.conf.Configuration)
#                                   exigidas pelo escritor Parquet (parquet-hadoop)
# E habilitamos o plugin de sistema de arquivos S3 (para o path 's3://...') copiando o
# flink-s3-fs-hadoop (que já vem em /opt/flink/opt) para a pasta de plugins.
#
# Tudo isso é infraestrutura (dependências), não código de aplicação.

FROM flink:1.20.1-scala_2.12-java17

# Versões dos conectores (pinadas). O sufixo "-1.20" casa com a versão do Flink.
ARG FLINK_VERSION=1.20.1
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG HADOOP_UBER_VERSION=2.8.3-10.0

# apt-get precisa de root; instalamos 'zip' só para remover um jar-in-jar conflitante (ver abaixo).
USER root

RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends zip; \
    # Conector Kafka para Flink SQL
    wget -nv -O /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
    # Formato Parquet para o connector filesystem
    wget -nv -O /opt/flink/lib/flink-sql-parquet-${FLINK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/${FLINK_VERSION}/flink-sql-parquet-${FLINK_VERSION}.jar; \
    # Classes do Hadoop exigidas pelo escritor Parquet (Configuration, Path etc.);
    # o acesso ao S3 continua indo pelo PLUGIN s3-fs-hadoop, isolado.
    wget -nv -O /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar; \
    # IMPORTANTE: o uber jar traz um commons-cli ANTIGO (< 1.5) que, no classpath do CLI do Flink,
    # sobrepõe o do flink-dist e QUEBRA o comando 'flink cancel'
    # (NoSuchMethodError: CommandLine.hasOption(Option)). O escritor Parquet NÃO usa commons-cli,
    # então removemos essas classes do uber jar — o CLI volta a usar o commons-cli 1.5 do flink-dist.
    zip -q -d /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar 'org/apache/commons/cli/*'; \
    # Habilita o plugin S3 (hadoop) movendo o jar de /opt para a pasta de plugins
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION}.jar /opt/flink/plugins/s3-fs-hadoop/; \
    # limpeza
    apt-get purge -y zip; apt-get autoremove -y; rm -rf /var/lib/apt/lists/*

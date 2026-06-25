-- schema.sql -- DDL das tabelas de origem (e-commerce) usadas nos tutoriais de ingestão.
-- Executado automaticamente pelo container Postgres (docker-entrypoint-initdb.d)
-- e manualmente no RDS (psql -f schema.sql).

DROP TABLE IF EXISTS vendas;
DROP TABLE IF EXISTS clientes;
DROP TABLE IF EXISTS produtos;

CREATE TABLE clientes (
    cliente_id    INTEGER PRIMARY KEY,
    nome          TEXT        NOT NULL,
    email         TEXT        NOT NULL,
    cidade        TEXT        NOT NULL,
    estado        CHAR(2)     NOT NULL,
    data_cadastro DATE        NOT NULL
);

CREATE TABLE produtos (
    produto_id INTEGER       PRIMARY KEY,
    nome       TEXT          NOT NULL,
    categoria  TEXT          NOT NULL,
    preco      NUMERIC(10,2) NOT NULL
);

CREATE TABLE vendas (
    venda_id    INTEGER       PRIMARY KEY,
    cliente_id  INTEGER       NOT NULL REFERENCES clientes(cliente_id),
    produto_id  INTEGER       NOT NULL REFERENCES produtos(produto_id),
    quantidade  INTEGER       NOT NULL,
    valor_total NUMERIC(12,2) NOT NULL,
    data_venda  TIMESTAMP     NOT NULL
);

CREATE INDEX idx_vendas_data ON vendas (data_venda);

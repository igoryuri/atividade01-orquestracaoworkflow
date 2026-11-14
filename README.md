# Atividade 01 - Pipeline ShopBrasil com Apache Airflow

Pipeline diário que coleta produtos da FakeStore API, valida os dados, calcula métricas por categoria em paralelo e persiste os resultados no PostgreSQL de forma idempotente.

## Requisitos atendidos

- TaskFlow API com `@dag` e `@task`;
- XComs automáticos por retorno;
- topologia linear, fan-out e fan-in;
- execução diária às 06:00 em `America/Sao_Paulo`;
- retry, exponential backoff, timeout e `try/except + raise`;
- callbacks de falha, retry e sucesso na task crítica;
- Dynamic Task Mapping com `.expand()`;
- pool `ecommerce_pool` com 2 slots;
- TaskGroups de ingestão, análise e persistência;
- PostgreSQL com `PostgresHook` e Connection do Airflow;
- snapshot idempotente por `(data_referencia, categoria)`;
- histórico em append lógico, idempotente por `(dag_run_id, categoria)`;
- Docker Compose.

## Pré-requisitos

- Docker Engine/Desktop;
- Docker Compose v2;
- portas locais `8080` e `5432` livres.

## Execução

```bash
cp .env.example .env
mkdir -p logs

docker compose up airflow-init

docker compose up -d postgres airflow-webserver airflow-scheduler
```

Acesse `http://localhost:8080`:

- usuário: `airflow`
- senha: `airflow`

Ative a DAG `shopbrasil_pricing_pipeline` e execute manualmente para o primeiro teste. Depois disso, o agendamento diário será às 06:00 no horário de São Paulo.

## Pool e conexão

O serviço `airflow-init` cria automaticamente:

- Connection: `shopbrasil_postgres`;
- Pool: `ecommerce_pool`, com 2 slots.

## Consultar o banco

```bash
docker compose exec postgres psql -U airflow -d shopbrasil
```

Dentro do `psql`:

```sql
SELECT * FROM pricing_categoria_snapshot ORDER BY categoria;
SELECT * FROM pricing_categoria_historico ORDER BY criado_em DESC, categoria;
```

## Demonstrar idempotência

1. Execute a DAG uma vez.
2. Limpe apenas a task `persistencia.salvar_metricas` na mesma DAG Run.
3. Execute novamente a task.
4. A chave primária do snapshot fará `UPSERT`, e a restrição `UNIQUE (dag_run_id, categoria)` impedirá repetição no histórico.

## Parar o ambiente

```bash
docker compose down
```

Para também apagar o banco:

```bash
docker compose down -v
```

## Estrutura

```text
atividade01_airflow_shopbrasil/
├── dags/
│   └── shopbrasil_pricing_pipeline.py
├── include/
│   └── consultas_validacao.sql
├── sql/
│   └── init-databases.sh
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── README.md
```

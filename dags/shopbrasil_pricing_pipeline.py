from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "shopbrasil_postgres"
POOL_NAME = "ecommerce_pool"
TIMEZONE = "America/Sao_Paulo"


def log_callback(context: dict[str, Any], event: str) -> None:
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception")
    print(
        f"[SHOPBRASIL][{event}] dag={dag_id} task={task_id} "
        f"run_id={run_id} exception={exception}"
    )


def on_failure_callback(context: dict[str, Any]) -> None:
    log_callback(context, "FAILURE")


def on_retry_callback(context: dict[str, Any]) -> None:
    log_callback(context, "RETRY")


def on_success_callback(context: dict[str, Any]) -> None:
    log_callback(context, "SUCCESS")


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Coleta produtos, calcula metricas por categoria e persiste no PostgreSQL.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "shopbrasil-data-team",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["atividade-01", "shopbrasil", "pricing"],
)
def shopbrasil_pricing_pipeline():

    @task_group(group_id="ingestao")
    def ingestao_group():
        @task(
            task_id="buscar_produtos",
            retries=4,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            execution_timeout=timedelta(minutes=3),
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback,
        )
        def buscar_produtos() -> list[dict[str, Any]]:
            # Simulacoes exclusivas de DAG Runs manuais com configuracao explicita.
            # Execucoes agendadas permanecem no comportamento normal.
            context = get_current_context()
            dag_run = context["dag_run"]
            modo_teste = (dag_run.conf or {}).get("modo_teste", "normal")
            if modo_teste not in {"normal", "falha_uma_vez", "falha_permanente"}:
                raise AirflowException(f"Modo de teste invalido: {modo_teste}")
            if dag_run.run_type == "manual":
                tentativa = context["ti"].try_number
                if modo_teste == "falha_permanente":
                    raise AirflowException(
                        f"[TESTE CONTROLADO] Falha permanente na tentativa {tentativa}."
                    )
                if modo_teste == "falha_uma_vez" and tentativa == 1:
                    raise AirflowException(
                        "[TESTE CONTROLADO] Falha temporaria na primeira tentativa."
                    )
            try:
                response = requests.get(API_URL, timeout=30)
                response.raise_for_status()
                produtos = response.json()
                if not isinstance(produtos, list) or not produtos:
                    raise ValueError("A API retornou uma lista vazia ou formato inesperado.")
                return produtos
            except (requests.RequestException, ValueError, TypeError) as exc:
                print(f"Falha controlada ao buscar produtos: {exc}")
                raise AirflowException("Nao foi possivel obter produtos da FakeStore API.") from exc

        @task(task_id="validar_produtos")
        def validar_produtos(produtos: list[dict[str, Any]]) -> list[dict[str, Any]]:
            campos = {"id", "title", "price", "category"}
            validos: list[dict[str, Any]] = []
            erros: list[str] = []
            for indice, produto in enumerate(produtos):
                ausentes = campos - produto.keys()
                if ausentes:
                    erros.append(f"indice={indice}, campos_ausentes={sorted(ausentes)}")
                    continue
                try:
                    preco = float(produto["price"])
                    if preco < 0 or not str(produto["category"]).strip():
                        raise ValueError("preco negativo ou categoria vazia")
                except (TypeError, ValueError) as exc:
                    erros.append(f"indice={indice}, erro={exc}")
                    continue
                validos.append(produto)
            if erros:
                raise AirflowException("Produtos invalidos: " + "; ".join(erros[:10]))
            return validos

        produtos = buscar_produtos()
        return validar_produtos(produtos)

    @task_group(group_id="analise")
    def analise_group(produtos: list[dict[str, Any]]):
        @task(task_id="listar_categorias")
        def listar_categorias(produtos_validos: list[dict[str, Any]]) -> list[dict[str, Any]]:
            categorias = sorted({str(p["category"]) for p in produtos_validos})
            return [
                {"categoria": categoria, "produtos": produtos_validos}
                for categoria in categorias
            ]

        @task(task_id="calcular_metricas_categoria", pool=POOL_NAME)
        def calcular_metricas_categoria(item: dict[str, Any]) -> dict[str, Any]:
            categoria = item["categoria"]
            precos = [
                float(p["price"])
                for p in item["produtos"]
                if p["category"] == categoria
            ]
            if not precos:
                raise AirflowException(f"Categoria sem produtos: {categoria}")
            return {
                "categoria": categoria,
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
                "quantidade_produtos": len(precos),
            }

        @task(task_id="consolidar_metricas")
        def consolidar_metricas(metricas: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return sorted(metricas, key=lambda registro: registro["categoria"])

        itens = listar_categorias(produtos)
        metricas_mapeadas = calcular_metricas_categoria.expand(item=itens)
        return consolidar_metricas(metricas_mapeadas)

    @task_group(group_id="persistencia")
    def persistencia_group(metricas: list[dict[str, Any]]):
        @task(task_id="criar_tabelas")
        def criar_tabelas() -> None:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            hook.run(
                """
                CREATE TABLE IF NOT EXISTS pricing_categoria_snapshot (
                    data_referencia DATE NOT NULL,
                    categoria TEXT NOT NULL,
                    preco_medio NUMERIC(12, 2) NOT NULL,
                    preco_minimo NUMERIC(12, 2) NOT NULL,
                    preco_maximo NUMERIC(12, 2) NOT NULL,
                    quantidade_produtos INTEGER NOT NULL,
                    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (data_referencia, categoria)
                );

                CREATE TABLE IF NOT EXISTS pricing_categoria_historico (
                    id BIGSERIAL PRIMARY KEY,
                    dag_run_id TEXT NOT NULL,
                    data_referencia DATE NOT NULL,
                    categoria TEXT NOT NULL,
                    preco_medio NUMERIC(12, 2) NOT NULL,
                    preco_minimo NUMERIC(12, 2) NOT NULL,
                    preco_maximo NUMERIC(12, 2) NOT NULL,
                    quantidade_produtos INTEGER NOT NULL,
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (dag_run_id, categoria)
                );
                """
            )

        @task(task_id="salvar_metricas")
        def salvar_metricas(metricas_consolidadas: list[dict[str, Any]], **context) -> int:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            data_referencia = context["logical_date"].in_timezone(TIMEZONE).date()
            dag_run_id = context["run_id"]
            snapshot_sql = """
                INSERT INTO pricing_categoria_snapshot (
                    data_referencia, categoria, preco_medio, preco_minimo,
                    preco_maximo, quantidade_produtos, atualizado_em
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (data_referencia, categoria) DO UPDATE SET
                    preco_medio = EXCLUDED.preco_medio,
                    preco_minimo = EXCLUDED.preco_minimo,
                    preco_maximo = EXCLUDED.preco_maximo,
                    quantidade_produtos = EXCLUDED.quantidade_produtos,
                    atualizado_em = NOW();
            """
            historico_sql = """
                INSERT INTO pricing_categoria_historico (
                    dag_run_id, data_referencia, categoria, preco_medio,
                    preco_minimo, preco_maximo, quantidade_produtos
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dag_run_id, categoria) DO NOTHING;
            """
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    for reg in metricas_consolidadas:
                        valores_snapshot = (
                            data_referencia, reg["categoria"], reg["preco_medio"],
                            reg["preco_minimo"], reg["preco_maximo"],
                            reg["quantidade_produtos"],
                        )
                        cursor.execute(snapshot_sql, valores_snapshot)
                        cursor.execute(
                            historico_sql,
                            (dag_run_id,) + valores_snapshot,
                        )
                conn.commit()
            return len(metricas_consolidadas)

        tabelas = criar_tabelas()
        gravacao = salvar_metricas(metricas)
        tabelas >> gravacao
        return gravacao

    @task(task_id="resumo_execucao", trigger_rule=TriggerRule.ALL_SUCCESS)
    def resumo_execucao(quantidade_categorias: int) -> None:
        print(f"Pipeline concluido. Categorias persistidas: {quantidade_categorias}")

    produtos_validos = ingestao_group()
    metricas_consolidadas = analise_group(produtos_validos)
    quantidade = persistencia_group(metricas_consolidadas)
    resumo_execucao(quantidade)


shopbrasil_pricing_pipeline()

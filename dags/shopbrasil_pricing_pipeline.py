"""Pipeline de pricing da ShopBrasil.

Coleta produtos da FakeStore API, valida o schema, calcula metricas de preco
por categoria em paralelo e persiste o resultado no PostgreSQL.

Topologia:
    LINEAR   ingestao -> analise -> persistencia -> resumo_execucao
    FAN-OUT  calcular_metricas_categoria.expand(...)  (uma task por categoria)
    FAN-IN   consolidar_metricas                      (junta as metricas)
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from operators.validar_produtos_operator import ValidarProdutosOperator

API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "shopbrasil_postgres"
POOL_NAME = "ecommerce_pool"
TIMEZONE = "America/Sao_Paulo"
CANAL_ALERTAS = "#pricing-alertas"

logger = logging.getLogger("airflow.task")


# ---------------------------------------------------------------------------
# Callbacks de ciclo de vida
# ---------------------------------------------------------------------------
def _descrever_contexto(context: dict[str, Any]) -> str:
    ti = context.get("task_instance")
    dag_obj = context.get("dag")
    return (
        f"dag={dag_obj.dag_id if dag_obj else 'desconhecida'} "
        f"task={ti.task_id if ti else 'desconhecida'} "
        f"run_id={context.get('run_id', 'desconhecido')} "
        f"tentativa={ti.try_number if ti else '?'}"
    )


def on_failure_callback(context: dict[str, Any]) -> None:
    """Simula o envio de um alerta (Slack/e-mail) quando uma task falha."""
    ti = context.get("task_instance")
    mensagem = (
        f"[ALERTA SIMULADO] envio para {CANAL_ALERTAS}: falha no pipeline de pricing. "
        f"{_descrever_contexto(context)} exception={context.get('exception')} "
        f"log={ti.log_url if ti else 'n/a'}"
    )
    logger.error(mensagem)


def on_retry_callback(context: dict[str, Any]) -> None:
    logger.warning(
        "[RETRY] %s exception=%s", _descrever_contexto(context), context.get("exception")
    )


def on_success_callback(context: dict[str, Any]) -> None:
    logger.info("[SUCCESS] %s", _descrever_contexto(context))


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Simula o alerta de SLA estourado (painel nao ficara pronto a tempo)."""
    logger.error(
        "[ALERTA SIMULADO] envio para %s: SLA estourado na DAG %s. Tasks: %s",
        CANAL_ALERTAS,
        dag.dag_id,
        task_list,
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Coleta produtos, calcula metricas por categoria e persiste no PostgreSQL.",
    schedule="0 6 * * *",  # todo dia as 06:00 no fuso do start_date (Sao Paulo)
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    default_args={
        "owner": "shopbrasil-data-team",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        # Toda task avisa quando falha.
        "on_failure_callback": on_failure_callback,
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
            """Busca o catalogo na API (task critica).

            Para demonstrar retry/callbacks, uma run MANUAL pode receber o conf
            {"modo_teste": "falha_uma_vez"} ou {"modo_teste": "falha_permanente"}.
            Runs agendadas sempre usam o comportamento normal.
            """
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
                logger.warning("Falha ao buscar produtos: %s", exc)
                # raise -> a task falha e o Airflow aplica retry com backoff
                raise AirflowException("Nao foi possivel obter produtos da FakeStore API.") from exc

        # Operador customizado: valida o schema antes do processamento.
        return ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=buscar_produtos(),
        ).output

    @task_group(group_id="analise")
    def analise_group(produtos: list[dict[str, Any]]):
        @task(task_id="listar_categorias")
        def listar_categorias(produtos_validos: list[dict[str, Any]]) -> list[dict[str, Any]]:
            """Agrupa os precos por categoria (XCom pequeno: so categoria + precos)."""
            precos_por_categoria: dict[str, list[float]] = {}
            for produto in produtos_validos:
                precos_por_categoria.setdefault(str(produto["category"]), []).append(
                    float(produto["price"])
                )
            return [
                {"categoria": categoria, "precos": precos}
                for categoria, precos in sorted(precos_por_categoria.items())
            ]

        @task(task_id="calcular_metricas_categoria", pool=POOL_NAME)
        def calcular_metricas_categoria(item: dict[str, Any]) -> dict[str, Any]:
            categoria = item["categoria"]
            precos = item["precos"]
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
        # FAN-OUT: uma task mapeada por categoria (limitada pelo pool de 2 slots)
        metricas_mapeadas = calcular_metricas_categoria.expand(item=itens)
        # FAN-IN: consolida o resultado de todas as categorias
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
            """Grava snapshot (UPSERT) e historico (append por run), de forma idempotente."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            # data_interval_end = momento em que a run agendada dispara (06:00 do dia);
            # em runs manuais equivale ao momento do trigger.
            data_referencia = context["data_interval_end"].in_timezone(TIMEZONE).date()
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
                        cursor.execute(historico_sql, (dag_run_id,) + valores_snapshot)
                conn.commit()
            return len(metricas_consolidadas)

        tabelas = criar_tabelas()
        gravacao = salvar_metricas(metricas)
        tabelas >> gravacao
        return gravacao

    # SLA: o painel precisa estar pronto ate 1h depois do disparo (07:00).
    # (Airflow 2.x nao aceita SLA em tasks mapeadas, por isso fica so na task final.)
    @task(task_id="resumo_execucao", sla=timedelta(hours=1))
    def resumo_execucao(quantidade_categorias: int) -> None:
        logger.info("Pipeline concluido. Categorias persistidas: %s", quantidade_categorias)

    # LINEAR: ingestao -> analise -> persistencia -> resumo
    produtos_validos = ingestao_group()
    metricas_consolidadas = analise_group(produtos_validos)
    quantidade = persistencia_group(metricas_consolidadas)
    resumo_execucao(quantidade)


shopbrasil_pricing_pipeline()

"""
dags/upload/open_meteo/dag_agriculture.py
------------------------------------------
DAG que extrai previsão agrícola do Open-Meteo e carrega
na tabela PREVISAO_AGRICOLA do Oracle XE.

A cada execução diária um novo snapshot de previsão (16 dias à frente)
é inserido — por isso usa INSERT simples, não MERGE.

Fluxo:
    extrair_transformar_task  >>  carregar_oracle_task

Staging intermediário: data/open_meteo/agriculture/staging_{ds}.json
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.open_meteo.agriculture.agriculture import extrair_todas_regioes
from loader.oracle_loader import OracleLoader
from utils.json_utils import salvar_staging, carregar_staging

logger = logging.getLogger(__name__)

STAGING_DIR = PROJECT_ROOT / "data" / "open_meteo" / "agriculture"


# ── Tasks ──────────────────────────────────────────────────────────────────────

def _extrair_transformar(**context) -> str:
    ds = context["ds"]
    registros = extrair_todas_regioes()
    arquivo = STAGING_DIR / f"staging_{ds}.json"
    salvar_staging(registros, arquivo)
    logger.info(f"[dag_agriculture] Staging salvo: {arquivo} ({len(registros)} registros)")
    return str(arquivo)


def _carregar_oracle(**context) -> int:
    arquivo = context["ti"].xcom_pull(task_ids="extrair_transformar_task")
    registros = carregar_staging(Path(arquivo))
    with OracleLoader() as loader:
        total = loader.carregar_agriculture(registros)
    logger.info(f"[dag_agriculture] {total} registros carregados no Oracle.")
    return total


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="open_meteo_agriculture",
    description="Extrai previsão agrícola do Open-Meteo e carrega no Oracle",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["open_meteo", "previsao_agricola", "oracle"],
) as dag:

    extrair_transformar_task = PythonOperator(
        task_id="extrair_transformar_task",
        python_callable=_extrair_transformar,
    )

    carregar_oracle_task = PythonOperator(
        task_id="carregar_oracle_task",
        python_callable=_carregar_oracle,
    )

    extrair_transformar_task >> carregar_oracle_task

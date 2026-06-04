"""
dags/upload/nasa_power/dag_solar_radiation.py
----------------------------------------------
DAG que extrai dados de radiação solar da NASA POWER e os carrega
na tabela SOLAR_RADIATION do Oracle XE.

Fluxo:
    extrair_transformar_task >> carregar_oracle_task >> limpar_staging_task

Staging intermediário: data/nasa_power/solar_radiation/staging_{ds}.json
Acionado exclusivamente pelo trigger_master (schedule_interval=None).
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.nasa_power.solar_radiation.solar_radiation import extrair_todas_regioes
from loader.oracle_loader import OracleLoader
from utils.json_utils import salvar_staging, carregar_staging

logger = logging.getLogger(__name__)

STAGING_DIR = PROJECT_ROOT / "data" / "nasa_power" / "solar_radiation"
TABELA = "SOLAR_RADIATION"


# ── Tasks ──────────────────────────────────────────────────────────────────────

def _extrair_transformar(**context) -> str:
    ds = context["ds"]
    registros = extrair_todas_regioes()
    if not registros:
        raise ValueError(f"[{TABELA}] Nenhum registro extraído da API. Verifique a conexão.")
    arquivo = STAGING_DIR / f"staging_{ds}.json"
    salvar_staging(registros, arquivo)
    logger.info(f"[dag_solar_radiation] Staging salvo: {arquivo} ({len(registros)} registros)")
    return str(arquivo)


def _carregar_oracle(**context) -> int:
    arquivo = context["ti"].xcom_pull(task_ids="extrair_transformar_task")
    registros = carregar_staging(Path(arquivo))
    with OracleLoader() as loader:
        total = loader.carregar_solar_radiation(registros)
        loader.registrar_execucao(context["dag"].dag_id, TABELA, total)
    logger.info(f"[dag_solar_radiation] {total} registros carregados no Oracle.")
    return total


def _limpar_staging(**context):
    arquivo = context["ti"].xcom_pull(task_ids="extrair_transformar_task")
    Path(arquivo).unlink(missing_ok=True)
    logger.info(f"[dag_solar_radiation] Staging removido: {arquivo}")


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="nasa_power_solar_radiation",
    description="Extrai dados de radiação solar da NASA POWER e carrega no Oracle",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nasa_power", "solar_radiation", "oracle"],
) as dag:

    extrair_transformar_task = PythonOperator(
        task_id="extrair_transformar_task",
        python_callable=_extrair_transformar,
    )

    carregar_oracle_task = PythonOperator(
        task_id="carregar_oracle_task",
        python_callable=_carregar_oracle,
    )

    limpar_staging_task = PythonOperator(
        task_id="limpar_staging_task",
        python_callable=_limpar_staging,
    )

    extrair_transformar_task >> carregar_oracle_task >> limpar_staging_task

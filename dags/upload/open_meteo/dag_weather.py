"""
dags/upload/open_meteo/dag_weather.py
--------------------------------------
Pipeline: Open-Meteo → staging JSON → WEATHER (Oracle)
Acionado pelo trigger_master. Lógica das tasks em utils/dag_factory.py.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.open_meteo.weather.weather import extrair_todas_regioes
from utils.dag_factory import criar_tasks

extrair_fn, carregar_fn, limpar_fn = criar_tasks(
    extrair_fn=extrair_todas_regioes,
    staging_dir=PROJECT_ROOT / "data" / "open_meteo" / "weather",
    tabela="WEATHER",
    loader_method="carregar_weather",
)

with DAG(
    dag_id="open_meteo_weather",
    description="Extrai histórico climático do Open-Meteo e carrega no Oracle",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["open_meteo", "weather", "oracle"],
) as dag:

    extrair_transformar_task = PythonOperator(
        task_id="extrair_transformar_task",
        python_callable=extrair_fn,
    )
    carregar_oracle_task = PythonOperator(
        task_id="carregar_oracle_task",
        python_callable=carregar_fn,
    )
    limpar_staging_task = PythonOperator(
        task_id="limpar_staging_task",
        python_callable=limpar_fn,
    )

    extrair_transformar_task >> carregar_oracle_task >> limpar_staging_task

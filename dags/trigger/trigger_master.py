"""
dags/trigger/trigger_master.py
-------------------------------
DAG mestre que dispara os 4 pipelines individuais em paralelo.
Roda todos os dias às 06:00 no horário de São Paulo.

Topologia:
    [trigger_agro_weather, trigger_solar_radiation,
     trigger_open_meteo_weather, trigger_open_meteo_agriculture]
"""

import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="trigger_master",
    description="Orquestrador principal — dispara os 4 pipelines às 06h (São Paulo)",
    schedule_interval="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["master", "orquestracao"],
) as dag:

    trigger_agro_weather = TriggerDagRunOperator(
        task_id="trigger_agro_weather",
        trigger_dag_id="nasa_power_agro_weather",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    trigger_solar_radiation = TriggerDagRunOperator(
        task_id="trigger_solar_radiation",
        trigger_dag_id="nasa_power_solar_radiation",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    trigger_open_meteo_weather = TriggerDagRunOperator(
        task_id="trigger_open_meteo_weather",
        trigger_dag_id="open_meteo_weather",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    trigger_open_meteo_agriculture = TriggerDagRunOperator(
        task_id="trigger_open_meteo_agriculture",
        trigger_dag_id="open_meteo_agriculture",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    # Todos os 4 rodam em paralelo (sem dependência entre si)
    [
        trigger_agro_weather,
        trigger_solar_radiation,
        trigger_open_meteo_weather,
        trigger_open_meteo_agriculture,
    ]

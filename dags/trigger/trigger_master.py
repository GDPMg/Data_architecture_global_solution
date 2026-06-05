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

    trigger_open_meteo_evapo = TriggerDagRunOperator(
        task_id="trigger_open_meteo_evapo",
        trigger_dag_id="open_meteo_evapo",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    trigger_open_meteo_agricultural_forecast = TriggerDagRunOperator(
        task_id="trigger_open_meteo_agricultural_forecast",
        trigger_dag_id="open_meteo_agricultural_forecast",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    [
        trigger_agro_weather,
        trigger_solar_radiation,
        trigger_open_meteo_evapo,
        trigger_open_meteo_agricultural_forecast,
    ]

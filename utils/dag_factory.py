import logging
from pathlib import Path
from typing import Callable

from airflow.models.param import Param
from loader.oracle_loader import OracleLoader
from utils.json_utils import salvar_staging, carregar_staging

# Parâmetro padrão para todas as DAGs históricas — importar e passar ao DAG
PARAM_MODO = {
    "modo": Param(
        "incremental",
        enum=["incremental", "full"],
        description="incremental: carrega d-1 | full: carrega últimos 120 dias",
    )
}


def criar_tasks(
    extrair_fn: Callable,
    staging_dir: Path,
    tabela: str,
    loader_method: str,
) -> tuple[Callable, Callable, Callable]:
    """
    Retorna as 3 callables prontas para uso em PythonOperator.
        extrair_fn    : função extrair_todas_regioes() do módulo de ingestão
        staging_dir   : diretório para o JSON intermediário
        tabela        : nome da tabela Oracle (ex: "AGRO_WEATHER")
        loader_method : nome do método do OracleLoader (ex: "carregar_agro_weather")
    """
    logger = logging.getLogger(__name__)
    staging_dir = Path(staging_dir)

    def extrair_transformar(**context) -> str:
        ds = context["ds"]
        modo = context.get("params", {}).get("modo", "incremental")
        logger.info(f"[{tabela}] Modo de carga: {modo}")
        registros = extrair_fn(modo=modo)
        if not registros:
            raise ValueError(f"[{tabela}] Nenhum registro extraído da API.")
        arquivo = staging_dir / f"staging_{ds}.json"
        salvar_staging(registros, arquivo)
        logger.info(f"[{tabela}] Staging salvo: {arquivo} ({len(registros)} registros)")
        return str(arquivo)

    def carregar_oracle(**context) -> int:
        arquivo = context["ti"].xcom_pull(task_ids="extrair_transformar_task")
        registros = carregar_staging(Path(arquivo))
        with OracleLoader() as loader:
            total = getattr(loader, loader_method)(registros)
            loader.registrar_execucao(context["dag"].dag_id, tabela, total)
        logger.info(f"[{tabela}] {total} registros carregados no Oracle.")
        return total

    def limpar_staging(**context) -> None:
        arquivo = context["ti"].xcom_pull(task_ids="extrair_transformar_task")
        Path(arquivo).unlink(missing_ok=True)
        logger.info(f"[{tabela}] Staging removido: {arquivo}")

    return extrair_transformar, carregar_oracle, limpar_staging

"""
utils/dag_factory.py
---------------------
Fábrica de tasks para as DAGs de ingestão.

Os 4 pipelines (agro_weather, solar_radiation, weather, agriculture) têm
exatamente o mesmo fluxo — só mudam: módulo de extração, diretório de
staging, nome da tabela e método do OracleLoader. Esta fábrica centraliza
a lógica e elimina a duplicação.

Uso em cada DAG:
    from utils.dag_factory import criar_tasks

    extrair_fn, carregar_fn, limpar_fn = criar_tasks(
        extrair_fn=extrair_todas_regioes,
        staging_dir=PROJECT_ROOT / "data" / "nasa_power" / "agro_weather",
        tabela="AGRO_WEATHER",
        loader_method="carregar_agro_weather",
    )
"""

import logging
from pathlib import Path
from typing import Callable

from loader.oracle_loader import OracleLoader
from utils.json_utils import salvar_staging, carregar_staging


def criar_tasks(
    extrair_fn: Callable,
    staging_dir: Path,
    tabela: str,
    loader_method: str,
) -> tuple[Callable, Callable, Callable]:
    """
    Retorna as 3 callables prontas para uso em PythonOperator.

    Parâmetros:
        extrair_fn    : função extrair_todas_regioes() do módulo de ingestão
        staging_dir   : diretório para o JSON intermediário
        tabela        : nome da tabela Oracle (ex: "AGRO_WEATHER")
        loader_method : nome do método do OracleLoader (ex: "carregar_agro_weather")
    """
    logger = logging.getLogger(__name__)
    staging_dir = Path(staging_dir)

    def extrair_transformar(**context) -> str:
        ds = context["ds"]
        registros = extrair_fn()
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

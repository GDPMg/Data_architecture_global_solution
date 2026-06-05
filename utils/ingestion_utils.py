"""
utils/ingestion_utils.py
------------------------
Utilitários compartilhados pelos módulos de ingestão.

Centraliza padrões que se repetem nos 4 pipelines para evitar duplicação.
"""

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def executar_para_todas_regioes(
    extrair_fn: Callable,
    transformar_fn: Callable,
    regioes: dict,
    nome_tabela: str,
    **kwargs,
) -> list[dict]:
    """
    Executa extração + transformação para todas as regiões agrícolas.

    Substitui a função extrair_todas_regioes() repetida nos 4 módulos.
    Falhas em uma região são logadas e não interrompem as demais.

    Parâmetros:
        extrair_fn    : função extrair(regiao_key, **kwargs) do módulo
        transformar_fn: função transformar(dados_api, regiao_key) do módulo
        regioes       : dict REGIOES_AGRICOLAS do cliente de API
        nome_tabela   : nome da tabela Oracle para uso nos logs
        **kwargs      : parâmetros extras passados direto para extrair_fn
                        (ex: janela_dias=90, data_inicio=..., dias_previsao=16)
    """
    todos_registros = []

    for regiao_key in regioes:
        try:
            dados_api = extrair_fn(regiao_key=regiao_key, **kwargs)
            registros = transformar_fn(dados_api, regiao_key)
            todos_registros.extend(registros)
            logger.info(f"[{nome_tabela}] {regiao_key}: {len(registros)} registros adicionados")
        except Exception as e:
            logger.error(f"[{nome_tabela}] Falha na região {regiao_key}: {e}")

    logger.info(f"[{nome_tabela}] Total geral: {len(todos_registros)} registros")
    return todos_registros


def arredondar(valor) -> Optional[float]:
    """Arredonda float para 2 casas decimais ou retorna None."""
    if valor is None:
        return None
    try:
        return round(float(valor), 2)
    except (TypeError, ValueError):
        return None

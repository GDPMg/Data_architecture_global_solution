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

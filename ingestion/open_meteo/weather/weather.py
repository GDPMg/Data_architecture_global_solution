"""
tables/open_meteo/clima_diario.py
----------------------------------
Define os parâmetros exatos da tabela CLIMA_DIARIO e transforma
a resposta da API em registros prontos para carga no Oracle.

Tabela Oracle alvo: CLIMA_DIARIO
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional
from script.api_open_meteo import OpenMeteoClient, REGIOES_AGRICOLAS

logger = logging.getLogger(__name__)


# ── Configuração da tabela ─────────────────────────────────────────────────────

NOME_TABELA = "CLIMA_DIARIO"

# Variáveis exatas que serão requisitadas na API
VARIAVEIS_DAILY = [
    "temperature_2m_max",        # Temperatura máxima (°C)
    "temperature_2m_min",        # Temperatura mínima (°C)
    "precipitation_sum",         # Precipitação total (mm)
    "relative_humidity_2m_max",  # Umidade relativa máxima (%)
    "wind_speed_10m_max",        # Velocidade máxima do vento (km/h)
]

# DDL Oracle de referência
DDL_ORACLE = """
CREATE TABLE CLIMA_DIARIO (
    id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao          VARCHAR2(100)  NOT NULL,
    data            DATE           NOT NULL,
    temp_max        NUMBER(5,2),
    temp_min        NUMBER(5,2),
    precipitacao    NUMBER(7,2),
    umidade_max     NUMBER(5,2),
    vento_max       NUMBER(6,2),
    dt_ingestao     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_clima_diario UNIQUE (regiao, data)
);
"""


# ── Funções principais ─────────────────────────────────────────────────────────

def extrair(
    regiao_key: str,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    janela_dias: int = 90,
) -> dict:
    """
    Extrai dados históricos de clima para uma região.

    Parâmetros:
        regiao_key  : chave da região (ex: "sorriso_mt")
        data_inicio : "YYYY-MM-DD" (opcional — usa janela_dias se omitido)
        data_fim    : "YYYY-MM-DD" (opcional — usa hoje se omitido)
        janela_dias : quantos dias de histórico se data_inicio não for informada

    Retorna:
        Resposta bruta da API (dict)
    """
    if data_fim is None:
        data_fim = date.today().strftime("%Y-%m-%d")
    if data_inicio is None:
        dt_inicio = date.today() - timedelta(days=janela_dias)
        data_inicio = dt_inicio.strftime("%Y-%m-%d")

    logger.info(f"[{NOME_TABELA}] Extraindo {regiao_key} de {data_inicio} até {data_fim}")

    client = OpenMeteoClient()
    return client.buscar_historico(
        regiao_key=regiao_key,
        variaveis_daily=VARIAVEIS_DAILY,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )


def transformar(dados_api: dict, regiao_key: str) -> list[dict]:
    """
    Transforma a resposta da API em lista de registros para o Oracle.

    Cada item da lista corresponde a uma linha da tabela CLIMA_DIARIO.

    Tratamentos aplicados:
        - Remoção de registros com data nula
        - Substituição de None por None (Oracle aceita NULL)
        - Arredondamento para 2 casas decimais
        - Adição de coluna regiao e dt_ingestao
    """
    regiao_nome = REGIOES_AGRICOLAS[regiao_key]["nome"]
    daily = dados_api.get("daily", {})

    datas        = daily.get("time", [])
    temp_max     = daily.get("temperature_2m_max", [])
    temp_min     = daily.get("temperature_2m_min", [])
    precipitacao = daily.get("precipitation_sum", [])
    umidade_max  = daily.get("relative_humidity_2m_max", [])
    vento_max    = daily.get("wind_speed_10m_max", [])

    registros = []
    dt_ingestao = datetime.now()
    total_nulos = 0

    for i, data_str in enumerate(datas):
        if not data_str:
            total_nulos += 1
            continue

        registro = {
            "regiao":       regiao_nome,
            "data":         datetime.strptime(data_str, "%Y-%m-%d").date(),
            "temp_max":     _arredondar(temp_max[i] if i < len(temp_max) else None),
            "temp_min":     _arredondar(temp_min[i] if i < len(temp_min) else None),
            "precipitacao": _arredondar(precipitacao[i] if i < len(precipitacao) else None),
            "umidade_max":  _arredondar(umidade_max[i] if i < len(umidade_max) else None),
            "vento_max":    _arredondar(vento_max[i] if i < len(vento_max) else None),
            "dt_ingestao":  dt_ingestao,
        }
        registros.append(registro)

    logger.info(
        f"[{NOME_TABELA}] Transformação concluída: {len(registros)} registros "
        f"({total_nulos} datas nulas ignoradas)"
    )
    return registros


def extrair_todas_regioes(
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    janela_dias: int = 90,
) -> list[dict]:
    """
    Conveniência: extrai e transforma todas as regiões de uma vez.
    Ideal para uso direto na DAG do Airflow.

    Retorna lista unificada de registros de todas as regiões.
    """
    todos_registros = []

    for regiao_key in REGIOES_AGRICOLAS:
        try:
            dados_api = extrair(
                regiao_key=regiao_key,
                data_inicio=data_inicio,
                data_fim=data_fim,
                janela_dias=janela_dias,
            )
            registros = transformar(dados_api, regiao_key)
            todos_registros.extend(registros)
            logger.info(f"[{NOME_TABELA}] {regiao_key}: {len(registros)} registros adicionados")
        except Exception as e:
            # Falha em uma região não deve parar as demais
            logger.error(f"[{NOME_TABELA}] Falha na região {regiao_key}: {e}")

    logger.info(f"[{NOME_TABELA}] Total geral: {len(todos_registros)} registros")
    return todos_registros


# ── Helpers ────────────────────────────────────────────────────────────────────

def _arredondar(valor) -> Optional[float]:
    """Arredonda float para 2 casas ou retorna None."""
    if valor is None:
        return None
    try:
        return round(float(valor), 2)
    except (TypeError, ValueError):
        return None
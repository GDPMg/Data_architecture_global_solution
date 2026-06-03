"""
tables/open_meteo/previsao_agricola.py
---------------------------------------
Define os parâmetros exatos da tabela PREVISAO_AGRICOLA e transforma
a resposta da API em registros prontos para carga no Oracle.

Tabela Oracle alvo: PREVISAO_AGRICOLA
"""

import logging
from datetime import datetime, date
from typing import Optional
from script.api_open_meteo import OpenMeteoClient, REGIOES_AGRICOLAS

logger = logging.getLogger(__name__)


# ── Configuração da tabela ─────────────────────────────────────────────────────

NOME_TABELA = "PREVISAO_AGRICOLA"
DIAS_PREVISAO_PADRAO = 16

# Variáveis exatas que serão requisitadas na API
VARIAVEIS_DAILY = [
    "et0_fao_evapotranspiration",    # Evapotranspiração de referência (mm)
    "precipitation_sum",             # Precipitação prevista (mm)
    "temperature_2m_max",            # Temperatura máxima prevista (°C)
    "shortwave_radiation_sum",       # Radiação solar total (MJ/m²)
    "precipitation_probability_max", # Probabilidade de precipitação (%)
]

# DDL Oracle de referência
DDL_ORACLE = """
CREATE TABLE PREVISAO_AGRICOLA (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data_previsao           DATE           NOT NULL,
    et0_evapotranspiracao   NUMBER(7,2),
    precipitacao_prevista   NUMBER(7,2),
    temp_max_prevista       NUMBER(5,2),
    radiacao_solar          NUMBER(8,2),
    prob_precipitacao       NUMBER(5,2),
    dt_ingestao             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_previsao_agricola UNIQUE (regiao, data_previsao, dt_ingestao)
);
"""


# ── Funções principais ─────────────────────────────────────────────────────────

def extrair(
    regiao_key: str,
    dias_previsao: int = DIAS_PREVISAO_PADRAO,
) -> dict:
    """
    Extrai previsão agrícola para os próximos dias para uma região.

    Parâmetros:
        regiao_key    : chave da região (ex: "cascavel_pr")
        dias_previsao : quantos dias à frente prever (máx 16)

    Retorna:
        Resposta bruta da API (dict)
    """
    logger.info(
        f"[{NOME_TABELA}] Extraindo previsão {dias_previsao} dias para {regiao_key}"
    )

    client = OpenMeteoClient()
    return client.buscar_previsao(
        regiao_key=regiao_key,
        variaveis_daily=VARIAVEIS_DAILY,
        dias_previsao=dias_previsao,
    )


def transformar(dados_api: dict, regiao_key: str) -> list[dict]:
    """
    Transforma a resposta da API em lista de registros para o Oracle.

    Cada item da lista corresponde a uma linha da tabela PREVISAO_AGRICOLA.

    Tratamentos aplicados:
        - Remoção de registros com data nula
        - Valores numéricos ausentes substituídos por None (NULL no Oracle)
        - Arredondamento para 2 casas decimais
        - Probabilidade de precipitação truncada a 100 se API retornar > 100
        - Adição de regiao e dt_ingestao
    """
    regiao_nome = REGIOES_AGRICOLAS[regiao_key]["nome"]
    daily = dados_api.get("daily", {})

    datas         = daily.get("time", [])
    et0           = daily.get("et0_fao_evapotranspiration", [])
    precipitacao  = daily.get("precipitation_sum", [])
    temp_max      = daily.get("temperature_2m_max", [])
    radiacao      = daily.get("shortwave_radiation_sum", [])
    prob_chuva    = daily.get("precipitation_probability_max", [])

    registros = []
    dt_ingestao = datetime.now()
    total_nulos = 0

    for i, data_str in enumerate(datas):
        if not data_str:
            total_nulos += 1
            continue

        prob = _arredondar(prob_chuva[i] if i < len(prob_chuva) else None)
        # Garante que probabilidade não ultrapasse 100%
        if prob is not None and prob > 100:
            prob = 100.0

        registro = {
            "regiao":                regiao_nome,
            "data_previsao":         datetime.strptime(data_str, "%Y-%m-%d").date(),
            "et0_evapotranspiracao": _arredondar(et0[i] if i < len(et0) else None),
            "precipitacao_prevista": _arredondar(precipitacao[i] if i < len(precipitacao) else None),
            "temp_max_prevista":     _arredondar(temp_max[i] if i < len(temp_max) else None),
            "radiacao_solar":        _arredondar(radiacao[i] if i < len(radiacao) else None),
            "prob_precipitacao":     prob,
            "dt_ingestao":           dt_ingestao,
        }
        registros.append(registro)

    logger.info(
        f"[{NOME_TABELA}] Transformação concluída: {len(registros)} registros "
        f"({total_nulos} datas nulas ignoradas)"
    )
    return registros


def extrair_todas_regioes(
    dias_previsao: int = DIAS_PREVISAO_PADRAO,
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
                dias_previsao=dias_previsao,
            )
            registros = transformar(dados_api, regiao_key)
            todos_registros.extend(registros)
            logger.info(
                f"[{NOME_TABELA}] {regiao_key}: {len(registros)} registros adicionados"
            )
        except Exception as e:
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
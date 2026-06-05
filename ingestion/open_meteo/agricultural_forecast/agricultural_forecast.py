import logging
from datetime import datetime, date
from typing import Optional
from script.api_open_meteo import OpenMeteoClient, REGIOES_AGRICOLAS
from utils.ingestion_utils import executar_para_todas_regioes, arredondar

logger = logging.getLogger(__name__)



NOME_TABELA = "AGRICULTURAL_FORECAST"
DIAS_PREVISAO_PADRAO = 16

VARIAVEIS_DAILY = [
    "et0_fao_evapotranspiration",    # Evapotranspiração de referência (mm)
    "precipitation_sum",             # Precipitação prevista (mm)
    "temperature_2m_max",            # Temperatura máxima prevista (°C)
    "shortwave_radiation_sum",       # Radiação solar total (MJ/m²)
    "precipitation_probability_max", # Probabilidade de precipitação (%)
]

def extrair(
    regiao_key: str,
    dias_previsao: int = DIAS_PREVISAO_PADRAO,
) -> dict:
    """
    Extrai previsão agrícola para os próximos dias para uma região.

    Parâmetros:
        regiao_key    : chave da região (ex: "cascavel_pr")
        dias_previsao : quantos dias à frente prever (máx 16)
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

    Tratamentos aplicados:
        - Remoção de registros com data nula
        - Valores numéricos ausentes substituídos por None (NULL no Oracle)
        - Arredondamento para 2 casas decimais
        - Probabilidade de precipitação truncada a 100 se API retornar > 100
        - Adição de regiao e dt_ingestao
    """
    regiao_nome = REGIOES_AGRICOLAS[regiao_key]["nome"]
    daily = dados_api.get("daily", {})

    datas        = daily.get("time", [])
    et0          = daily.get("et0_fao_evapotranspiration", [])
    precipitacao = daily.get("precipitation_sum", [])
    temp_max     = daily.get("temperature_2m_max", [])
    radiacao     = daily.get("shortwave_radiation_sum", [])
    prob_chuva   = daily.get("precipitation_probability_max", [])

    registros = []
    dt_ingestao = datetime.now()
    total_nulos = 0

    for i, data_str in enumerate(datas):
        if not data_str:
            total_nulos += 1
            continue

        prob = arredondar(prob_chuva[i] if i < len(prob_chuva) else None)
        if prob is not None and prob > 100:
            prob = 100.0

        registro = {
            "regiao":                regiao_nome,
            "data_previsao":         datetime.strptime(data_str, "%Y-%m-%d").date(),
            "et0_evapotranspiracao": arredondar(et0[i] if i < len(et0) else None),
            "precipitacao_prevista": arredondar(precipitacao[i] if i < len(precipitacao) else None),
            "temp_max_prevista":     arredondar(temp_max[i] if i < len(temp_max) else None),
            "radiacao_solar":        arredondar(radiacao[i] if i < len(radiacao) else None),
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
    **kwargs,
) -> list[dict]:
    """Extrai e transforma todas as regiões. Ideal para uso direto na DAG."""
    return executar_para_todas_regioes(
        extrair_fn=extrair,
        transformar_fn=transformar,
        regioes=REGIOES_AGRICOLAS,
        nome_tabela=NOME_TABELA,
        dias_previsao=dias_previsao,
    )

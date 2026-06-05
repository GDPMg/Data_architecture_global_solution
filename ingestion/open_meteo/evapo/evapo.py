"""
ingestion/open_meteo/evapo/evapo.py
-------------------------------------
Define os parâmetros da tabela EVAPO e transforma
a resposta da API em registros prontos para carga no Oracle.

Tabela Oracle alvo: EVAPO

Dados coletados são exclusivos desta fonte — variáveis não disponíveis
em AGRO_WEATHER (NASA) nem em AGRICULTURAL_FORECAST (previsão):
  - ET0 histórico (AGRICULTURAL_FORECAST só tem previsão)
  - Déficit de pressão de vapor (estresse hídrico das plantas)
  - Duração efetiva de sol (horas)
  - Rajadas máximas de vento (distinto de velocidade máxima)
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional
from script.api_open_meteo import OpenMeteoClient, REGIOES_AGRICOLAS
from utils.ingestion_utils import executar_para_todas_regioes, arredondar

JANELA_FULL_DIAS = 120

logger = logging.getLogger(__name__)


# ── Configuração da tabela ─────────────────────────────────────────────────────

NOME_TABELA = "EVAPO"

VARIAVEIS_DAILY = [
    "et0_fao_evapotranspiration",  # Evapotranspiração de referência histórica (mm/dia)
    "vapor_pressure_deficit_max",  # Déficit de pressão de vapor máximo (kPa)
    "sunshine_duration",           # Duração do sol (segundos — convertido para horas)
    "wind_gusts_10m_max",          # Rajada máxima de vento a 10m (km/h)
]

DDL_ORACLE = """
CREATE TABLE EVAPO (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data                    DATE           NOT NULL,
    et0_evapotranspiracao   NUMBER(7,2),   -- et0_fao_evapotranspiration (mm/dia)
    deficit_pressao_vapor   NUMBER(6,3),   -- vapor_pressure_deficit_max (kPa)
    duracao_sol             NUMBER(5,2),   -- sunshine_duration convertida (horas)
    rajada_vento_max        NUMBER(6,2),   -- wind_gusts_10m_max (km/h)
    dt_ingestao             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_evapo UNIQUE (regiao, data)
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
    Extrai dados históricos de evapotranspiração para uma região.

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

    Cada item corresponde a uma linha da tabela EVAPO.

    Tratamentos aplicados:
        - Remoção de registros com data nula
        - sunshine_duration convertida de segundos para horas
        - Valores negativos de ET0 e rajada convertidos para None
        - Arredondamento: ET0/rajada a 2 casas, déficit a 3 casas, sol a 2 casas
    """
    regiao_nome = REGIOES_AGRICOLAS[regiao_key]["nome"]
    daily = dados_api.get("daily", {})

    datas    = daily.get("time", [])
    et0      = daily.get("et0_fao_evapotranspiration", [])
    vpd      = daily.get("vapor_pressure_deficit_max", [])
    sunshine = daily.get("sunshine_duration", [])
    rajada   = daily.get("wind_gusts_10m_max", [])

    registros = []
    dt_ingestao = datetime.now()
    total_nulos = 0

    for i, data_str in enumerate(datas):
        if not data_str:
            total_nulos += 1
            continue

        val_et0      = arredondar(et0[i] if i < len(et0) else None)
        val_vpd      = _arredondar_n(vpd[i] if i < len(vpd) else None, 3)
        val_sunshine = _segundos_para_horas(sunshine[i] if i < len(sunshine) else None)
        val_rajada   = arredondar(rajada[i] if i < len(rajada) else None)

        # ET0 e rajada não podem ser negativos fisicamente
        if val_et0 is not None and val_et0 < 0:
            val_et0 = None
        if val_rajada is not None and val_rajada < 0:
            val_rajada = None

        registro = {
            "regiao":                regiao_nome,
            "data":                  datetime.strptime(data_str, "%Y-%m-%d").date(),
            "et0_evapotranspiracao": val_et0,
            "deficit_pressao_vapor": val_vpd,
            "duracao_sol":           val_sunshine,
            "rajada_vento_max":      val_rajada,
            "dt_ingestao":           dt_ingestao,
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
    janela_dias: int = JANELA_FULL_DIAS,
    modo: str = "incremental",
) -> list[dict]:
    """
    Extrai e transforma todas as regiões. Ideal para uso direto na DAG.

    Parâmetros:
        modo : "incremental" → carrega apenas d-1 (ontem)
               "full"        → carrega os últimos JANELA_FULL_DIAS dias
    """
    if modo == "incremental":
        ontem = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        data_inicio = ontem
        data_fim = ontem
        logger.info(f"[{NOME_TABELA}] Incremental: carregando {ontem}")
    else:
        ontem = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        data_fim = ontem
        logger.info(f"[{NOME_TABELA}] Full: carregando últimos {janela_dias} dias até {ontem}")

    return executar_para_todas_regioes(
        extrair_fn=extrair,
        transformar_fn=transformar,
        regioes=REGIOES_AGRICOLAS,
        nome_tabela=NOME_TABELA,
        data_inicio=data_inicio,
        data_fim=data_fim,
        janela_dias=janela_dias,
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _segundos_para_horas(valor) -> Optional[float]:
    """Converte sunshine_duration de segundos (API) para horas."""
    if valor is None:
        return None
    try:
        horas = float(valor) / 3600.0
        return round(max(0.0, min(24.0, horas)), 2)
    except (TypeError, ValueError):
        return None


def _arredondar_n(valor, casas: int) -> Optional[float]:
    """Arredonda para N casas decimais, retorna None se inválido."""
    if valor is None:
        return None
    try:
        return round(float(valor), casas)
    except (TypeError, ValueError):
        return None

"""
tables/nasa_power/solar_radiation_daily.py
-------------------------------------------
Define os parâmetros exatos da tabela SOLAR_RADIATION_DAILY e transforma
a resposta da API em registros prontos para carga no Oracle.

Fonte dos dados: NASA CERES SYN1deg + GEWEX SRB (satélites)
Tabela Oracle alvo: SOLAR_RADIATION_DAILY

Por que essa tabela?
    Radiação solar é o dado mais distintivo da NASA POWER vs. Open-Meteo.
    É derivada exclusivamente de satélites (CERES/SRB), não de modelos de reanálise.
    Fundamental para cálculo de produtividade agrícola e zoneamento de culturas.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional
from script.api_nasa_power import NasaPowerClient, REGIOES_AGRICOLAS
from utils.ingestion_utils import executar_para_todas_regioes

JANELA_FULL_DIAS = 120
LATENCIA_NASA_DIAS = 7

logger = logging.getLogger(__name__)


# ── Configuração da tabela ─────────────────────────────────────────────────────

NOME_TABELA = "SOLAR_RADIATION_DAILY"

# Parâmetros NASA POWER para esta tabela
# Fonte: CERES SYN1deg (satélite) — disponíveis no community AG
PARAMETROS = [
    "ALLSKY_SFC_SW_DWN",   # Radiação solar de superfície (céu aberto) — Wh/m²/dia
    "CLRSKY_SFC_SW_DWN",   # Radiação solar de superfície (céu limpo)  — Wh/m²/dia
    "ALLSKY_KT",           # Índice de clareza atmosférica (0-1, adimensional)
    "ALLSKY_SFC_PAR_TOT",  # PAR total — radiação fotossinteticamente ativa — W/m²
    "TOA_SW_DWN",          # Radiação no topo da atmosfera — Wh/m²/dia
]

# DDL Oracle de referência
DDL_ORACLE = """
CREATE TABLE SOLAR_RADIATION_DAILY (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data                    DATE           NOT NULL,
    radiacao_sup_total      NUMBER(10,2),   -- ALLSKY_SFC_SW_DWN (Wh/m²/dia)
    radiacao_sup_ceu_limpo  NUMBER(10,2),   -- CLRSKY_SFC_SW_DWN (Wh/m²/dia)
    indice_clareza          NUMBER(5,4),    -- ALLSKY_KT (0.0000 a 1.0000)
    par_fotossintetico      NUMBER(8,2),    -- ALLSKY_SFC_PAR_TOT (W/m²)
    radiacao_topo_atmosfera NUMBER(10,2),   -- TOA_SW_DWN (Wh/m²/dia)
    dt_ingestao             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_solar_radiation UNIQUE (regiao, data)
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
    Extrai dados de radiação solar para uma região.

    Parâmetros:
        regiao_key  : chave da região (ex: "sorriso_mt")
        data_inicio : "YYYYMMDD" (opcional — usa janela_dias se omitido)
        data_fim    : "YYYYMMDD" (opcional — usa hoje - 7 dias se omitido)
        janela_dias : dias de histórico quando datas não forem informadas

    Nota:
        A NASA POWER tem latência de ~7 dias nos dados mais recentes.
        O método buscar_janela já aplica esse ajuste automaticamente.
    """
    client = NasaPowerClient()

    if data_inicio and data_fim:
        logger.info(f"[{NOME_TABELA}] Extraindo {regiao_key}: {data_inicio} → {data_fim}")
        return client.buscar(
            regiao_key=regiao_key,
            parametros=PARAMETROS,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
    else:
        logger.info(f"[{NOME_TABELA}] Extraindo {regiao_key}: janela de {janela_dias} dias")
        return client.buscar_janela(
            regiao_key=regiao_key,
            parametros=PARAMETROS,
            janela_dias=janela_dias,
        )


def transformar(dados_api: dict, regiao_key: str) -> list[dict]:
    """
    Transforma a resposta da NASA POWER em lista de registros para o Oracle.

    Estrutura da resposta NASA POWER:
        dados["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]["20240101"] = valor

    Tratamentos aplicados:
        - Conversão de chaves de data "YYYYMMDD" → objeto date
        - Fill values (-999) substituídos por None (NULL no Oracle)
        - Índice de clareza (ALLSKY_KT) truncado ao intervalo [0, 1]
        - Arredondamento para 2 casas decimais (4 para índice de clareza)
        - Adição de regiao e dt_ingestao
    """
    regiao_nome = REGIOES_AGRICOLAS[regiao_key]["nome"]
    client = NasaPowerClient()

    parametros_dados = dados_api.get("properties", {}).get("parameter", {})

    allsky     = parametros_dados.get("ALLSKY_SFC_SW_DWN", {})
    clrsky     = parametros_dados.get("CLRSKY_SFC_SW_DWN", {})
    kt         = parametros_dados.get("ALLSKY_KT", {})
    par        = parametros_dados.get("ALLSKY_SFC_PAR_TOT", {})
    toa        = parametros_dados.get("TOA_SW_DWN", {})

    # Todas as datas disponíveis no primeiro parâmetro retornado
    datas = sorted(allsky.keys())

    registros = []
    dt_ingestao = datetime.now()
    total_fill = 0

    for data_str in datas:
        try:
            data = datetime.strptime(data_str, "%Y%m%d").date()
        except ValueError:
            logger.warning(f"[{NOME_TABELA}] Data inválida ignorada: {data_str}")
            continue

        val_allsky = _limpar(allsky.get(data_str), client)
        val_clrsky = _limpar(clrsky.get(data_str), client)
        val_kt     = _limpar_kt(kt.get(data_str), client)
        val_par    = _limpar(par.get(data_str), client)
        val_toa    = _limpar(toa.get(data_str), client)

        # Conta registros onde todos os valores são fill
        if all(v is None for v in [val_allsky, val_clrsky, val_kt, val_par, val_toa]):
            total_fill += 1
            continue

        registro = {
            "regiao":                  regiao_nome,
            "data":                    data,
            "radiacao_sup_total":      val_allsky,
            "radiacao_sup_ceu_limpo":  val_clrsky,
            "indice_clareza":          val_kt,
            "par_fotossintetico":      val_par,
            "radiacao_topo_atmosfera": val_toa,
            "dt_ingestao":             dt_ingestao,
        }
        registros.append(registro)

    logger.info(
        f"[{NOME_TABELA}] Transformação concluída: {len(registros)} registros "
        f"({total_fill} registros fill value ignorados)"
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
        modo : "incremental" → carrega o dia mais recente disponível (hoje - LATENCIA_NASA_DIAS)
               "full"        → carrega os últimos JANELA_FULL_DIAS dias
    """
    if modo == "incremental":
        d1_nasa = (date.today() - timedelta(days=LATENCIA_NASA_DIAS)).strftime("%Y%m%d")
        data_inicio = d1_nasa
        data_fim = d1_nasa
        logger.info(f"[{NOME_TABELA}] Incremental: carregando {d1_nasa} (d-{LATENCIA_NASA_DIAS} NASA)")
    else:
        logger.info(f"[{NOME_TABELA}] Full: carregando últimos {janela_dias} dias")

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

def _limpar(valor, client: NasaPowerClient) -> Optional[float]:
    """Converte fill value para None e arredonda para 2 casas."""
    if client.is_fill_value(valor):
        return None
    try:
        return round(float(valor), 2)
    except (TypeError, ValueError):
        return None


def _limpar_kt(valor, client: NasaPowerClient) -> Optional[float]:
    """
    Trata o índice de clareza ALLSKY_KT.
    Deve estar entre 0 e 1. Arredonda para 4 casas decimais.
    """
    if client.is_fill_value(valor):
        return None
    try:
        v = float(valor)
        v = max(0.0, min(1.0, v))  # clamp ao intervalo válido
        return round(v, 4)
    except (TypeError, ValueError):
        return None
"""
tables/nasa_power/agro_meteo_daily.py
--------------------------------------
Define os parâmetros exatos da tabela AGRO_METEO_DAILY e transforma
a resposta da API em registros prontos para carga no Oracle.

Fonte dos dados: NASA MERRA-2 (modelo de reanálise com dados de satélite)
Tabela Oracle alvo: AGRO_METEO_DAILY

Por que essa tabela?
    Complementa a SOLAR_RADIATION_DAILY com variáveis agrometeorológicas
    clássicas (temperatura, umidade, vento, ponto de orvalho, precipitação).
    Derivadas do MERRA-2, que integra satélites + observações de superfície.
    Permite cruzamento rico com os dados do Open-Meteo nas consultas analíticas.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional
from script.api_nasa_power import NasaPowerClient, REGIOES_AGRICOLAS
from utils.ingestion_utils import executar_para_todas_regioes

JANELA_FULL_DIAS = 120
# NASA POWER tem ~7 dias de latência — d-1 real é o dia mais recente disponível
LATENCIA_NASA_DIAS = 7

logger = logging.getLogger(__name__)


# ── Configuração da tabela ─────────────────────────────────────────────────────

NOME_TABELA = "AGRO_METEO_DAILY"

# Parâmetros NASA POWER para esta tabela
# Fonte: MERRA-2 (satélite + reanálise) — community AG
PARAMETROS = [
    "T2M",           # Temperatura média a 2m (°C)
    "T2M_MAX",       # Temperatura máxima a 2m (°C)
    "T2M_MIN",       # Temperatura mínima a 2m (°C)
    "T2MDEW",        # Ponto de orvalho a 2m (°C) — indica umidade do ar
    "RH2M",          # Umidade relativa a 2m (%)
    "PRECTOTCORR",   # Precipitação corrigida (mm/dia)
    "WS2M",          # Velocidade do vento a 2m (m/s)
    "WS2M_MAX",      # Velocidade máxima do vento a 2m (m/s)
]

# DDL Oracle de referência
DDL_ORACLE = """
CREATE TABLE AGRO_METEO_DAILY (
    id                  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao              VARCHAR2(100)  NOT NULL,
    data                DATE           NOT NULL,
    temp_media          NUMBER(5,2),    -- T2M    (°C)
    temp_max            NUMBER(5,2),    -- T2M_MAX (°C)
    temp_min            NUMBER(5,2),    -- T2M_MIN (°C)
    ponto_orvalho       NUMBER(5,2),    -- T2MDEW  (°C)
    umidade_relativa    NUMBER(5,2),    -- RH2M    (%)
    precipitacao        NUMBER(7,2),    -- PRECTOTCORR (mm/dia)
    vento_medio         NUMBER(6,2),    -- WS2M    (m/s)
    vento_max           NUMBER(6,2),    -- WS2M_MAX (m/s)
    dt_ingestao         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agro_meteo_daily UNIQUE (regiao, data)
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
    Extrai dados agrometeorológicos para uma região.

    Parâmetros:
        regiao_key  : chave da região (ex: "rio_verde_go")
        data_inicio : "YYYYMMDD" (opcional — usa janela_dias se omitido)
        data_fim    : "YYYYMMDD" (opcional — usa hoje - 7 dias se omitido)
        janela_dias : dias de histórico quando datas não forem informadas
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
        dados["properties"]["parameter"]["T2M"]["20240101"] = valor

    Tratamentos aplicados:
        - Conversão de chaves de data "YYYYMMDD" → objeto date
        - Fill values (-999) substituídos por None (NULL no Oracle)
        - Validação de intervalo: umidade entre 0-100%, temp entre -80 e 60°C
        - Vento negativo convertido para None (fisicamente inválido)
        - Precipitação negativa convertida para None
        - Arredondamento para 2 casas decimais
        - Adição de regiao e dt_ingestao
    """
    regiao_nome = REGIOES_AGRICOLAS[regiao_key]["nome"]
    client = NasaPowerClient()

    parametros_dados = dados_api.get("properties", {}).get("parameter", {})

    t2m       = parametros_dados.get("T2M", {})
    t2m_max   = parametros_dados.get("T2M_MAX", {})
    t2m_min   = parametros_dados.get("T2M_MIN", {})
    t2mdew    = parametros_dados.get("T2MDEW", {})
    rh2m      = parametros_dados.get("RH2M", {})
    prec      = parametros_dados.get("PRECTOTCORR", {})
    ws2m      = parametros_dados.get("WS2M", {})
    ws2m_max  = parametros_dados.get("WS2M_MAX", {})

    datas = sorted(t2m.keys())

    registros = []
    dt_ingestao = datetime.now()
    total_fill = 0

    for data_str in datas:
        try:
            data = datetime.strptime(data_str, "%Y%m%d").date()
        except ValueError:
            logger.warning(f"[{NOME_TABELA}] Data inválida ignorada: {data_str}")
            continue

        val_t2m      = _limpar_temp(t2m.get(data_str), client)
        val_t2m_max  = _limpar_temp(t2m_max.get(data_str), client)
        val_t2m_min  = _limpar_temp(t2m_min.get(data_str), client)
        val_t2mdew   = _limpar_temp(t2mdew.get(data_str), client)
        val_rh2m     = _limpar_umidade(rh2m.get(data_str), client)
        val_prec     = _limpar_precipitacao(prec.get(data_str), client)
        val_ws2m     = _limpar_vento(ws2m.get(data_str), client)
        val_ws2m_max = _limpar_vento(ws2m_max.get(data_str), client)

        valores = [val_t2m, val_t2m_max, val_t2m_min, val_rh2m, val_prec]
        if all(v is None for v in valores):
            total_fill += 1
            continue

        registro = {
            "regiao":           regiao_nome,
            "data":             data,
            "temp_media":       val_t2m,
            "temp_max":         val_t2m_max,
            "temp_min":         val_t2m_min,
            "ponto_orvalho":    val_t2mdew,
            "umidade_relativa": val_rh2m,
            "precipitacao":     val_prec,
            "vento_medio":      val_ws2m,
            "vento_max":        val_ws2m_max,
            "dt_ingestao":      dt_ingestao,
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
        # NASA tem latência de ~7 dias; d-1 equivale ao último dia com dados disponíveis
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

def _limpar_temp(valor, client: NasaPowerClient) -> Optional[float]:
    """Limpa temperatura: fill → None, fora de [-80, 60] → None."""
    if client.is_fill_value(valor):
        return None
    try:
        v = float(valor)
        if v < -80 or v > 60:
            return None
        return round(v, 2)
    except (TypeError, ValueError):
        return None


def _limpar_umidade(valor, client: NasaPowerClient) -> Optional[float]:
    """Limpa umidade relativa: fill → None, clamp a [0, 100]."""
    if client.is_fill_value(valor):
        return None
    try:
        v = float(valor)
        v = max(0.0, min(100.0, v))
        return round(v, 2)
    except (TypeError, ValueError):
        return None


def _limpar_precipitacao(valor, client: NasaPowerClient) -> Optional[float]:
    """Limpa precipitação: fill → None, negativo → None."""
    if client.is_fill_value(valor):
        return None
    try:
        v = float(valor)
        if v < 0:
            return None
        return round(v, 2)
    except (TypeError, ValueError):
        return None


def _limpar_vento(valor, client: NasaPowerClient) -> Optional[float]:
    """Limpa velocidade do vento: fill → None, negativo → None."""
    if client.is_fill_value(valor):
        return None
    try:
        v = float(valor)
        if v < 0:
            return None
        return round(v, 2)
    except (TypeError, ValueError):
        return None
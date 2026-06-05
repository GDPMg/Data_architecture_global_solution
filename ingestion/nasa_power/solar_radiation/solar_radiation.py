import logging
from datetime import datetime, date, timedelta
from typing import Optional
from script.api_nasa_power import NasaPowerClient, REGIOES_AGRICOLAS
from utils.ingestion_utils import executar_para_todas_regioes

JANELA_FULL_DIAS = 120
LATENCIA_NASA_DIAS = 7

logger = logging.getLogger(__name__)



NOME_TABELA = "SOLAR_RADIATION_DAILY"

PARAMETROS = [
    "ALLSKY_SFC_SW_DWN",   # Radiação solar de superfície (céu aberto) — Wh/m²/dia
    "CLRSKY_SFC_SW_DWN",   # Radiação solar de superfície (céu limpo)  — Wh/m²/dia
    "ALLSKY_KT",           # Índice de clareza atmosférica (0-1, adimensional)
    "ALLSKY_SFC_PAR_TOT",  # PAR total — radiação fotossinteticamente ativa — W/m²
    "TOA_SW_DWN",          # Radiação no topo da atmosfera — Wh/m²/dia
]


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
"""
nasa_power_client.py
---------------------
Cliente base generalizado para a API NASA POWER (AG Community).
Deve ser usado pelos scripts de cada tabela em tables/nasa_power/.

Documentação oficial: https://power.larc.nasa.gov/docs/services/api/
Parâmetros disponíveis: https://power.larc.nasa.gov/docs/methodology/
"""

import requests
import logging
from datetime import datetime, date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


# ── Constantes ─────────────────────────────────────────────────────────────────

BASE_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Comunidade AG = dados voltados para agrometeorologia
COMMUNITY = "AG"
FORMAT = "JSON"
TIMEZONE = "LST"  # Local Solar Time — padrão da API para agricultura

# Limite de parâmetros por requisição (restrição da NASA POWER API)
MAX_PARAMETROS = 20

# Regiões agrícolas — mesmas do Open-Meteo para cruzamento de dados
REGIOES_AGRICOLAS = {
    "sorriso_mt":        {"latitude": -12.54, "longitude": -55.72, "nome": "Sorriso/MT"},
    "ribeirao_preto_sp": {"latitude": -21.17, "longitude": -47.81, "nome": "Ribeirão Preto/SP"},
    "rio_verde_go":      {"latitude": -17.80, "longitude": -50.93, "nome": "Rio Verde/GO"},
    "cascavel_pr":       {"latitude": -24.96, "longitude": -53.46, "nome": "Cascavel/PR"},
    "barreiras_ba":      {"latitude": -12.15, "longitude": -45.00, "nome": "Barreiras/BA"},
}

TIMEOUT_SEGUNDOS = 60  # NASA POWER pode ser mais lenta que Open-Meteo

# Valor que a API retorna quando dado está ausente
FILL_VALUE = -999.0


# ── Cliente base ───────────────────────────────────────────────────────────────

class NasaPowerClient:
    """
    Cliente generalizado para a API NASA POWER (comunidade AG, granularidade diária).

    Uso pelos scripts de tabela:
        client = NasaPowerClient()
        dados = client.buscar(
            regiao_key="sorriso_mt",
            parametros=["T2M", "PRECTOTCORR", "ALLSKY_SFC_SW_DWN"],
            data_inicio="20240101",
            data_fim="20240331",
        )
    """

    def __init__(self, timeout: int = TIMEOUT_SEGUNDOS):
        self.timeout = timeout
        self.session = requests.Session()

    # ── Métodos públicos ───────────────────────────────────────────────────────

    def buscar(
        self,
        regiao_key: str,
        parametros: list[str],
        data_inicio: str,
        data_fim: str,
        community: str = COMMUNITY,
    ) -> dict:
        """
        Busca dados diários para uma região e intervalo de datas.

        Parâmetros:
            regiao_key  : chave em REGIOES_AGRICOLAS (ex: "sorriso_mt")
            parametros  : lista de parâmetros NASA POWER (ex: ["T2M", "RH2M"])
                          Máx 20 por requisição.
            data_inicio : string "YYYYMMDD" (formato exigido pela NASA POWER)
            data_fim    : string "YYYYMMDD"
            community   : comunidade da API (padrão: "AG")

        Retorna:
            dict com os dados da API já validado, ou lança exceção.
        """
        if len(parametros) > MAX_PARAMETROS:
            raise ValueError(
                f"[NasaPower] Máximo de {MAX_PARAMETROS} parâmetros por requisição. "
                f"Recebido: {len(parametros)}"
            )

        regiao = self._obter_regiao(regiao_key)

        params = {
            "parameters": ",".join(parametros),
            "community":  community,
            "longitude":  regiao["longitude"],
            "latitude":   regiao["latitude"],
            "start":      self._validar_data(data_inicio),
            "end":        self._validar_data(data_fim),
            "format":     FORMAT,
        }

        return self._fazer_requisicao(params, regiao["nome"])

    def buscar_janela(
        self,
        regiao_key: str,
        parametros: list[str],
        janela_dias: int = 90,
        community: str = COMMUNITY,
    ) -> dict:
        """
        Conveniência: busca os últimos N dias a partir de hoje.

        A NASA POWER tem latência de alguns dias (dados chegam com ~7 dias de atraso).
        Por isso data_fim é ajustado para 7 dias atrás automaticamente.

        Parâmetros:
            regiao_key  : chave em REGIOES_AGRICOLAS
            parametros  : lista de parâmetros NASA POWER
            janela_dias : quantos dias de histórico buscar (padrão 90)
            community   : comunidade da API
        """
        data_fim = date.today() - timedelta(days=7)   # latência da NASA
        data_inicio = data_fim - timedelta(days=janela_dias)

        return self.buscar(
            regiao_key=regiao_key,
            parametros=parametros,
            data_inicio=data_inicio.strftime("%Y%m%d"),
            data_fim=data_fim.strftime("%Y%m%d"),
            community=community,
        )

    def listar_regioes(self) -> list[dict]:
        """Retorna a lista de regiões disponíveis com lat/lon."""
        return [{"key": k, **v} for k, v in REGIOES_AGRICOLAS.items()]

    # ── Métodos internos ───────────────────────────────────────────────────────

    def _fazer_requisicao(self, params: dict, regiao_nome: str) -> dict:
        """Executa a requisição HTTP, trata erros e retorna o JSON."""
        logger.info(
            f"[NasaPower] Requisição → região: {regiao_nome} | "
            f"parâmetros: {params.get('parameters')} | "
            f"{params.get('start')} → {params.get('end')}"
        )

        try:
            response = self.session.get(
                BASE_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"[NasaPower] Timeout após {self.timeout}s. "
                "A API da NASA pode estar lenta — tente novamente."
            )
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"[NasaPower] Falha de conexão: {e}")
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(
                f"[NasaPower] Erro HTTP {response.status_code}: {response.text}"
            )

        dados = response.json()

        # NASA POWER retorna erros dentro do JSON com HTTP 200
        if "messages" in dados:
            for msg in dados["messages"]:
                if "error" in msg.lower() or "invalid" in msg.lower():
                    raise ValueError(f"[NasaPower] Erro da API: {msg}")

        n_registros = len(
            next(iter(dados.get("properties", {})
                          .get("parameter", {})
                          .values()), {})
        )
        logger.info(f"[NasaPower] Sucesso → {n_registros} registros diários")
        return dados

    def _obter_regiao(self, regiao_key: str) -> dict:
        """Valida e retorna os dados da região."""
        if regiao_key not in REGIOES_AGRICOLAS:
            disponiveis = list(REGIOES_AGRICOLAS.keys())
            raise KeyError(
                f"[NasaPower] Região '{regiao_key}' não encontrada. "
                f"Disponíveis: {disponiveis}"
            )
        return REGIOES_AGRICOLAS[regiao_key]

    def _validar_data(self, data_str: str) -> str:
        """Valida formato YYYYMMDD (exigido pela NASA POWER) e retorna a string."""
        try:
            datetime.strptime(data_str, "%Y%m%d")
        except ValueError:
            raise ValueError(
                f"[NasaPower] Data inválida '{data_str}'. "
                "Use o formato YYYYMMDD (ex: 20240101)."
            )
        return data_str

    @staticmethod
    def is_fill_value(valor) -> bool:
        """Verifica se o valor é o marcador de dado ausente da NASA POWER (-999)."""
        if valor is None:
            return True
        try:
            return float(valor) <= FILL_VALUE
        except (TypeError, ValueError):
            return True
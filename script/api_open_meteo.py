"""
open_meteo_client.py
--------------------
Cliente base generalizado para a API Open-Meteo.
Deve ser usado pelos scripts de cada tabela em tables/open_meteo/.

Documentação da API: https://open-meteo.com/en/docs
"""

import requests
import logging
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


# ── Constantes ────────────────────────────────────────────────────────────────

BASE_URL = "https://api.open-meteo.com/v1"

# Regiões agrícolas pré-definidas (lat, lon, nome)
REGIOES_AGRICOLAS = {
    "sorriso_mt":        {"latitude": -12.54, "longitude": -55.72, "nome": "Sorriso/MT"},
    "ribeirao_preto_sp": {"latitude": -21.17, "longitude": -47.81, "nome": "Ribeirão Preto/SP"},
    "rio_verde_go":      {"latitude": -17.80, "longitude": -50.93, "nome": "Rio Verde/GO"},
    "cascavel_pr":       {"latitude": -24.96, "longitude": -53.46, "nome": "Cascavel/PR"},
    "barreiras_ba":      {"latitude": -12.15, "longitude": -45.00, "nome": "Barreiras/BA"},
}

TIMEZONE_PADRAO = "America/Sao_Paulo"
TIMEOUT_SEGUNDOS = 30


# ── Cliente base ───────────────────────────────────────────────────────────────

class OpenMeteoClient:
    """
    Cliente generalizado para a API Open-Meteo.

    Uso pelos scripts de tabela:
        client = OpenMeteoClient()
        dados = client.buscar_historico(
            regiao_key="sorriso_mt",
            variaveis_daily=["temperature_2m_max", "precipitation_sum"],
            data_inicio="2024-01-01",
            data_fim="2024-03-31",
        )
    """

    def __init__(self, timeout: int = TIMEOUT_SEGUNDOS):
        self.timeout = timeout
        self.session = requests.Session()

    # ── Métodos públicos ───────────────────────────────────────────────────────

    def buscar_historico(
        self,
        regiao_key: str,
        variaveis_daily: list[str],
        data_inicio: str,
        data_fim: str,
        variaveis_hourly: Optional[list[str]] = None,
        timezone: str = TIMEZONE_PADRAO,
    ) -> dict:
        """
        Busca dados históricos (archive) para uma região e intervalo de datas.

        Parâmetros:
            regiao_key      : chave em REGIOES_AGRICOLAS (ex: "sorriso_mt")
            variaveis_daily : lista de variáveis diárias (ex: ["temperature_2m_max"])
            data_inicio     : string "YYYY-MM-DD"
            data_fim        : string "YYYY-MM-DD"
            variaveis_hourly: lista de variáveis horárias (opcional)
            timezone        : fuso horário (padrão: America/Sao_Paulo)

        Retorna:
            dict com os dados da API já validado, ou lança exceção.
        """
        regiao = self._obter_regiao(regiao_key)

        params = {
            "latitude":   regiao["latitude"],
            "longitude":  regiao["longitude"],
            "start_date": self._validar_data(data_inicio),
            "end_date":   self._validar_data(data_fim),
            "timezone":   timezone,
        }

        if variaveis_daily:
            params["daily"] = ",".join(variaveis_daily)
        if variaveis_hourly:
            params["hourly"] = ",".join(variaveis_hourly)

        return self._fazer_requisicao(
            endpoint="/archive",
            params=params,
            regiao_nome=regiao["nome"],
        )

    def buscar_previsao(
        self,
        regiao_key: str,
        variaveis_daily: list[str],
        dias_previsao: int = 16,
        variaveis_hourly: Optional[list[str]] = None,
        timezone: str = TIMEZONE_PADRAO,
    ) -> dict:
        """
        Busca previsão futura (forecast) para uma região.

        Parâmetros:
            regiao_key      : chave em REGIOES_AGRICOLAS (ex: "sorriso_mt")
            variaveis_daily : lista de variáveis diárias
            dias_previsao   : quantos dias à frente (máx 16)
            variaveis_hourly: lista de variáveis horárias (opcional)
            timezone        : fuso horário

        Retorna:
            dict com os dados da API já validado, ou lança exceção.
        """
        if dias_previsao > 16:
            raise ValueError(f"dias_previsao máximo é 16. Recebido: {dias_previsao}")

        regiao = self._obter_regiao(regiao_key)

        params = {
            "latitude":        regiao["latitude"],
            "longitude":       regiao["longitude"],
            "forecast_days":   dias_previsao,
            "timezone":        timezone,
        }

        if variaveis_daily:
            params["daily"] = ",".join(variaveis_daily)
        if variaveis_hourly:
            params["hourly"] = ",".join(variaveis_hourly)

        return self._fazer_requisicao(
            endpoint="/forecast",
            params=params,
            regiao_nome=regiao["nome"],
        )

    def listar_regioes(self) -> list[dict]:
        """Retorna a lista de regiões disponíveis com lat/lon."""
        return [
            {"key": k, **v}
            for k, v in REGIOES_AGRICOLAS.items()
        ]

    # ── Métodos internos ───────────────────────────────────────────────────────

    def _fazer_requisicao(
        self,
        endpoint: str,
        params: dict,
        regiao_nome: str,
    ) -> dict:
        """Executa a requisição HTTP, trata erros e retorna o JSON."""
        url = BASE_URL + endpoint

        logger.info(
            f"[OpenMeteo] Requisição → {endpoint} | região: {regiao_nome} | "
            f"params: { {k: v for k, v in params.items() if k not in ('latitude', 'longitude')} }"
        )

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"[OpenMeteo] Timeout após {self.timeout}s para {url}"
            )
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"[OpenMeteo] Falha de conexão: {e}")
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(
                f"[OpenMeteo] Erro HTTP {response.status_code}: {response.text}"
            )

        dados = response.json()

        # A API retorna 'error: true' com HTTP 200 em alguns casos
        if dados.get("error"):
            raise ValueError(
                f"[OpenMeteo] API retornou erro: {dados.get('reason', 'desconhecido')}"
            )

        logger.info(
            f"[OpenMeteo] Sucesso → {len(dados.get('daily', {}).get('time', []))} registros diários"
        )
        return dados

    def _obter_regiao(self, regiao_key: str) -> dict:
        """Valida e retorna os dados da região."""
        if regiao_key not in REGIOES_AGRICOLAS:
            disponiveis = list(REGIOES_AGRICOLAS.keys())
            raise KeyError(
                f"[OpenMeteo] Região '{regiao_key}' não encontrada. "
                f"Disponíveis: {disponiveis}"
            )
        return REGIOES_AGRICOLAS[regiao_key]

    def _validar_data(self, data_str: str) -> str:
        """Valida formato YYYY-MM-DD e retorna a string."""
        try:
            datetime.strptime(data_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"[OpenMeteo] Data inválida '{data_str}'. Use o formato YYYY-MM-DD."
            )
        return data_str
import time
import requests
import logging
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

MAX_TENTATIVAS = 3
BACKOFF_SEGUNDOS = 2

BASE_URL_FORECAST = "https://api.open-meteo.com/v1"
BASE_URL_ARCHIVE = "https://archive-api.open-meteo.com/v1"

REGIOES_AGRICOLAS = {
    "sorriso_mt":        {"latitude": -12.54, "longitude": -55.72, "nome": "Sorriso/MT"},
    "ribeirao_preto_sp": {"latitude": -21.17, "longitude": -47.81, "nome": "Ribeirão Preto/SP"},
    "rio_verde_go":      {"latitude": -17.80, "longitude": -50.93, "nome": "Rio Verde/GO"},
    "cascavel_pr":       {"latitude": -24.96, "longitude": -53.46, "nome": "Cascavel/PR"},
    "barreiras_ba":      {"latitude": -12.15, "longitude": -45.00, "nome": "Barreiras/BA"},
}

TIMEZONE_PADRAO = "America/Sao_Paulo"
TIMEOUT_SEGUNDOS = 30


class OpenMeteoClient:
    def __init__(self, timeout: int = TIMEOUT_SEGUNDOS):
        self.timeout = timeout
        self.session = requests.Session()

    def buscar_historico(
        self,
        regiao_key: str,
        variaveis_daily: list[str],
        data_inicio: str,
        data_fim: str,
        variaveis_hourly: Optional[list[str]] = None,
        timezone: str = TIMEZONE_PADRAO,
    ) -> dict:

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
            base_url=BASE_URL_ARCHIVE,
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
            base_url=BASE_URL_FORECAST,
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

    def _fazer_requisicao(
        self,
        base_url: str,
        endpoint: str,
        params: dict,
        regiao_nome: str,
    ) -> dict:
        """Executa a requisição HTTP com retry e backoff exponencial."""
        url = base_url + endpoint

        logger.info(
            f"[OpenMeteo] Requisição → {endpoint} | região: {regiao_nome} | "
            f"params: { {k: v for k, v in params.items() if k not in ('latitude', 'longitude')} }"
        )

        ultimo_erro = None
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
            except requests.exceptions.Timeout as e:
                ultimo_erro = TimeoutError(f"[OpenMeteo] Timeout após {self.timeout}s para {url}")
            except requests.exceptions.ConnectionError as e:
                ultimo_erro = ConnectionError(f"[OpenMeteo] Falha de conexão: {e}")
            except requests.exceptions.HTTPError:
                status = response.status_code
                if status < 500 and status != 429:
                    raise RuntimeError(f"[OpenMeteo] Erro HTTP {status}: {response.text}")
                ultimo_erro = RuntimeError(f"[OpenMeteo] Erro HTTP {status}: {response.text}")
            else:
                dados = response.json()
                if dados.get("error"):
                    raise ValueError(
                        f"[OpenMeteo] API retornou erro: {dados.get('reason', 'desconhecido')}"
                    )
                logger.info(
                    f"[OpenMeteo] Sucesso → {len(dados.get('daily', {}).get('time', []))} registros diários"
                )
                return dados

            if tentativa < MAX_TENTATIVAS:
                espera = BACKOFF_SEGUNDOS ** tentativa
                logger.warning(
                    f"[OpenMeteo] Tentativa {tentativa}/{MAX_TENTATIVAS} falhou. "
                    f"Aguardando {espera}s... Erro: {ultimo_erro}"
                )
                time.sleep(espera)

        raise ultimo_erro

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
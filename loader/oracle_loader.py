"""
loader/oracle_loader.py
-----------------------
Conecta ao Oracle XE e carrega registros nas 4 tabelas do projeto.

Uso como context manager (recomendado nas DAGs):
    with OracleLoader() as loader:
        loader.carregar_agro_weather(registros)
"""

import logging
import os
from datetime import date, datetime
from typing import Optional

import oracledb

logger = logging.getLogger(__name__)

# Em Docker, ORACLE_DSN deve ser "host.docker.internal:1521/XE"
# Fora do Docker, "localhost:1521/XE"
ORACLE_USER = os.getenv("ORACLE_USER", "system")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "123")
ORACLE_DSN = os.getenv("ORACLE_DSN", "localhost:1521/XE")


class OracleLoader:

    def __init__(
        self,
        user: str = ORACLE_USER,
        password: str = ORACLE_PASSWORD,
        dsn: str = ORACLE_DSN,
    ):
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        logger.info(f"[OracleLoader] Conectado ao Oracle: {dsn}")

    # ── Context manager ────────────────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
            logger.error(f"[OracleLoader] Rollback por exceção: {exc_val}")
        self.conn.close()
        return False

    # ── Carga por tabela ───────────────────────────────────────────────────────

    def carregar_agro_weather(self, registros: list[dict]) -> int:
        """MERGE INTO AGRO_WEATHER por (regiao, data)."""
        if not registros:
            logger.info("[AGRO_WEATHER] Sem registros para carregar.")
            return 0

        sql = """
        MERGE INTO AGRO_WEATHER tgt
        USING (SELECT :regiao AS regiao, :data AS data FROM DUAL) src
        ON (tgt.regiao = src.regiao AND tgt.data = src.data)
        WHEN MATCHED THEN UPDATE SET
            tgt.temp_media        = :temp_media,
            tgt.temp_max          = :temp_max,
            tgt.temp_min          = :temp_min,
            tgt.ponto_orvalho     = :ponto_orvalho,
            tgt.umidade_relativa  = :umidade_relativa,
            tgt.precipitacao      = :precipitacao,
            tgt.vento_medio       = :vento_medio,
            tgt.vento_max         = :vento_max,
            tgt.dt_ingestao       = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT
            (regiao, data, temp_media, temp_max, temp_min,
             ponto_orvalho, umidade_relativa, precipitacao,
             vento_medio, vento_max)
        VALUES
            (src.regiao, src.data, :temp_media, :temp_max, :temp_min,
             :ponto_orvalho, :umidade_relativa, :precipitacao,
             :vento_medio, :vento_max)
        """
        return self._executar_merge(sql, registros, "AGRO_WEATHER")

    def carregar_solar_radiation(self, registros: list[dict]) -> int:
        """MERGE INTO SOLAR_RADIATION por (regiao, data)."""
        if not registros:
            logger.info("[SOLAR_RADIATION] Sem registros para carregar.")
            return 0

        sql = """
        MERGE INTO SOLAR_RADIATION tgt
        USING (SELECT :regiao AS regiao, :data AS data FROM DUAL) src
        ON (tgt.regiao = src.regiao AND tgt.data = src.data)
        WHEN MATCHED THEN UPDATE SET
            tgt.radiacao_sup_total      = :radiacao_sup_total,
            tgt.radiacao_sup_ceu_limpo  = :radiacao_sup_ceu_limpo,
            tgt.indice_clareza          = :indice_clareza,
            tgt.par_fotossintetico      = :par_fotossintetico,
            tgt.radiacao_topo_atmosfera = :radiacao_topo_atmosfera,
            tgt.dt_ingestao             = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT
            (regiao, data, radiacao_sup_total, radiacao_sup_ceu_limpo,
             indice_clareza, par_fotossintetico, radiacao_topo_atmosfera)
        VALUES
            (src.regiao, src.data, :radiacao_sup_total, :radiacao_sup_ceu_limpo,
             :indice_clareza, :par_fotossintetico, :radiacao_topo_atmosfera)
        """
        return self._executar_merge(sql, registros, "SOLAR_RADIATION")

    def carregar_evapo(self, registros: list[dict]) -> int:
        """MERGE INTO EVAPO por (regiao, data)."""
        if not registros:
            logger.info("[EVAPO] Sem registros para carregar.")
            return 0

        sql = """
        MERGE INTO EVAPO tgt
        USING (SELECT :regiao AS regiao, :data AS data FROM DUAL) src
        ON (tgt.regiao = src.regiao AND tgt.data = src.data)
        WHEN MATCHED THEN UPDATE SET
            tgt.et0_evapotranspiracao = :et0_evapotranspiracao,
            tgt.deficit_pressao_vapor = :deficit_pressao_vapor,
            tgt.duracao_sol           = :duracao_sol,
            tgt.rajada_vento_max      = :rajada_vento_max,
            tgt.dt_ingestao           = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT
            (regiao, data, et0_evapotranspiracao, deficit_pressao_vapor,
             duracao_sol, rajada_vento_max)
        VALUES
            (src.regiao, src.data, :et0_evapotranspiracao, :deficit_pressao_vapor,
             :duracao_sol, :rajada_vento_max)
        """
        return self._executar_merge(sql, registros, "EVAPO")

    def carregar_agriculture(self, registros: list[dict]) -> int:
        """
        INSERT INTO AGRICULTURE.
        Não usa MERGE pois dt_ingestao é parte da chave UNIQUE —
        cada execução diária gera um snapshot novo de previsão.
        """
        if not registros:
            logger.info("[AGRICULTURE] Sem registros para carregar.")
            return 0

        sql = """
        INSERT INTO AGRICULTURE
            (regiao, data_previsao, et0_evapotranspiracao, precipitacao_prevista,
             temp_max_prevista, radiacao_solar, prob_precipitacao, dt_ingestao)
        VALUES
            (:regiao, :data_previsao, :et0_evapotranspiracao, :precipitacao_prevista,
             :temp_max_prevista, :radiacao_solar, :prob_precipitacao, :dt_ingestao)
        """
        params = [_preparar_registro(r) for r in registros]

        with self.conn.cursor() as cur:
            cur.executemany(sql, params)

        self.conn.commit()
        logger.info(f"[AGRICULTURE] {len(params)} registros inseridos.")
        return len(params)

    def registrar_execucao(self, dag_id: str, tabela: str, qtd_registros: int) -> None:
        """Grava uma linha em PIPELINE_LOG para auditoria de cada execução."""
        sql = """
        INSERT INTO PIPELINE_LOG (dag_id, tabela, qtd_registros, dt_execucao)
        VALUES (:dag_id, :tabela, :qtd_registros, CURRENT_TIMESTAMP)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, {"dag_id": dag_id, "tabela": tabela, "qtd_registros": qtd_registros})
        self.conn.commit()
        logger.info(f"[PIPELINE_LOG] {dag_id} → {tabela}: {qtd_registros} registros")

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _executar_merge(self, sql: str, registros: list[dict], tabela: str) -> int:
        # dt_ingestao é gerenciado pelo CURRENT_TIMESTAMP no SQL do MERGE,
        # por isso não pode aparecer como bind variable nos params
        params = [
            {k: v for k, v in _preparar_registro(r).items() if k != "dt_ingestao"}
            for r in registros
        ]

        with self.conn.cursor() as cur:
            cur.executemany(sql, params)

        self.conn.commit()
        logger.info(f"[{tabela}] MERGE concluído: {len(params)} registros processados.")
        return len(params)


# ── Serialização de tipos Python → Oracle ─────────────────────────────────────

def _preparar_registro(registro: dict) -> dict:
    return {chave: _converter_valor(valor) for chave, valor in registro.items()}


def _converter_valor(valor):
    if isinstance(valor, (date, datetime)):
        return valor
    if isinstance(valor, str):
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(valor, fmt) if " " in valor or "." in valor else \
                       datetime.strptime(valor, fmt).date()
            except ValueError:
                continue
    return valor

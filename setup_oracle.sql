-- setup_oracle.sql
-- DDL para criação das 4 tabelas do projeto Global Solution
-- Conexão: system/123@localhost:1521/XE
--
-- Execução via SQL*Plus:
--   sqlplus system/123@localhost:1521/XE @setup_oracle.sql

-- ── AGRO_WEATHER ──────────────────────────────────────────────────────────────
-- Dados agrometeorológicos diários da NASA POWER (fonte: MERRA-2)

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE AGRO_WEATHER';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE AGRO_WEATHER (
    id                  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao              VARCHAR2(100)  NOT NULL,
    data                DATE           NOT NULL,
    temp_media          NUMBER(5,2),        -- T2M    (°C)
    temp_max            NUMBER(5,2),        -- T2M_MAX (°C)
    temp_min            NUMBER(5,2),        -- T2M_MIN (°C)
    ponto_orvalho       NUMBER(5,2),        -- T2MDEW  (°C)
    umidade_relativa    NUMBER(5,2),        -- RH2M    (%)
    precipitacao        NUMBER(7,2),        -- PRECTOTCORR (mm/dia)
    vento_medio         NUMBER(6,2),        -- WS2M    (m/s)
    vento_max           NUMBER(6,2),        -- WS2M_MAX (m/s)
    dt_ingestao         TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agro_weather UNIQUE (regiao, data)
);

-- ── SOLAR_RADIATION ───────────────────────────────────────────────────────────
-- Dados de radiação solar diários da NASA POWER (fonte: CERES/SRB)

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE SOLAR_RADIATION';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE SOLAR_RADIATION (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data                    DATE           NOT NULL,
    radiacao_sup_total      NUMBER(10,2),   -- ALLSKY_SFC_SW_DWN (Wh/m²/dia)
    radiacao_sup_ceu_limpo  NUMBER(10,2),   -- CLRSKY_SFC_SW_DWN (Wh/m²/dia)
    indice_clareza          NUMBER(5,4),    -- ALLSKY_KT (0.0000 a 1.0000)
    par_fotossintetico      NUMBER(8,2),    -- ALLSKY_SFC_PAR_TOT (W/m²)
    radiacao_topo_atmosfera NUMBER(10,2),   -- TOA_SW_DWN (Wh/m²/dia)
    dt_ingestao             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_solar_radiation UNIQUE (regiao, data)
);

-- ── WEATHER ───────────────────────────────────────────────────────────────────
-- Histórico climático diário do Open-Meteo (últimos 90 dias)

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE WEATHER';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE WEATHER (
    id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao          VARCHAR2(100)  NOT NULL,
    data            DATE           NOT NULL,
    temp_max        NUMBER(5,2),
    temp_min        NUMBER(5,2),
    precipitacao    NUMBER(7,2),
    umidade_max     NUMBER(5,2),
    vento_max       NUMBER(6,2),
    dt_ingestao     TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_weather UNIQUE (regiao, data)
);

-- ── AGRICULTURE ───────────────────────────────────────────────────────────────
-- Previsão agrícola de 16 dias do Open-Meteo (snapshot diário)

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE AGRICULTURE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE AGRICULTURE (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data_previsao           DATE           NOT NULL,
    et0_evapotranspiracao   NUMBER(7,2),
    precipitacao_prevista   NUMBER(7,2),
    temp_max_prevista       NUMBER(5,2),
    radiacao_solar          NUMBER(8,2),
    prob_precipitacao       NUMBER(5,2),
    dt_ingestao             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agriculture UNIQUE (regiao, data_previsao, dt_ingestao)
);

-- ── Verificação ───────────────────────────────────────────────────────────────
SELECT table_name FROM user_tables
WHERE table_name IN ('AGRO_WEATHER', 'SOLAR_RADIATION', 'WEATHER', 'AGRICULTURE')
ORDER BY table_name;

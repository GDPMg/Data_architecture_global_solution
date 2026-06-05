-- analytical_queries.sql
-- 6 consultas analíticas sobre dados agroclimáticos
-- Conexão: system/123@localhost:1521/XE
--
-- Tabelas utilizadas:
--   AGRO_WEATHER         (NASA POWER — histórico agrometeorológico)
--   SOLAR_RADIATION      (NASA POWER — radiação solar histórica)
--   EVAPO                (Open-Meteo — evapotranspiração e estresse hídrico histórico)
--   AGRICULTURAL_FORECAST (Open-Meteo — previsão agrícola 16 dias)
-- ──────────────────────────────────────────────────────────────────────────────


-- ── Q1: Balanço Hídrico Diário por Região ─────────────────────────────────────
-- Objetivo : Mostra se cada região está em déficit ou excesso hídrico no dia.
-- Lógica   : balanço = precipitação (mm) - ET0 (mm/dia)
--            Positivo = excesso  |  Negativo = déficit
-- Tabelas  : AGRO_WEATHER JOIN EVAPO
-- Janela   : últimos 30 dias

SELECT
    aw.regiao,
    aw.data,
    aw.precipitacao                                                         AS precipitacao_mm,
    e.et0_evapotranspiracao                                                 AS et0_mm,
    ROUND(aw.precipitacao - e.et0_evapotranspiracao, 2)                    AS balanco_hidrico_mm,
    CASE
        WHEN aw.precipitacao - e.et0_evapotranspiracao >= 0 THEN 'EXCESSO'
        ELSE 'DEFICIT'
    END                                                                     AS situacao
FROM AGRO_WEATHER aw
JOIN EVAPO e ON aw.regiao = e.regiao AND aw.data = e.data
WHERE aw.data >= SYSDATE - 30
ORDER BY aw.regiao, aw.data;


-- ── Q2: Ranking de Estresse Hídrico por Região (últimos 30 dias) ──────────────
-- Objetivo : Identifica quais regiões sofreram maior estresse hídrico.
-- Lógica   : Média do déficit de pressão de vapor (VPD) e ET0 por região.
--            VPD alto + ET0 alto = plantas sob alto estresse.
-- Tabelas  : EVAPO
-- Janela   : últimos 30 dias

SELECT
    regiao,
    ROUND(AVG(deficit_pressao_vapor), 3)                                   AS vpd_medio_kpa,
    ROUND(AVG(et0_evapotranspiracao), 2)                                   AS et0_medio_mm,
    ROUND(AVG(duracao_sol), 2)                                             AS horas_sol_medias,
    ROUND(MAX(rajada_vento_max), 2)                                        AS maior_rajada_kmh,
    RANK() OVER (ORDER BY AVG(deficit_pressao_vapor) DESC)                 AS rank_estresse
FROM EVAPO
WHERE data >= SYSDATE - 30
GROUP BY regiao
ORDER BY rank_estresse;


-- ── Q3: Previsão de Déficit Hídrico — Próximos 16 Dias ────────────────────────
-- Objetivo : Antecipa regiões em risco de déficit hídrico na próxima quinzena.
-- Lógica   : deficit_projetado = SUM(ET0) - SUM(precipitação ponderada)
--            Precipitação ponderada = precipitacao_prevista * prob_precipitacao / 100
--            (desconta a probabilidade de não chover)
-- Tabelas  : AGRICULTURAL_FORECAST

SELECT
    regiao,
    COUNT(*)                                                               AS dias_previstos,
    ROUND(SUM(et0_evapotranspiracao), 2)                                   AS et0_acumulado_mm,
    ROUND(SUM(precipitacao_prevista), 2)                                   AS precip_prevista_mm,
    ROUND(SUM(precipitacao_prevista * prob_precipitacao / 100), 2)         AS precip_ponderada_mm,
    ROUND(SUM(et0_evapotranspiracao)
        - SUM(precipitacao_prevista * prob_precipitacao / 100), 2)         AS deficit_projetado_mm,
    ROUND(MAX(temp_max_prevista), 2)                                       AS temp_max_pico_c
FROM AGRICULTURAL_FORECAST
WHERE data_previsao >= TRUNC(SYSDATE)
GROUP BY regiao
ORDER BY deficit_projetado_mm DESC;


-- ── Q4: Eficiência Fotossintética e Cobertura de Nuvens ───────────────────────
-- Objetivo : Avalia o impacto de nuvens e umidade na radiação útil para as plantas.
-- Lógica   : perda_nuvens = radiacao_sup_ceu_limpo - radiacao_sup_total (Wh/m²)
--            Índice de clareza próximo de 1 = céu limpo; próximo de 0 = muito nublado.
--            PAR = radiação fotossintética ativa disponível para as culturas.
-- Tabelas  : SOLAR_RADIATION JOIN AGRO_WEATHER
-- Granul.  : mensal por região

SELECT
    sr.regiao,
    TO_CHAR(sr.data, 'YYYY-MM')                                            AS mes,
    ROUND(AVG(sr.indice_clareza), 4)                                       AS clareza_media,
    ROUND(AVG(sr.par_fotossintetico), 2)                                   AS par_medio_wm2,
    ROUND(AVG(sr.radiacao_sup_ceu_limpo - sr.radiacao_sup_total), 2)       AS perda_nuvens_wh_m2,
    ROUND(AVG(aw.umidade_relativa), 2)                                     AS umidade_media_pct,
    ROUND(AVG(aw.ponto_orvalho), 2)                                        AS ponto_orvalho_medio_c
FROM SOLAR_RADIATION sr
JOIN AGRO_WEATHER aw ON sr.regiao = aw.regiao AND sr.data = aw.data
GROUP BY sr.regiao, TO_CHAR(sr.data, 'YYYY-MM')
ORDER BY sr.regiao, mes;


-- ── Q5: Sumário Climático Mensal por Região ───────────────────────────────────
-- Objetivo : Visão consolidada do clima mês a mês para acompanhar tendências
--            sazonais (temperatura, chuva, umidade).
-- Tabelas  : AGRO_WEATHER
-- Janela   : últimos 120 dias (~4 meses)

SELECT
    regiao,
    TO_CHAR(data, 'YYYY-MM')                                               AS mes,
    ROUND(AVG(temp_media), 2)                                              AS temp_media_c,
    ROUND(MAX(temp_max), 2)                                                AS temp_max_c,
    ROUND(MIN(temp_min), 2)                                                AS temp_min_c,
    ROUND(SUM(precipitacao), 2)                                            AS precip_acumulada_mm,
    ROUND(AVG(umidade_relativa), 2)                                        AS umidade_media_pct,
    ROUND(AVG(vento_medio), 2)                                             AS vento_medio_ms,
    COUNT(*)                                                               AS dias_com_dado
FROM AGRO_WEATHER
WHERE data >= SYSDATE - 120
GROUP BY regiao, TO_CHAR(data, 'YYYY-MM')
ORDER BY regiao, mes;


-- ── Q6: Alertas de Condições Críticas para Agricultura ────────────────────────
-- Objetivo : Conta dias de risco agrícola por tipo de evento em cada região.
-- Lógica   : Três critérios de alerta:
--   - Calor extremo  : temp_max > 35°C
--   - Seca severa    : sem chuva (precipitacao = 0) e demanda hídrica alta (ET0 > 6 mm)
--   - Vento perigoso : rajada_vento_max > 80 km/h
-- Tabelas  : AGRO_WEATHER JOIN EVAPO
-- Janela   : últimos 30 dias

SELECT
    aw.regiao,
    COUNT(*)                                                               AS total_dias_criticos,
    SUM(CASE WHEN aw.temp_max > 35 THEN 1 ELSE 0 END)                     AS dias_calor_extremo,
    SUM(CASE WHEN aw.precipitacao = 0
             AND e.et0_evapotranspiracao > 6 THEN 1 ELSE 0 END)           AS dias_seca_severa,
    SUM(CASE WHEN e.rajada_vento_max > 80 THEN 1 ELSE 0 END)              AS dias_vento_perigoso
FROM AGRO_WEATHER aw
JOIN EVAPO e ON aw.regiao = e.regiao AND aw.data = e.data
WHERE aw.data >= SYSDATE - 30
  AND (
      aw.temp_max > 35
      OR (aw.precipitacao = 0 AND e.et0_evapotranspiracao > 6)
      OR e.rajada_vento_max > 80
  )
GROUP BY aw.regiao
ORDER BY total_dias_criticos DESC;

# Global Solution — Pipeline de Dados Agroclimáticos

## Sumário

1. [Descrição da Solução](#1-descrição-da-solução)
2. [Objetivo do Pipeline](#2-objetivo-do-pipeline)
3. [Fontes de Dados](#3-fontes-de-dados)
4. [Arquitetura do Pipeline](#4-arquitetura-do-pipeline)
5. [Estrutura de Diretórios](#5-estrutura-de-diretórios)
6. [Etapas das DAGs](#6-etapas-das-dags)
7. [Transformações Realizadas](#7-transformações-realizadas)
8. [Modelagem das Tabelas Oracle](#8-modelagem-das-tabelas-oracle)
9. [Consultas Analíticas](#9-consultas-analíticas)
10. [Conclusão Técnica](#10-conclusão-técnica)
11. [Como Executar](#11-como-executar)

---

## 1. Descrição da Solução

O **Projeto** é um pipeline de ingestão e análise de dados agroclimáticos desenvolvido para monitorar condições meteorológicas em 5 regiões agrícolas estratégicas do Brasil. A solução integra duas APIs públicas de dados climáticos — **Open-Meteo** e **NASA POWER** — orquestrando a coleta, transformação e carga diária de informações em um banco de dados **Oracle**, com toda a execução gerenciada pelo **Apache Airflow** em ambiente **Docker**.

A proposta resolve um problema real do agronegócio: a necessidade de centralizar dados de fontes heterogêneas (satélite, modelos atmosféricos e previsão numérica) em um único repositório estruturado, pronto para análise e tomada de decisão sobre irrigação, plantio e gestão de riscos climáticos.

**Regiões monitoradas:**

| Região | Estado |
|---|---|
| Sorriso | Mato Grosso |
| Ribeirão Preto | São Paulo |
| Rio Verde | Goiás |
| Cascavel | Paraná |
| Barreiras | Bahia |

---

## 2. Objetivo do Pipeline

O pipeline foi desenvolvido com quatro objetivos principais:

1. **Consolidar dados históricos** de temperatura, precipitação, umidade, radiação solar, evapotranspiração e vento, das duas principais APIs climáticas gratuitas disponíveis, evitando dependência de uma única fonte.

2. **Calcular indicadores exclusivos** não disponíveis em uma única API: ET0 histórica, déficit de pressão de vapor (VPD), índice de clareza atmosférica e radiação fotossintética ativa (PAR).

3. **Gerar previsões agrícolas** de até 16 dias, com substituição automática dos dados a cada execução, garantindo que a tabela de previsão sempre reflita o estado mais atual dos modelos climáticos.

4. **Disponibilizar uma base analítica** pronta para consultas sobre balanço hídrico, estresse hídrico, eficiência fotossintética e alertas de condições críticas para a agricultura.

---

## 3. Fontes de Dados

### 3.1 Open-Meteo

- **URL**: `https://open-meteo.com`
- **Endpoints utilizados**:
  - Archive API (`archive-api.open-meteo.com/v1/archive`) — dados históricos diários
  - Forecast API (`api.open-meteo.com/v1/forecast`) — previsão de até 16 dias
- **Custo**: Gratuito, sem autenticação
- **Latência**: Sem latência — dados do dia anterior disponíveis no mesmo dia
- **Granularidade**: Diária por coordenada geográfica
- **Dados fornecidos ao projeto**:

| Variável da API | Tabela | Descrição | Unidade |
|---|---|---|---|
| `et0_fao_evapotranspiration` | EVAPO | Evapotranspiração de referência FAO-56 | mm/dia |
| `vapor_pressure_deficit_max` | EVAPO | Déficit máximo de pressão de vapor | kPa |
| `sunshine_duration` | EVAPO | Duração efetiva do sol | horas |
| `wind_gusts_10m_max` | EVAPO | Rajada máxima de vento a 10m | km/h |
| `et0_fao_evapotranspiration` | AGRICULTURAL_FORECAST | ET0 prevista | mm |
| `precipitation_sum` | AGRICULTURAL_FORECAST | Precipitação prevista | mm |
| `temperature_2m_max` | AGRICULTURAL_FORECAST | Temperatura máxima prevista | °C |
| `shortwave_radiation_sum` | AGRICULTURAL_FORECAST | Radiação solar prevista | MJ/m² |
| `precipitation_probability_max` | AGRICULTURAL_FORECAST | Probabilidade de chuva | % |

### 3.2 NASA POWER

- **URL**: `https://power.larc.nasa.gov`
- **Endpoint**: `api/temporal/daily/point`
- **Comunidade**: `AG` (Agriculture)
- **Custo**: Gratuito, sem autenticação
- **Latência**: ~7 dias (dados de satélite exigem processamento)
- **Modelo de dados**: MERRA-2 (reanálise) e CERES/SRB (satélite)
- **Granularidade**: Diária por coordenada geográfica
- **Dados fornecidos ao projeto**:

| Parâmetro NASA | Tabela | Descrição | Unidade |
|---|---|---|---|
| `T2M` | AGRO_WEATHER | Temperatura média a 2m | °C |
| `T2M_MAX` | AGRO_WEATHER | Temperatura máxima a 2m | °C |
| `T2M_MIN` | AGRO_WEATHER | Temperatura mínima a 2m | °C |
| `T2MDEW` | AGRO_WEATHER | Ponto de orvalho a 2m | °C |
| `RH2M` | AGRO_WEATHER | Umidade relativa a 2m | % |
| `PRECTOTCORR` | AGRO_WEATHER | Precipitação corrigida | mm/dia |
| `WS2M` | AGRO_WEATHER | Velocidade média do vento a 2m | m/s |
| `WS2M_MAX` | AGRO_WEATHER | Velocidade máxima do vento a 2m | m/s |
| `ALLSKY_SFC_SW_DWN` | SOLAR_RADIATION | Radiação solar total (céu aberto) | Wh/m²/dia |
| `CLRSKY_SFC_SW_DWN` | SOLAR_RADIATION | Radiação solar (céu limpo) | Wh/m²/dia |
| `ALLSKY_KT` | SOLAR_RADIATION | Índice de clareza atmosférica | adimensional (0–1) |
| `ALLSKY_SFC_PAR_TOT` | SOLAR_RADIATION | Radiação fotossintética ativa | W/m² |
| `TOA_SW_DWN` | SOLAR_RADIATION | Radiação no topo da atmosfera | Wh/m²/dia |

---

## 4. Arquitetura do Pipeline

```
┌─────────────────────────────────────────────────────────┐
│                    Apache Airflow                        │
│                                                         │
│   trigger_master (06:00 diário)                         │
│        │                                                │
│   ┌────┴────────────────────────────────────┐           │
│   │  4 DAGs disparadas sequencialmente      │           │
│   │                                         │           │
│   │  nasa_power_agro_weather                │           │
│   │  nasa_power_solar_radiation             │           │
│   │  open_meteo_evapo                       │           │
│   │  open_meteo_agricultural_forecast       │           │
│   └─────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
         │                            │
         ▼                            ▼
  ┌─────────────┐             ┌──────────────┐
  │ NASA POWER  │             │  Open-Meteo  │
  │  MERRA-2    │             │ Archive/Fcst │
  │  CERES/SRB  │             │              │
  └──────┬──────┘             └──────┬───────┘
         │                           │
         ▼                           ▼
  ┌─────────────────────────────────────────┐
  │            Python ETL                   │
  │  Extração → Transformação → Staging     │
  │         (JSON temporário)               │
  └─────────────────┬───────────────────────┘
                    │
                    ▼
  ┌─────────────────────────────────────────┐
  │            Oracle XE                    │
  │                                         │
  │  AGRO_WEATHER      SOLAR_RADIATION      │
  │  EVAPO             AGRICULTURAL_FORECAST│
  │  PIPELINE_LOG                           │
  └─────────────────────────────────────────┘
                    │
                    ▼
  ┌─────────────────────────────────────────┐
  │        Consultas Analíticas             │
  │  (queries/analytical_queries.sql)       │
  └─────────────────────────────────────────┘
```

**Tecnologias utilizadas:**

| Componente | Tecnologia | Versão |
|---|---|---|
| Linguagem | Python | 3.11 |
| Orquestrador | Apache Airflow | 2.x |
| Banco de dados | Oracle Database XE | — |
| Driver Oracle | oracledb | — |
| Containerização | Docker + Docker Compose | — |
| Banco metadados Airflow | PostgreSQL | 15 |

---

## 5. Estrutura de Diretórios

```
global-solution/
├── dags/
│   ├── trigger/
│   │   └── trigger_master.py              # DAG orquestrador (dispara os 4 pipelines)
│   └── upload/
│       ├── open_meteo/
│       │   ├── dag_evapo.py               # Pipeline EVAPO
│       │   └── dag_agricultural_forecast.py  # Pipeline AGRICULTURAL_FORECAST
│       └── nasa_power/
│           ├── dag_agro_weather.py        # Pipeline AGRO_WEATHER
│           └── dag_solar_radiation.py     # Pipeline SOLAR_RADIATION
│
├── ingestion/
│   ├── open_meteo/
│   │   ├── evapo/
│   │   │   └── evapo.py                  # ETL da tabela EVAPO
│   │   └── agricultural_forecast/
│   │       └── agricultural_forecast.py  # ETL da tabela AGRICULTURAL_FORECAST
│   └── nasa_power/
│       ├── agro_weather/
│       │   └── agro_weather.py           # ETL da tabela AGRO_WEATHER
│       └── solar_radiation/
│           └── solar_radiation.py        # ETL da tabela SOLAR_RADIATION
│
├── script/
│   ├── api_open_meteo.py                 # Cliente HTTP Open-Meteo
│   └── api_nasa_power.py                 # Cliente HTTP NASA POWER
│
├── loader/
│   └── oracle_loader.py                  # Carregamento Oracle (MERGE)
│
├── utils/
│   ├── dag_factory.py                    # Fábrica de tasks para Airflow
│   ├── ingestion_utils.py                # Utilitários de ingestão
│   └── json_utils.py                     # Serialização JSON para staging
│
├── queries/
│   └── analytical_queries.sql            # 6 consultas analíticas
│
├── setup_oracle.sql                      # DDL: criação das tabelas Oracle
└── docker-compose.yaml                   # Configuração do ambiente Docker
```

---

## 6. Etapas das DAGs

### DAG Mestre — `trigger_master`

Executa todos os dias às **06:00 (horário de São Paulo)** e dispara os 4 pipelines de ingestão sequencialmente.

```
trigger_master (06:00 diário)
    │
    ├─ trigger_agro_weather          → nasa_power_agro_weather
    ├─ trigger_solar_radiation       → nasa_power_solar_radiation
    ├─ trigger_open_meteo_evapo      → open_meteo_evapo
    └─ trigger_agricultural_forecast → open_meteo_agricultural_forecast
```

Cada trigger aguarda a conclusão da DAG anterior (`wait_for_completion=True`) e verifica o status a cada 30 segundos.

### Pipelines individuais (padrão comum)

Cada um dos 4 pipelines segue o mesmo fluxo de 3 tasks:

```
extrair_transformar_task
        │
        │  (puxa dados da API, transforma e salva em JSON staging)
        │
        ▼
carregar_oracle_task
        │
        │  (lê JSON staging, executa MERGE no Oracle, registra em PIPELINE_LOG)
        │
        ▼
limpar_staging_task
        │
        │  (remove arquivo JSON temporário)
```

**Modo de carga (parâmetro `modo`):**

Os pipelines das 3 tabelas históricas (AGRO_WEATHER, SOLAR_RADIATION, EVAPO) aceitam um parâmetro configurável ao disparar manualmente no Airflow:

| Modo | Comportamento | Quando usar |
|---|---|---|
| `incremental` *(padrão)* | Carrega apenas o dia anterior (d-1) | Execução diária automática |
| `full` | Carrega os últimos 120 dias | Carga inicial ou reprocessamento |

`FULL`: foi definido como um cenário hipotético em que seriam considerados 120 dias. Se fosse necessário pegar todo o histórico real, o processamento seria muito grande.

Para selecionar o modo **full**, acesse a DAG no Airflow UI → **Trigger DAG w/ config** → altere o parâmetro `modo` para `full`.

---

## 7. Transformações Realizadas

### 7.1 AGRO_WEATHER (NASA POWER)

| Dado | Transformação |
|---|---|
| Fill value `-999` | Substituído por `NULL` |
| Temperatura fora de `[-80°C, 60°C]` | Substituída por `NULL` |
| Umidade relativa | Clamp para `[0%, 100%]` |
| Vento negativo | Substituído por `NULL` |
| Precipitação negativa | Substituída por `NULL` |
| Registros com todos os valores nulos | Descartados |
| Todos os valores numéricos | Arredondados a 2 casas decimais |
| Data `YYYYMMDD` (string NASA) | Convertida para `date` Python |

### 7.2 SOLAR_RADIATION (NASA POWER)

| Dado | Transformação |
|---|---|
| Fill value `-999` | Substituído por `NULL` |
| Índice de clareza (`ALLSKY_KT`) | Clamp para `[0, 1]`, arredondado a 4 casas decimais |
| Demais variáveis de radiação | Arredondadas a 2 casas decimais |
| Registros totalmente nulos | Descartados |

### 7.3 EVAPO (Open-Meteo)

| Dado | Transformação |
|---|---|
| `sunshine_duration` (segundos) | Convertida para horas (`÷ 3600`), clamp para `[0, 24]` |
| ET0 negativa | Substituída por `NULL` (fisicamente impossível) |
| Rajada de vento negativa | Substituída por `NULL` |
| Déficit de pressão de vapor | Arredondado a 3 casas decimais |
| ET0 e rajada | Arredondados a 2 casas decimais |
| Duração do sol | Arredondada a 2 casas decimais |

### 7.4 AGRICULTURAL_FORECAST (Open-Meteo)

| Dado | Transformação |
|---|---|
| Probabilidade de precipitação > 100% | Truncada a `100.0` |
| Todos os valores numéricos | Arredondados a 2 casas decimais |
| Datas nulas | Descartadas |

### 7.5 Carga no Oracle — Padrão MERGE

Todas as tabelas utilizam `MERGE` ao invés de `INSERT` puro. O comportamento é:

- **Registro já existe** (mesma região + data): atualiza todos os campos e renova `dt_ingestao`
- **Registro novo**: insere

Isso garante **idempotência** — a mesma execução pode rodar múltiplas vezes sem duplicar dados.

---

## 8. Modelagem das Tabelas Oracle

### AGRO_WEATHER
Dados agrometeorológicos históricos diários — fonte NASA POWER (modelo MERRA-2).

```sql
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
```

### SOLAR_RADIATION
Dados de radiação solar históricos diários — fonte NASA POWER (satélite CERES/SRB).

```sql
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
```

### EVAPO
Dados históricos de evapotranspiração e estresse hídrico — fonte Open-Meteo (últimos 120 dias).

```sql
CREATE TABLE EVAPO (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data                    DATE           NOT NULL,
    et0_evapotranspiracao   NUMBER(7,2),   -- et0_fao_evapotranspiration (mm/dia)
    deficit_pressao_vapor   NUMBER(6,3),   -- vapor_pressure_deficit_max (kPa)
    duracao_sol             NUMBER(5,2),   -- sunshine_duration (horas)
    rajada_vento_max        NUMBER(6,2),   -- wind_gusts_10m_max (km/h)
    dt_ingestao             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_evapo_diario UNIQUE (regiao, data)
);
```

### AGRICULTURAL_FORECAST
Previsão agrícola de 16 dias — fonte Open-Meteo (atualizada diariamente com substituição).

```sql
CREATE TABLE AGRICULTURAL_FORECAST (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    regiao                  VARCHAR2(100)  NOT NULL,
    data_previsao           DATE           NOT NULL,
    et0_evapotranspiracao   NUMBER(7,2),   -- et0_fao_evapotranspiration (mm)
    precipitacao_prevista   NUMBER(7,2),   -- precipitation_sum (mm)
    temp_max_prevista       NUMBER(5,2),   -- temperature_2m_max (°C)
    radiacao_solar          NUMBER(8,2),   -- shortwave_radiation_sum (MJ/m²)
    prob_precipitacao       NUMBER(5,2),   -- precipitation_probability_max (%)
    dt_ingestao             TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agriculture UNIQUE (regiao, data_previsao)
);
```

### PIPELINE_LOG
Auditoria de cada execução bem-sucedida.

```sql
CREATE TABLE PIPELINE_LOG (
    id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_id        VARCHAR2(100)  NOT NULL,
    tabela        VARCHAR2(100)  NOT NULL,
    qtd_registros NUMBER         NOT NULL,
    dt_execucao   TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);
```

---

## 9. Consultas Analíticas

As consultas estão disponíveis em `queries/analytical_queries.sql`. Abaixo um resumo:

| # | Nome | Tabelas | Objetivo |
|---|---|---|---|
| Q1 | Balanço Hídrico Diário | AGRO_WEATHER + EVAPO | Identifica dias de déficit ou excesso hídrico por região (últimos 30 dias) |
| Q2 | Ranking de Estresse Hídrico | EVAPO | Classifica regiões pelo nível de estresse hídrico (VPD médio) |
| Q3 | Previsão de Déficit — 16 dias | AGRICULTURAL_FORECAST | Projeta risco de déficit hídrico com precipitação ponderada pela probabilidade |
| Q4 | Eficiência Fotossintética | SOLAR_RADIATION + AGRO_WEATHER | Avalia impacto de nuvens e umidade na radiação útil para as plantas |
| Q5 | Sumário Climático Mensal | AGRO_WEATHER | Consolidação mensal de temperatura, chuva e umidade (últimos 120 dias) |
| Q6 | Alertas de Condições Críticas | AGRO_WEATHER + EVAPO | Conta dias com calor extremo (>35°C), seca severa ou vento perigoso (>80 km/h) |

---

## 10. Conclusão Técnica

O projeto demonstra a viabilidade de construir um pipeline de dados agroclimáticos de baixo custo e alta confiabilidade, utilizando exclusivamente APIs públicas e tecnologias open source.

**Pontos de destaque técnico:**

- **Complementaridade das fontes**: NASA POWER fornece dados validados por satélite com maior rigor científico; Open-Meteo fornece dados mais recentes e variáveis de estresse hídrico (VPD, ET0 histórico) não disponíveis na NASA.

- **Idempotência**: O padrão MERGE garante que reexecuções não geram duplicatas, simplificando a estratégia de reprocessamento.

- **Modos de carga**: A separação entre carga `full` (120 dias) e `incremental` (d-1) otimiza o volume de dados transferido na execução diária automática, reduzindo chamadas à API.

- **Isolamento de falhas**: A função `executar_para_todas_regioes` captura exceções por região — uma falha em Sorriso/MT não impede a ingestão de Cascavel/PR.

- **Staging JSON**: O arquivo intermediário entre a extração e a carga no Oracle desacopla as duas fases, permitindo reprocessar a carga sem nova chamada à API.

- **Auditoria**: O `PIPELINE_LOG` registra toda execução, habilitando rastreabilidade completa de quando e quantos dados cada pipeline carregou.

**Limitações identificadas:**

- A NASA POWER tem latência de ~7 dias, o que cria uma defasagem entre os dados das duas fontes em consultas com JOIN por data.
- O `LocalExecutor` do Airflow processa as DAGs de forma sequencial; para escalar para dezenas de regiões, seria necessário migrar para `CeleryExecutor`.

---

## 11. Como Executar

### Pré-requisitos

| Ferramenta | Versão recomendada |
|---|---|
| Docker Desktop | Mais recente |
| Oracle Database XE | 21c ou 18c |
| SQL Developer | Para executar `setup_oracle.sql` |

> **Importante:** O Oracle XE deve estar instalado e rodando no Windows (fora do Docker). O Airflow, rodando em container, acessa o banco via `host.docker.internal:1521/XE`.

---

### Passo 1 — Criar as tabelas no Oracle

Abra o SQL*Plus e execute:

```bash
sqlplus system/123@localhost:1521/XE @setup_oracle.sql
```

Verifique se as 5 tabelas foram criadas:

```
AGRO_WEATHER
AGRICULTURAL_FORECAST
EVAPO
PIPELINE_LOG
SOLAR_RADIATION
```

---

### Passo 2 — Subir o ambiente Docker

Na raiz do projeto:

```bash
docker compose up -d
```

Aguarde ~30 segundos para o Airflow inicializar. O comando sobe:
- PostgreSQL (banco de metadados do Airflow)
- Airflow Webserver (UI)
- Airflow Scheduler (executor de DAGs)

---

### Passo 3 — Acessar o Airflow

Abra o navegador em: **http://localhost:8080**

| Campo | Valor |
|---|---|
| Usuário | `admin` |
| Senha | `admin` |

---

### Passo 4 — Carga inicial (Full)

Na primeira execução, é necessário carregar o histórico completo (120 dias). Faça isso individualmente para cada DAG histórica:

1. Acesse a DAG desejada (ex: `open_meteo_evapo`)
2. Clique em **Trigger DAG** (botão de play)
3. Selecione **Trigger DAG w/ config**
4. Altere o parâmetro `modo` de `incremental` para `full`
5. Confirme

Repita para: `nasa_power_agro_weather`, `nasa_power_solar_radiation`, `open_meteo_evapo`.

A DAG `open_meteo_agricultural_forecast` não possui parâmetro de modo — sempre carrega os 16 dias de previsão.

---

### Passo 5 — Execução automática diária

Após a carga inicial, ative a DAG mestre para execução automática:

1. Acesse `trigger_master` no Airflow
2. Ative o toggle **Paused → Active**

A partir daí, todos os dias às 06:00 (horário de São Paulo) o pipeline executa automaticamente no modo `incremental`.

---

### Passo 6 — Executar as consultas analíticas

Conecte ao Oracle e execute o arquivo de queries: `analytical_queries.sql` no SQL Developer e execute cada query individualmente.

---

### Desligar o ambiente

```bash
docker compose down
```

---

### Variáveis de ambiente (opcional)

As credenciais do Oracle podem ser sobrescritas via variáveis de ambiente antes de subir o Docker:

| Variável | Padrão | Descrição |
|---|---|---|
| `ORACLE_USER` | `system` | Usuário do Oracle |
| `ORACLE_PASSWORD` | `123` | Senha do Oracle |
| `ORACLE_DSN` | `host.docker.internal:1521/XE` | String de conexão |

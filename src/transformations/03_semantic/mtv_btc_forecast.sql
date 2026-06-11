-- =============================================================================
-- METRIC VIEW: z_bitcoin.gold.mtv_btc_forecast
--
-- Camada semântica para BI / AI-BI / Genie sobre previsão de preço BTC.
-- Fonte : z_bitcoin.gold.f_forecast (saída do AI_FORECAST, horizonte 24h, PI 95%)
-- Grãos : 15m / 30m / 1h / 4h (múltiplos de 15 — menores não deriváveis)
--
-- Colunas de saída do AI_FORECAST (Databricks):
--   ds          TIMESTAMP   horário da previsão
--   symbol      STRING      chave de agrupamento
--   y_forecast  DOUBLE      previsão pontual (preço de fechamento)
--   y_lower     DOUBLE      limite inferior (PI = 0.95)
--   y_upper     DOUBLE      limite superior (PI = 0.95)
--
-- Executar APÓS o pipeline `btc_transformations` ter sido atualizado ao menos 1×.
-- =============================================================================

CREATE OR REPLACE VIEW z_bitcoin.gold.mtv_btc_forecast
WITH METRICS LANGUAGE YAML
AS $$
version: 1.1
comment: "Previsão de preço BTC 24h com intervalo de confiança 95%. Dimensões temporais multi-grão: 15m / 30m / 1h / 4h."

source: z_bitcoin.gold.f_forecast

filter: y_forecast IS NOT NULL AND y_forecast > 0

dimensions:
# ----- Identificador do ativo ---------------------------------------------
  - name: symbol
    expr: symbol
    display_name: "Ativo"
    comment: "Símbolo do par de negociação (ex: BTCUSDT)"
    synonyms:
      - "moeda"
      - "par"
      - "ticker"
      - "criptomoeda"
      - "coin"

# ----- Dimensões temporais multi-grão (BI escolhe uma por query) ----------
  - name: forecast_ts_15m
    expr: ds
    display_name: "Horário Previsão 15min"
    comment: "Timestamp da previsão no grão de 15 minutos (grão base)"
    synonyms:
      - "data hora previsao"
      - "timestamp forecast"
      - "quando"

  - name: forecast_ts_30m
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ds) / 1800) * 1800) AS TIMESTAMP)
    display_name: "Horário Previsão 30min"
    comment: "Timestamp truncado para janela de 30 minutos"
    synonyms:
      - "meia hora"
      - "30 minutos"

  - name: forecast_ts_1h
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ds) / 3600) * 3600) AS TIMESTAMP)
    display_name: "Horário Previsão 1h"
    comment: "Timestamp truncado para janela de 1 hora"
    synonyms:
      - "hora"
      - "hourly"
      - "1 hora"

  - name: forecast_ts_4h
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ds) / 14400) * 14400) AS TIMESTAMP)
    display_name: "Horário Previsão 4h"
    comment: "Timestamp truncado para janela de 4 horas"
    synonyms:
      - "4 horas"
      - "quatro horas"

# ----- Bucket de horizonte (calculado na geração, não dinâmico) -----------
  - name: horizonte_bucket
    expr: >
      CASE
        WHEN TIMESTAMPDIFF(MINUTE, ds, (SELECT MAX(ds) FROM z_bitcoin.gold.f_forecast)) >= 1380 THEN '0-1h'
        WHEN TIMESTAMPDIFF(MINUTE, ds, (SELECT MAX(ds) FROM z_bitcoin.gold.f_forecast)) >= 1200 THEN '1-4h'
        WHEN TIMESTAMPDIFF(MINUTE, ds, (SELECT MAX(ds) FROM z_bitcoin.gold.f_forecast)) >= 720  THEN '4-12h'
        ELSE '12-24h'
      END
    display_name: "Faixa de Horizonte"
    comment: "Categorização do horizonte de previsão relativo ao fim do forecast (0-1h mais próximo, 12-24h mais distante)"
    synonyms:
      - "horizonte"
      - "distancia"
      - "prazo"
      - "curto prazo"
      - "longo prazo"

# ----- Data para filtros de calendário ------------------------------------
  - name: data_previsao
    expr: DATE(ds)
    display_name: "Data da Previsão"
    comment: "Data da previsão (sem hora) para filtros de calendário"
    synonyms:
      - "dia"
      - "date"

measures:
# ----- Previsão pontual ---------------------------------------------------
  - name: preco_previsto
    expr: AVG(y_forecast)
    display_name: "Preço Previsto (USD)"
    comment: "Previsão média do preço de fechamento. AVG mantém curva suave em roll-ups."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "forecast"
      - "previsao"
      - "preco estimado"
      - "quanto vai custar"

  - name: preco_previsto_inicio
    expr: MIN_BY(y_forecast, ds)
    display_name: "Preço Previsto Início"
    comment: "Primeiro valor previsto do período (útil para abertura de sparkline)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "primeiro preco"
      - "abertura previsao"

  - name: preco_previsto_fim
    expr: MAX_BY(y_forecast, ds)
    display_name: "Preço Previsto Fim"
    comment: "Último valor previsto do período (útil para fechamento de sparkline)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "ultimo preco"
      - "fechamento previsao"

  - name: preco_previsto_min
    expr: MIN(y_forecast)
    display_name: "Preço Previsto Mínimo"
    comment: "Menor preço previsto no período"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "menor previsao"
      - "piso previsto"

  - name: preco_previsto_max
    expr: MAX(y_forecast)
    display_name: "Preço Previsto Máximo"
    comment: "Maior preço previsto no período"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "maior previsao"
      - "teto previsto"

# ----- Intervalo de confiança 95% -----------------------------------------
  - name: limite_inferior_95
    expr: AVG(y_lower)
    display_name: "Limite Inferior 95%"
    comment: "Limite inferior do intervalo de predição com 95% de confiança"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "piso"
      - "minimo esperado"
      - "lower bound"

  - name: limite_superior_95
    expr: AVG(y_upper)
    display_name: "Limite Superior 95%"
    comment: "Limite superior do intervalo de predição com 95% de confiança"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "teto"
      - "maximo esperado"
      - "upper bound"

# ----- Incerteza ----------------------------------------------------------
  - name: largura_intervalo_usd
    expr: AVG(y_upper - y_lower)
    display_name: "Largura Intervalo (USD)"
    comment: "Largura do intervalo de predição em USD. Maior = modelo menos confiante."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "incerteza"
      - "amplitude"
      - "spread"
      - "confianca"

  - name: largura_intervalo_pct
    expr: AVG((y_upper - y_lower) / NULLIF(y_forecast, 0)) * 100
    display_name: "Largura Intervalo (%)"
    comment: "Largura do intervalo como % do preço previsto. Comparável entre regimes de preço."
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "incerteza percentual"
      - "erro relativo"

  - name: score_confianca
    expr: 100 - (AVG((y_upper - y_lower) / NULLIF(y_forecast, 0)) * 100)
    display_name: "Score de Confiança"
    comment: "Métrica invertida de incerteza (100 - largura_intervalo_pct). Quanto maior, mais confiante o modelo."
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
    synonyms:
      - "confiabilidade"
      - "certeza"
      - "precisao modelo"

# ----- Direção prevista ---------------------------------------------------
  - name: variacao_prevista_pct
    expr: (MAX_BY(y_forecast, ds) - MIN_BY(y_forecast, ds)) / NULLIF(MIN_BY(y_forecast, ds), 0) * 100
    display_name: "Variação Prevista (%)"
    comment: "Variação percentual entre primeiro e último preço previsto do período"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "direcao"
      - "tendencia"
      - "vai subir"
      - "vai cair"

  - name: variacao_prevista_usd
    expr: MAX_BY(y_forecast, ds) - MIN_BY(y_forecast, ds)
    display_name: "Variação Prevista (USD)"
    comment: "Variação absoluta em USD entre primeiro e último preço previsto"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "delta preco"
      - "diferenca absoluta"

  - name: sinal_tendencia
    expr: CASE WHEN (MAX_BY(y_forecast, ds) - MIN_BY(y_forecast, ds)) > 0 THEN 'Alta' WHEN (MAX_BY(y_forecast, ds) - MIN_BY(y_forecast, ds)) < 0 THEN 'Baixa' ELSE 'Neutro' END
    display_name: "Sinal de Tendência"
    comment: "Direção esperada da previsão: Alta, Baixa ou Neutro"
    synonyms:
      - "direcao mercado"
      - "bullish ou bearish"

  - name: volatilidade_prevista
    expr: STDDEV(y_forecast)
    display_name: "Volatilidade Prevista"
    comment: "Desvio padrão dos preços previstos no período (proxy de volatilidade esperada)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "desvio"
      - "dispersao"
      - "instabilidade"

# ----- Potencial de ganho/perda -------------------------------------------
  - name: potencial_upside_pct
    expr: (AVG(y_upper) - AVG(y_forecast)) / NULLIF(AVG(y_forecast), 0) * 100
    display_name: "Potencial Upside (%)"
    comment: "Ganho potencial até limite superior (cenário otimista)"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "ganho maximo"
      - "cenario otimista"

  - name: potencial_downside_pct
    expr: (AVG(y_forecast) - AVG(y_lower)) / NULLIF(AVG(y_forecast), 0) * 100
    display_name: "Potencial Downside (%)"
    comment: "Perda potencial até limite inferior (cenário pessimista)"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "perda maxima"
      - "cenario pessimista"
      - "risco"

  - name: assimetria_risco
    expr: (AVG(y_upper) - AVG(y_forecast)) / NULLIF(AVG(y_forecast) - AVG(y_lower), 1)
    display_name: "Assimetria de Risco"
    comment: "Razão entre upside e downside. > 1 = mais upside, < 1 = mais downside"
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "risco retorno"
      - "simetria"

# ----- Cobertura e qualidade ----------------------------------------------
  - name: qtd_pontos_previsao
    expr: COUNT(*)
    display_name: "Qtd Pontos"
    comment: "Quantidade de pontos de previsão no período (útil para QA)"
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - "quantidade"
      - "pontos"
      - "registros"

  - name: horizonte_minutos
    expr: (UNIX_TIMESTAMP(MAX(ds)) - UNIX_TIMESTAMP(MIN(ds))) / 60
    display_name: "Horizonte (min)"
    comment: "Extensão temporal do forecast em minutos"
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
    synonyms:
      - "duracao"
      - "cobertura"
      - "extensao"

  - name: horizonte_horas
    expr: (UNIX_TIMESTAMP(MAX(ds)) - UNIX_TIMESTAMP(MIN(ds))) / 3600
    display_name: "Horizonte (horas)"
    comment: "Extensão temporal do forecast em horas"
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
    synonyms:
      - "duracao horas"
      - "cobertura temporal"
$$;
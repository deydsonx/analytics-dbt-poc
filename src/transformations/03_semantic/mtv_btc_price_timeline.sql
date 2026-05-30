-- =============================================================================
-- METRIC VIEW: z_bitcoin.gold.mtv_btc_price_timeline
--
-- Camada semântica unificada: Histórico + Forecast em timeline contínua.
-- Ideal para gráfico de linha com preço real vs previsão.
--
-- Fonte combinada via UNION ALL:
--   - f_trade_analytics (histórico real)
--   - f_forecast (previsão 24h)
--
-- Grãos: 15m / 30m / 1h / 4h
-- =============================================================================

CREATE OR REPLACE VIEW z_bitcoin.gold.mtv_btc_price_timeline
WITH METRICS LANGUAGE YAML
AS $$
version: 1.1
comment: "Timeline unificada de preço BTC: histórico real + previsão 24h. Use 'Tipo Dado' para distinguir séries no gráfico."

source: >
  SELECT
    window_start AS ts,
    symbol,
    close_price AS preco,
    CAST(NULL AS DOUBLE) AS preco_lower,
    CAST(NULL AS DOUBLE) AS preco_upper,
    'Histórico' AS tipo_dado
  FROM z_bitcoin.gold.f_trade_analytics
  WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
  UNION ALL
  SELECT
    ds AS ts,
    symbol,
    y_forecast AS preco,
    y_lower AS preco_lower,
    y_upper AS preco_upper,
    'Previsão' AS tipo_dado
  FROM z_bitcoin.gold.f_forecast
  WHERE y_forecast IS NOT NULL

dimensions:
  # ----- Identificadores -----------------------------------------------------
  - name: symbol
    expr: symbol
    display_name: "Ativo"
    comment: "Símbolo do par (ex: BTCUSDT)"
    synonyms:
      - "moeda"
      - "ticker"

  - name: tipo_dado
    expr: tipo_dado
    display_name: "Tipo Dado"
    comment: "Distingue dados reais (Histórico) de previsões (Previsão)"
    synonyms:
      - "serie"
      - "origem"
      - "real vs previsao"
      - "historico ou forecast"

  # ----- Dimensões temporais multi-grão --------------------------------------
  - name: ts_15m
    expr: ts
    display_name: "Horário 15min"
    comment: "Timestamp no grão de 15 minutos (base)"
    synonyms:
      - "data hora"
      - "quando"

  - name: ts_30m
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 1800) * 1800) AS TIMESTAMP)
    display_name: "Horário 30min"
    comment: "Timestamp truncado para 30 minutos"

  - name: ts_1h
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 3600) * 3600) AS TIMESTAMP)
    display_name: "Horário 1h"
    comment: "Timestamp truncado para 1 hora"

  - name: ts_4h
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ts) / 14400) * 14400) AS TIMESTAMP)
    display_name: "Horário 4h"
    comment: "Timestamp truncado para 4 horas"

  - name: data
    expr: DATE(ts)
    display_name: "Data"
    comment: "Data para filtros de calendário"
    synonyms:
      - "dia"

measures:
  # ----- Preço principal -----------------------------------------------------
  - name: preco
    expr: AVG(preco)
    display_name: "Preço (USD)"
    comment: "Preço médio do período (histórico = close, previsão = forecast)"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "valor"
      - "cotacao"
      - "preco btc"

  - name: preco_inicio
    expr: MIN_BY(preco, ts)
    display_name: "Preço Início"
    comment: "Primeiro preço do período"
    format:
      type: currency
      currency: USD
      decimals: 2

  - name: preco_fim
    expr: MAX_BY(preco, ts)
    display_name: "Preço Fim"
    comment: "Último preço do período"
    format:
      type: currency
      currency: USD
      decimals: 2

  # ----- Intervalo de confiança (só para forecast) ---------------------------
  - name: limite_inferior
    expr: AVG(preco_lower)
    display_name: "Limite Inferior 95%"
    comment: "Limite inferior do intervalo de confiança (NULL para histórico)"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "piso"
      - "lower bound"

  - name: limite_superior
    expr: AVG(preco_upper)
    display_name: "Limite Superior 95%"
    comment: "Limite superior do intervalo de confiança (NULL para histórico)"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "teto"
      - "upper bound"

  # ----- Métricas auxiliares -------------------------------------------------
  - name: qtd_pontos
    expr: COUNT(*)
    display_name: "Qtd Pontos"
    comment: "Quantidade de pontos de dados no período"
    format:
      type: number
      decimals: 0
$$;

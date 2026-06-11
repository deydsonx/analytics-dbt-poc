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
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
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
      currency_code: USD
      decimal_places:
        type: exact
        places: 2

  - name: preco_fim
    expr: MAX_BY(preco, ts)
    display_name: "Preço Fim"
    comment: "Último preço do período"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2

  - name: preco_min
    expr: MIN(preco)
    display_name: "Preço Mínimo"
    comment: "Menor preço observado no período"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "minimo"
      - "piso"

  - name: preco_max
    expr: MAX(preco)
    display_name: "Preço Máximo"
    comment: "Maior preço observado no período"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "maximo"
      - "teto"

  # ----- Variação e movimento -------------------------------------------------
  - name: variacao_periodo_pct
    expr: (MAX_BY(preco, ts) - MIN_BY(preco, ts)) / NULLIF(MIN_BY(preco, ts), 0) * 100
    display_name: "Variação Período (%)"
    comment: "Variação percentual entre primeiro e último preço"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "retorno"
      - "performance"
      - "mudanca"

  - name: variacao_periodo_usd
    expr: MAX_BY(preco, ts) - MIN_BY(preco, ts)
    display_name: "Variação Período (USD)"
    comment: "Variação absoluta em USD entre primeiro e último preço"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "delta"
      - "diferenca"

  - name: amplitude_pct
    expr: (MAX(preco) - MIN(preco)) / NULLIF(MIN(preco), 0) * 100
    display_name: "Amplitude (%)"
    comment: "Range total do período como percentual do mínimo"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "range"
      - "volatilidade"

  # ----- Intervalo de confiança (só para forecast) ---------------------------
  - name: limite_inferior
    expr: AVG(preco_lower)
    display_name: "Limite Inferior 95%"
    comment: "Limite inferior do intervalo de confiança (NULL para histórico)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "piso"
      - "lower bound"

  - name: limite_superior
    expr: AVG(preco_upper)
    display_name: "Limite Superior 95%"
    comment: "Limite superior do intervalo de confiança (NULL para histórico)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "teto"
      - "upper bound"

  - name: largura_banda
    expr: AVG(preco_upper - preco_lower)
    display_name: "Largura da Banda"
    comment: "Largura do intervalo de confiança em USD (só para previsão)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "incerteza"
      - "spread"

  - name: largura_banda_pct
    expr: AVG((preco_upper - preco_lower) / NULLIF(preco, 0)) * 100
    display_name: "Largura da Banda (%)"
    comment: "Largura do intervalo como % do preço (só para previsão)"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "incerteza relativa"

  # ----- Métricas estatísticas -----------------------------------------------
  - name: desvio_padrao
    expr: STDDEV(preco)
    display_name: "Desvio Padrão"
    comment: "Desvio padrão dos preços no período"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "volatilidade"
      - "dispersao"

  - name: coef_variacao
    expr: STDDEV(preco) / NULLIF(AVG(preco), 0) * 100
    display_name: "Coeficiente de Variação"
    comment: "Volatilidade relativa (desvio / média)"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "cv"
      - "volatilidade relativa"

  # ----- Tendência e direção -----------------------------------------------
  - name: direcao
    expr: CASE WHEN MAX_BY(preco, ts) > MIN_BY(preco, ts) THEN 'Alta' WHEN MAX_BY(preco, ts) < MIN_BY(preco, ts) THEN 'Baixa' ELSE 'Neutro' END
    display_name: "Direção"
    comment: "Tendência do período: Alta, Baixa ou Neutro"
    synonyms:
      - "tendencia"
      - "movimento"
      - "bullish bearish"

  - name: momentum
    expr: (MAX_BY(preco, ts) - MIN_BY(preco, ts)) / NULLIF((UNIX_TIMESTAMP(MAX(ts)) - UNIX_TIMESTAMP(MIN(ts))) / 3600, 0)
    display_name: "Momentum (USD/hora)"
    comment: "Taxa de variação do preço por hora"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - "velocidade"
      - "taxa variacao"

  # ----- Métricas auxiliares -------------------------------------------------
  - name: qtd_pontos
    expr: COUNT(*)
    display_name: "Qtd Pontos"
    comment: "Quantidade de pontos de dados no período"
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

  - name: duracao_horas
    expr: (UNIX_TIMESTAMP(MAX(ts)) - UNIX_TIMESTAMP(MIN(ts))) / 3600
    display_name: "Duração (horas)"
    comment: "Extensão temporal dos dados em horas"
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
    synonyms:
      - "periodo"
      - "janela temporal"

  # ----- Comparação Histórico vs Forecast ----------------------------------
  - name: tem_intervalo_confianca
    expr: CASE WHEN AVG(preco_upper) IS NOT NULL THEN 'Sim' ELSE 'Não' END
    display_name: "Tem IC 95%"
    comment: "Indica se o registro tem intervalo de confiança (Previsão = Sim, Histórico = Não)"
    synonyms:
      - "possui banda"
      - "forecast flag"
$$;
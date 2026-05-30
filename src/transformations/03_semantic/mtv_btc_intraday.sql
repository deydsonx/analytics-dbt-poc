-- =============================================================================
-- METRIC VIEW: z_bitcoin.gold.mtv_btc_intraday
--
-- Camada semântica para BI / AI-BI / Genie sobre analytics de trades BTC.
-- Fonte : z_bitcoin.gold.f_trade_analytics (base de 15 minutos)
-- Grãos : 15m / 30m / 1h / 4h (múltiplos de 15 — menores não deriváveis)
--
-- Executar APÓS o pipeline `gold_transformations` ter sido atualizado ao menos 1×.
-- Sintaxe verificada contra docs Databricks (Metric Views, Mai 2026): YAML v1.1
-- =============================================================================

CREATE OR REPLACE VIEW z_bitcoin.gold.mtv_btc_intraday
WITH METRICS LANGUAGE YAML
AS $$
version: 1.1
comment: "KPIs de trade BTC intraday baseado em f_trade_analytics. Dimensões temporais multi-grão: 15m / 30m / 1h / 4h."

source: z_bitcoin.gold.f_trade_analytics

filter: volume > 0 AND trade_count > 0

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
  - name: ts_15m
    expr: window_start
    display_name: "Horário 15min"
    comment: "Início da janela de 15 minutos (grão base)"
    synonyms:
      - "data hora"
      - "timestamp"
      - "quando"
      - "horario"

  - name: ts_30m
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(window_start) / 1800) * 1800) AS TIMESTAMP)
    display_name: "Horário 30min"
    comment: "Timestamp truncado para janela de 30 minutos"
    synonyms:
      - "meia hora"
      - "30 minutos"

  - name: ts_1h
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(window_start) / 3600) * 3600) AS TIMESTAMP)
    display_name: "Horário 1h"
    comment: "Timestamp truncado para janela de 1 hora"
    synonyms:
      - "hora"
      - "hourly"
      - "1 hora"

  - name: ts_4h
    expr: CAST(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(window_start) / 14400) * 14400) AS TIMESTAMP)
    display_name: "Horário 4h"
    comment: "Timestamp truncado para janela de 4 horas (padrão em análise técnica crypto)"
    synonyms:
      - "4 horas"
      - "quatro horas"

# ----- Data para filtros de calendário ------------------------------------
  - name: data
    expr: DATE(window_start)
    display_name: "Data"
    comment: "Data do trade (sem hora) para filtros de calendário"
    synonyms:
      - "dia"
      - "date"

# ----- Sessão de mercado --------------------------------------------------
  - name: sessao_mercado
    expr: >
      CASE
        WHEN HOUR(window_start) BETWEEN 0 AND 7   THEN 'Asia'
        WHEN HOUR(window_start) BETWEEN 8 AND 15  THEN 'Europa'
        WHEN HOUR(window_start) BETWEEN 16 AND 23 THEN 'Americas'
      END
    display_name: "Sessão de Mercado"
    comment: "Sessão geográfica baseada no horário UTC (Asia/Europa/Americas)"
    synonyms:
      - "regiao"
      - "timezone"
      - "mercado"

measures:
# ----- OHLC (MIN_BY / MAX_BY mantêm roll-ups determinísticos) -------------
  - name: preco_abertura
    expr: MIN_BY(open_price, window_start)
    display_name: "Preço Abertura"
    comment: "Preço de abertura do primeiro candle do período"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "open"
      - "abertura"
      - "primeiro preco"

  - name: preco_maximo
    expr: MAX(high_price)
    display_name: "Preço Máximo"
    comment: "Maior preço atingido no período"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "high"
      - "topo"
      - "maximo"
      - "maior preco"

  - name: preco_minimo
    expr: MIN(low_price)
    display_name: "Preço Mínimo"
    comment: "Menor preço atingido no período"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "low"
      - "fundo"
      - "minimo"
      - "menor preco"

  - name: preco_fechamento
    expr: MAX_BY(close_price, window_start)
    display_name: "Preço Fechamento"
    comment: "Preço de fechamento do último candle do período"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "close"
      - "fechamento"
      - "ultimo preco"
      - "preco atual"

# ----- Volume / VWAP (componentes aditivos mantêm VWAP correto) -----------
  - name: vwap
    expr: SUM(notional_usd) / NULLIF(SUM(volume), 0)
    display_name: "VWAP"
    comment: "Preço médio ponderado por volume (Volume Weighted Average Price)"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "preco medio"
      - "preco ponderado"
      - "volume weighted"

  - name: volume_btc
    expr: SUM(volume)
    display_name: "Volume (BTC)"
    comment: "Volume total negociado em BTC"
    format:
      type: number
      decimals: 4
    synonyms:
      - "volume"
      - "quantidade"
      - "btc negociado"

  - name: volume_usd
    expr: SUM(notional_usd)
    display_name: "Volume (USD)"
    comment: "Volume financeiro total em dólares"
    format:
      type: currency
      currency: USD
      decimals: 0
    synonyms:
      - "notional"
      - "valor negociado"
      - "dolares"
      - "movimentacao"

# ----- Atividade e fluxo --------------------------------------------------
  - name: qtd_trades
    expr: SUM(trade_count)
    display_name: "Qtd Trades"
    comment: "Número total de negociações executadas"
    format:
      type: number
      decimals: 0
    synonyms:
      - "trades"
      - "negociacoes"
      - "operacoes"
      - "quantidade"

  - name: volume_compra
    expr: SUM(buy_volume)
    display_name: "Volume Compra (BTC)"
    comment: "Volume de ordens de compra (taker buy)"
    format:
      type: number
      decimals: 4
    synonyms:
      - "buy volume"
      - "compras"
      - "demanda"

  - name: volume_venda
    expr: SUM(sell_volume)
    display_name: "Volume Venda (BTC)"
    comment: "Volume de ordens de venda (taker sell)"
    format:
      type: number
      decimals: 4
    synonyms:
      - "sell volume"
      - "vendas"
      - "oferta"

  - name: desequilibrio_taker
    expr: (SUM(buy_volume) - SUM(sell_volume)) / NULLIF(SUM(volume), 0)
    display_name: "Desequilíbrio Taker"
    comment: "Razão entre compras e vendas. Positivo = pressão compradora, Negativo = pressão vendedora."
    format:
      type: percent
      decimals: 2
    synonyms:
      - "imbalance"
      - "fluxo"
      - "pressao"
      - "direcao"
      - "bulls vs bears"

  - name: qtd_block_trades
    expr: SUM(block_trade_count)
    display_name: "Qtd Block Trades"
    comment: "Número de operações de grande porte (sinaliza atividade institucional)"
    format:
      type: number
      decimals: 0
    synonyms:
      - "block trades"
      - "institucional"
      - "whale"
      - "baleia"

# ----- Volatilidade e range -----------------------------------------------
  - name: range_usd
    expr: MAX(high_price) - MIN(low_price)
    display_name: "Range (USD)"
    comment: "Amplitude de preço no período (máximo - mínimo)"
    format:
      type: currency
      currency: USD
      decimals: 2
    synonyms:
      - "amplitude"
      - "variacao"
      - "oscilacao"

  - name: range_pct
    expr: (MAX(high_price) - MIN(low_price)) / NULLIF(MIN(low_price), 0) * 100
    display_name: "Range (%)"
    comment: "Amplitude de preço como percentual do mínimo"
    format:
      type: percent
      decimals: 2
    synonyms:
      - "volatilidade"
      - "oscilacao percentual"

  - name: retorno_periodo
    expr: (MAX_BY(close_price, window_start) - MIN_BY(open_price, window_start)) / NULLIF(MIN_BY(open_price, window_start), 0) * 100
    display_name: "Retorno Período (%)"
    comment: "Retorno percentual do período (fechamento vs abertura)"
    format:
      type: percent
      decimals: 2
    synonyms:
      - "retorno"
      - "ganho"
      - "perda"
      - "performance"
      - "variacao preco"

# ----- Métricas de intensidade --------------------------------------------
  - name: volume_por_trade
    expr: SUM(volume) / NULLIF(SUM(trade_count), 0)
    display_name: "Volume Médio por Trade"
    comment: "Tamanho médio das operações em BTC"
    format:
      type: number
      decimals: 6
    synonyms:
      - "tamanho medio"
      - "ticket medio"
      - "avg trade size"

  - name: trades_por_minuto
    expr: SUM(trade_count) / 15.0
    display_name: "Trades por Minuto"
    comment: "Frequência média de negociações (baseado em janela de 15min)"
    format:
      type: number
      decimals: 1
    synonyms:
      - "frequencia"
      - "ritmo"
      - "atividade"
$$;

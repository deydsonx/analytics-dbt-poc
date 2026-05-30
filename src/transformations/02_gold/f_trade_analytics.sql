CREATE MATERIALIZED VIEW ${target_catalog}.gold.f_trade_analytics
COMMENT '1-minute OHLCV, VWAP, taker imbalance, block-trade counts.'
CLUSTER BY (window_start)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
AS
WITH windowed AS (
    SELECT
        window(trade_ts, '1 minutes') AS w,
        symbol,
        price,
        quantity,
        notional_usd,
        trade_time_ms,
        side,
        is_block_trade
    FROM ${target_catalog}.silver.trades_btc
)
SELECT
    w.start                                       AS window_start,
    w.end                                         AS window_end,
    symbol,

    -- OHLC (deterministic via MIN_BY / MAX_BY on trade_time_ms)
    MIN_BY(price, trade_time_ms)                  AS open_price,
    MAX(price)                                    AS high_price,
    MIN(price)                                    AS low_price,
    MAX_BY(price, trade_time_ms)                  AS close_price,

    -- Volume / VWAP
    SUM(notional_usd) / NULLIF(SUM(quantity), 0)  AS vwap,
    SUM(quantity)                                 AS volume,
    SUM(notional_usd)                             AS notional_usd,

    -- Activity & flow
    COUNT(*)                                                                  AS trade_count,
    SUM(CASE WHEN side = 'BUY'  THEN quantity ELSE 0 END)                     AS buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END)                     AS sell_volume,
    ( SUM(CASE WHEN side = 'BUY'  THEN quantity ELSE 0 END)
    - SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END))
      / NULLIF(SUM(quantity), 0)                                              AS taker_imbalance,
    SUM(CASE WHEN is_block_trade THEN 1 ELSE 0 END)                           AS block_trade_count
FROM windowed
GROUP BY ALL
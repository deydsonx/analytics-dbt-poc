-- Silver: trades BTC limpos, deduplicados e validados (Bybit V5 spot).
-- Fonte: stg_trades_typed (view temporária de stg_trades_btc.sql).
-- Dedupe: por trade_id em janela de 5 min com WATERMARK explícito (bounded state).
-- Refs: https://docs.databricks.com/aws/en/ldp/best-practices?language=SQL
CREATE OR REFRESH STREAMING TABLE ${target_catalog}.silver.trades_btc (
  CONSTRAINT valid_price                 EXPECT (price > 0)                       ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity              EXPECT (quantity > 0)                    ON VIOLATION DROP ROW,
  CONSTRAINT valid_side                  EXPECT (side IN ('BUY','SELL'))          ON VIOLATION DROP ROW,
  CONSTRAINT not_null_trade_id           EXPECT (trade_id IS NOT NULL)            ON VIOLATION DROP ROW,
  CONSTRAINT event_before_or_eq_ingested EXPECT (trade_time_ms <= ingested_at_ms)
)
CLUSTER BY (trade_ts)  -- Adicionar 'symbol' quando ingerir 2+ pares
COMMENT 'Trades BTC limpos, deduplicados e validados (Bybit V5 spot). Colunas BI-friendly.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  
  -- Data skipping optimization (queries temporais/symbol-filtered)
  'delta.dataSkippingNumIndexedCols'       = '3',
  'delta.dataSkippingStatsColumns'         = 'trade_ts,symbol,side',
  
  -- Retention optimization (Silver é intermediária)
  'delta.deletedFileRetentionDuration'     = 'interval 3 days',
  'delta.logRetentionDuration'             = 'interval 3 days',
  
  -- Streaming state optimization (deduplicação com WATERMARK)
  'spark.sql.streaming.stateStore.rocksdb.compactOnCommit'            = 'true',
  'spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled' = 'true',
  
  -- Checkpoint tuning (streaming com microbatches frequentes)
  'delta.checkpointInterval'               = '50',
  
  -- Liquid clustering tuning
  'delta.tuneFileSizesForRewrites'         = 'true'
)
AS
SELECT
    trade_id,
    MAX_BY(symbol, trade_time_ms)           AS symbol,
    MAX_BY(price, trade_time_ms)            AS price,
    MAX_BY(quantity, trade_time_ms)         AS quantity,
    MAX_BY(notional_usd, trade_time_ms)     AS notional_usd,
    MAX_BY(side, trade_time_ms)             AS side,
    MAX(trade_time_ms)                      AS trade_time_ms,
    MAX_BY(trade_ts, trade_time_ms)         AS trade_ts,
    MAX_BY(ingested_at_ms, trade_time_ms)   AS ingested_at_ms,
    MAX_BY(ingested_ts, trade_time_ms)      AS ingested_ts,
    MAX_BY(cross_sequence, trade_time_ms)   AS cross_sequence,
    MAX_BY(is_block_trade, trade_time_ms)   AS is_block_trade,
    CURRENT_TIMESTAMP()                     AS silver_processed_at
FROM 
    STREAM(stg_trades_typed)
WATERMARK 
    trade_ts DELAY OF INTERVAL 5 MINUTES
GROUP BY 
    ALL
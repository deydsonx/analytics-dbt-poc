-- Step 1 ─ normaliza tipos sem materializar (sem custo de storage).
-- Nomes de colunas BI-friendly: trade_ts, ingested_ts, notional_usd, side.
CREATE TEMPORARY VIEW stg_trades_typed AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    (price * quantity)              AS notional_usd,
    UPPER(taker_side)               AS side,
    trade_time                      AS trade_time_ms,
    timestamp_millis(trade_time)    AS trade_ts,
    ingested_at                     AS ingested_at_ms,
    timestamp_millis(ingested_at)   AS ingested_ts,
    cross_sequence,
    is_block_trade
FROM 
    STREAM(${bronze_table});
CREATE VIEW ${target_catalog}.gold.vw_trades_realtime
COMMENT 'Gold projection of silver.trades_btc with lag_ms telemetry.'
AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    is_block_trade,
    cross_sequence,
    trade_time_ms,
    ingested_at_ms,
    trade_ts,
    ingested_ts,
    notional_usd,
    ingested_at_ms - trade_time_ms AS lag_ms
FROM 
    ${target_catalog}.silver.trades_btc
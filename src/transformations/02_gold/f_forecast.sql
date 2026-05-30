CREATE MATERIALIZED VIEW ${target_catalog}.gold.f_forecast
COMMENT '24h close-price forecast via ai_forecast(). 14-day training window.'
CLUSTER BY (ds)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'       = 'true',
    'delta.autoOptimize.autoCompact'         = 'true',    
    -- Data skipping optimization (queries com filtros)
    'delta.dataSkippingNumIndexedCols'       = '2',
    'delta.dataSkippingStatsColumns'         = 'ds',
    -- Retention optimization (forecast data é temporal)
    'delta.deletedFileRetentionDuration'     = 'interval 7 days',
    'delta.logRetentionDuration'             = 'interval 7 days',    
    -- Checkpoint tuning (refresh 1min)
    'delta.checkpointInterval'               = '20',
    -- Liquid clustering tuning
    'delta.tuneFileSizesForRewrites'         = 'true'
)
AS
SELECT *
FROM AI_FORECAST(
    TABLE(
        SELECT
            window_start  AS ds,
            close_price   AS y,
            symbol
        FROM ${target_catalog}.gold.f_trade_analytics
        WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 14 DAYS
    ),
    horizon                    => CURRENT_TIMESTAMP() + INTERVAL 2 HOURS,
    time_col                   => 'ds',
    value_col                  => 'y',
    group_col                  => array('symbol'),
    prediction_interval_width  => 0.95
)
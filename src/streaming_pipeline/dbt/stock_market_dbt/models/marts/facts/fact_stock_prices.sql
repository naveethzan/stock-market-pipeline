{{
    config(
        materialized='incremental',
        unique_key='stock_price_key',
        dist='symbol',
        sort=['date_key', 'time_key']
    )
}}

WITH stock_source AS (
    SELECT *
    FROM {{ ref('stg_processed_stock') }}
    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

indicators_source AS (
    SELECT *
    FROM {{ ref('stg_technical_indicators') }}
    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

stock_data AS (
    SELECT 
        s.*,
        t.rsi,
        t.macd,
        t.sma_20,
        t.sma_50,
        t.vwap
    FROM stock_source s
    LEFT JOIN indicators_source t
        ON s.symbol = t.symbol 
        AND s.timestamp = t.timestamp
)

SELECT
    -- Keys
    MD5(symbol || '::' || timestamp::VARCHAR) AS stock_price_key,
    symbol AS company_symbol,
    TO_CHAR(DATE(timestamp), 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(HOUR FROM timestamp) * 60 + EXTRACT(MINUTE FROM timestamp) AS time_key,
    
    -- Degenerate dimensions
    symbol,
    timestamp,
    
    -- Measures (all pre-calculated by Spark)
    price AS current_price,
    open_price,
    high_price,
    low_price,
    close_price,
    previous_close,
    change_amount,
    change_percentage,
    volume,
    market_cap,
    pe_ratio,
    
    -- Technical indicators (from joined data)
    rsi,
    macd,
    sma_20,
    sma_50,
    vwap,
    
    -- Simple categorizations
    CASE 
        WHEN change_percentage > 2 THEN 'Strong Up'
        WHEN change_percentage > 0 THEN 'Up'
        WHEN change_percentage < -2 THEN 'Strong Down'
        WHEN change_percentage < 0 THEN 'Down'
        ELSE 'Flat'
    END AS price_trend,
    
    CASE
        WHEN rsi > 70 THEN 'Overbought'
        WHEN rsi < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal,
    
    -- Audit
    CURRENT_TIMESTAMP AS created_at
FROM stock_data

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

company_dim AS (
    SELECT 
        company_key,
        symbol
    FROM {{ ref('dim_company') }}
    WHERE is_current = true
),

stock_data AS (
    SELECT 
        s.*,
        -- Technical indicators from Phase 3 schema (LEFT JOIN)
        t.rsi_14,
        t.macd,
        t.macd_signal,
        t.macd_histogram,
        t.bollinger_upper,
        t.bollinger_middle,
        t.bollinger_lower,
        t.data_quality_score as tech_data_quality_score,
        -- Company key
        c.company_key
    FROM stock_source s
    LEFT JOIN indicators_source t
        ON s.symbol = t.symbol 
        AND s.timestamp = t.timestamp
    LEFT JOIN company_dim c
        ON s.symbol = c.symbol
)

SELECT
    -- Keys (matching Phase 3 schema spec)
    MD5(symbol || '::' || timestamp::VARCHAR) AS stock_price_key,
    company_key,
    TO_CHAR(DATE(timestamp), 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(HOUR FROM timestamp) * 60 + EXTRACT(MINUTE FROM timestamp) AS time_key,
    
    -- Stock Price Fields (from processed-stock-prices topic)
    symbol,
    timestamp,
    current_price,
    open_price,
    high_price,
    low_price,
    volume,
    sma_5min,
    sma_20min,
    price_trend_5min,
    price_volatility,
    trading_session,
    vwap,
    price_change_abs,
    price_momentum,
    
    -- Technical Indicators (from processed-technical-indicators topic, LEFT JOIN)
    rsi_14,
    macd,
    macd_signal,
    macd_histogram,
    bollinger_upper,
    bollinger_middle,
    bollinger_lower,
    
    -- Metadata
    data_quality_score,
    tech_data_quality_score,
    
    -- Audit
    CURRENT_TIMESTAMP AS created_at
FROM stock_data

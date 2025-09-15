{{
    config(
        materialized='incremental',
        unique_key='trading_volume_key',
        dist='symbol',
        sort=['date_key', 'time_key']
    )
}}

WITH volume_source AS (
    SELECT *
    FROM {{ ref('stg_trading_volume') }}
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

volume_data AS (
    SELECT 
        v.*,
        c.company_key
    FROM volume_source v
    LEFT JOIN company_dim c
        ON v.symbol = c.symbol
)

SELECT
    -- Keys (matching Phase 3 schema spec)
    MD5(symbol || '::' || timestamp::VARCHAR) AS trading_volume_key,
    company_key,
    TO_CHAR(DATE(timestamp), 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(HOUR FROM timestamp) * 60 + EXTRACT(MINUTE FROM timestamp) AS time_key,
    
    -- Volume Fields (from processed-trading-volume topic)
    symbol,
    timestamp,
    volume,
    volume_ma_5min,
    volume_ma_20min,
    volume_trend,
    volume_anomaly,
    volume_ratio,
    volume_weighted_price,
    volume_category,
    
    -- Audit
    CURRENT_TIMESTAMP AS created_at
FROM volume_data

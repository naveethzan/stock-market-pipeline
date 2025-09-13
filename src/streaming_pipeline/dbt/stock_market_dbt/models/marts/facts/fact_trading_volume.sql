{{
    config(
        materialized='incremental',
        unique_key='trading_volume_key',
        dist='symbol',
        sort=['date_key', 'time_key']
    )
}}

WITH volume_data AS (
    SELECT *
    FROM {{ ref('stg_aggregated_data') }}
    {% if is_incremental() %}
        WHERE dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Keys
    MD5(symbol || '::' || window_start::VARCHAR) AS trading_volume_key,
    symbol AS company_symbol,
    TO_CHAR(DATE(window_start), 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(HOUR FROM window_start) * 60 + EXTRACT(MINUTE FROM window_start) AS time_key,
    
    -- Degenerate dimensions
    symbol,
    window_start,
    window_end,
    
    -- Measures (all pre-calculated by Spark)
    total_volume,
    trade_count,
    avg_price,
    min_price,
    max_price,
    price_volatility,
    vwap,
    
    -- Simple categorizations
    CASE
        WHEN price_volatility > 0.05 THEN 'High'
        WHEN price_volatility > 0.02 THEN 'Medium'
        ELSE 'Low'
    END AS volatility_level,
    
    -- Audit
    CURRENT_TIMESTAMP AS created_at
FROM volume_data

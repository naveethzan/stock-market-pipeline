{{
    config(
        materialized='incremental',
        unique_key='record_id',
        dist='symbol',
        sort=['timestamp']
    )
}}

WITH source_data AS (
    SELECT
        kafka_key,
        kafka_value,
        kafka_timestamp
    FROM {{ source('streaming', 'processed_stock_prices_stream') }}
    {% if is_incremental() %}
        WHERE kafka_timestamp > (SELECT MAX(kafka_timestamp) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        -- Unique identifier
        MD5(kafka_key || '::' || kafka_timestamp::VARCHAR) AS record_id,
        
        -- Core stock data fields matching Phase 3 producer schema
        kafka_value:symbol::VARCHAR AS symbol,
        kafka_value:timestamp::TIMESTAMP AS timestamp,
        kafka_value:current_price::DECIMAL(10,2) AS current_price,
        kafka_value:open_price::DECIMAL(10,2) AS open_price,
        kafka_value:high_price::DECIMAL(10,2) AS high_price,
        kafka_value:low_price::DECIMAL(10,2) AS low_price,
        kafka_value:volume::BIGINT AS volume,
        
        -- Moving averages and technical indicators
        kafka_value:sma_5min::DECIMAL(10,4) AS sma_5min,
        kafka_value:sma_20min::DECIMAL(10,4) AS sma_20min,
        kafka_value:price_trend_5min::VARCHAR AS price_trend_5min,
        kafka_value:price_volatility::DECIMAL(10,4) AS price_volatility,
        kafka_value:vwap::DECIMAL(10,4) AS vwap,
        kafka_value:price_change_abs::DECIMAL(10,4) AS price_change_abs,
        kafka_value:price_momentum::DECIMAL(10,4) AS price_momentum,
        
        -- Trading session and metadata
        kafka_value:trading_session::VARCHAR AS trading_session,
        kafka_value:producer_timestamp::TIMESTAMP AS producer_timestamp,
        kafka_value:processing_timestamp::TIMESTAMP AS processing_timestamp,
        kafka_value:data_quality_score::DECIMAL(5,2) AS data_quality_score,
        
        -- System metadata
        kafka_timestamp,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source_data
)

SELECT * FROM parsed

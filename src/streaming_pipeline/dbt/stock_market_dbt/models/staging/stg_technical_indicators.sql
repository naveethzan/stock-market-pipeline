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
    FROM {{ source('streaming', 'technical_indicators_stream') }}
    {% if is_incremental() %}
        WHERE kafka_timestamp > (SELECT MAX(kafka_timestamp) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        MD5(kafka_key || '::' || kafka_timestamp::VARCHAR) AS record_id,
        
        -- Basic fields
        kafka_value:symbol::VARCHAR AS symbol,
        kafka_value:timestamp::TIMESTAMP AS timestamp,
        
        -- Moving averages (already calculated by Spark)
        kafka_value:sma_20::DECIMAL(10,2) AS sma_20,
        kafka_value:sma_50::DECIMAL(10,2) AS sma_50,
        kafka_value:ema_12::DECIMAL(10,2) AS ema_12,
        kafka_value:ema_26::DECIMAL(10,2) AS ema_26,
        
        -- Technical indicators (already calculated by Spark)
        kafka_value:rsi::DECIMAL(5,2) AS rsi,
        kafka_value:macd::DECIMAL(10,2) AS macd,
        kafka_value:macd_signal::DECIMAL(10,2) AS macd_signal,
        kafka_value:bollinger_upper::DECIMAL(10,2) AS bollinger_upper,
        kafka_value:bollinger_middle::DECIMAL(10,2) AS bollinger_middle,
        kafka_value:bollinger_lower::DECIMAL(10,2) AS bollinger_lower,
        kafka_value:vwap::DECIMAL(10,2) AS vwap,
        kafka_value:atr::DECIMAL(10,2) AS atr,
        
        kafka_timestamp,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source_data
)

SELECT * FROM parsed

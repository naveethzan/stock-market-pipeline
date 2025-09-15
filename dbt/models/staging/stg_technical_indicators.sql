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
    FROM {{ source('streaming', 'processed_technical_indicators_stream') }}
    {% if is_incremental() %}
        WHERE kafka_timestamp > (SELECT MAX(kafka_timestamp) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        -- Unique identifier
        MD5(kafka_key || '::' || kafka_timestamp::VARCHAR) AS record_id,
        
        -- Basic fields matching Phase 3 producer schema
        kafka_value:symbol::VARCHAR AS symbol,
        kafka_value:timestamp::TIMESTAMP AS timestamp,
        
        -- Technical indicators from Phase 3 schema
        kafka_value:rsi_14::DECIMAL(5,2) AS rsi_14,
        kafka_value:macd::DECIMAL(10,4) AS macd,
        kafka_value:macd_signal::DECIMAL(10,4) AS macd_signal,
        kafka_value:macd_histogram::DECIMAL(10,4) AS macd_histogram,
        kafka_value:bollinger_upper::DECIMAL(10,4) AS bollinger_upper,
        kafka_value:bollinger_lower::DECIMAL(10,4) AS bollinger_lower,
        kafka_value:bollinger_middle::DECIMAL(10,4) AS bollinger_middle,
        
        -- Metadata fields
        kafka_value:producer_timestamp::TIMESTAMP AS producer_timestamp,
        kafka_value:processing_timestamp::TIMESTAMP AS processing_timestamp,
        kafka_value:data_quality_score::DECIMAL(5,2) AS data_quality_score,
        
        -- System metadata
        kafka_timestamp,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source_data
)

SELECT * FROM parsed

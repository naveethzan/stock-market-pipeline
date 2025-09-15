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
    FROM {{ source('streaming', 'processed_trading_volume_stream') }}
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
        kafka_value:volume::BIGINT AS volume,
        
        -- Volume moving averages and metrics
        kafka_value:volume_ma_5min::DECIMAL(15,2) AS volume_ma_5min,
        kafka_value:volume_ma_20min::DECIMAL(15,2) AS volume_ma_20min,
        kafka_value:volume_trend::VARCHAR AS volume_trend,
        kafka_value:volume_anomaly::BOOLEAN AS volume_anomaly,
        kafka_value:volume_ratio::DECIMAL(10,4) AS volume_ratio,
        kafka_value:volume_weighted_price::DECIMAL(10,4) AS volume_weighted_price,
        kafka_value:volume_category::VARCHAR AS volume_category,
        
        -- Metadata fields
        kafka_value:producer_timestamp::TIMESTAMP AS producer_timestamp,
        kafka_value:processing_timestamp::TIMESTAMP AS processing_timestamp,
        
        -- System metadata
        kafka_timestamp,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source_data
)

SELECT * FROM parsed

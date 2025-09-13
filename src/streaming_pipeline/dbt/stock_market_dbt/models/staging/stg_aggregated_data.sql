{{
    config(
        materialized='incremental',
        unique_key='record_id',
        dist='symbol',
        sort=['window_start']
    )
}}

WITH source_data AS (
    SELECT
        kafka_key,
        kafka_value,
        kafka_timestamp
    FROM {{ source('streaming', 'aggregated_data_stream') }}
    {% if is_incremental() %}
        WHERE kafka_timestamp > (SELECT MAX(kafka_timestamp) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        MD5(kafka_key || '::' || kafka_timestamp::VARCHAR) AS record_id,
        
        -- Window fields
        kafka_value:symbol::VARCHAR AS symbol,
        kafka_value:window_start::TIMESTAMP AS window_start,
        kafka_value:window_end::TIMESTAMP AS window_end,
        
        -- Aggregated metrics (already calculated by Spark)
        kafka_value:avg_price::DECIMAL(10,2) AS avg_price,
        kafka_value:min_price::DECIMAL(10,2) AS min_price,
        kafka_value:max_price::DECIMAL(10,2) AS max_price,
        kafka_value:total_volume::BIGINT AS total_volume,
        kafka_value:trade_count::INTEGER AS trade_count,
        kafka_value:price_volatility::DECIMAL(10,4) AS price_volatility,
        kafka_value:vwap::DECIMAL(10,2) AS vwap,
        
        kafka_timestamp,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source_data
)

SELECT * FROM parsed

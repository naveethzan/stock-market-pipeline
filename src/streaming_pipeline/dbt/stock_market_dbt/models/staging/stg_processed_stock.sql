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
    FROM {{ source('streaming', 'processed_stock_stream') }}
    {% if is_incremental() %}
        WHERE kafka_timestamp > (SELECT MAX(kafka_timestamp) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        -- Unique identifier
        MD5(kafka_key || '::' || kafka_timestamp::VARCHAR) AS record_id,
        
        -- Parse JSON fields (no calculations, just extraction)
        kafka_value:symbol::VARCHAR AS symbol,
        kafka_value:company_name::VARCHAR AS company_name,
        kafka_value:timestamp::TIMESTAMP AS timestamp,
        kafka_value:price::DECIMAL(10,2) AS price,
        kafka_value:volume::BIGINT AS volume,
        kafka_value:open::DECIMAL(10,2) AS open_price,
        kafka_value:high::DECIMAL(10,2) AS high_price,
        kafka_value:low::DECIMAL(10,2) AS low_price,
        kafka_value:close::DECIMAL(10,2) AS close_price,
        kafka_value:previous_close::DECIMAL(10,2) AS previous_close,
        kafka_value:change_amount::DECIMAL(10,2) AS change_amount,
        kafka_value:change_percentage::DECIMAL(5,2) AS change_percentage,
        kafka_value:market_cap::BIGINT AS market_cap,
        kafka_value:pe_ratio::DECIMAL(10,2) AS pe_ratio,
        kafka_value:week_52_high::DECIMAL(10,2) AS week_52_high,
        kafka_value:week_52_low::DECIMAL(10,2) AS week_52_low,
        kafka_value:avg_volume_30d::BIGINT AS avg_volume_30d,
        kafka_value:dividend_yield::DECIMAL(5,2) AS dividend_yield,
        kafka_value:beta::DECIMAL(5,2) AS beta,
        kafka_value:exchange::VARCHAR AS exchange,
        kafka_value:currency::VARCHAR AS currency,
        
        -- Metadata
        kafka_timestamp,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source_data
)

SELECT * FROM parsed

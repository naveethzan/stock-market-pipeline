{{
    config(
        materialized='view',
        dist='symbol',
        sort=['dbt_valid_from']
    )
}}

SELECT 
    dbt_scd_id as company_key,
    symbol,
    company_name,
    exchange, 
    market_cap,
    source_timestamp,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to IS NULL) as is_current,
    dbt_updated_at as updated_at
FROM {{ ref('company_snapshot') }}

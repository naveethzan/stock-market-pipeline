{% snapshot company_snapshot %}
    {{
        config(
            target_database='stockmarket',
            target_schema='marts',
            unique_key='symbol',
            strategy='check',
            check_cols=['company_name', 'exchange', 'market_cap'],
            invalidate_hard_deletes=true
        )
    }}
    
    SELECT 
        symbol,
        company_name, 
        exchange,
        market_cap,
        timestamp as source_timestamp
    FROM {{ ref('stg_processed_stock') }}
    WHERE company_name IS NOT NULL
      AND exchange IS NOT NULL  
      AND market_cap IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) = 1
    
{% endsnapshot %}

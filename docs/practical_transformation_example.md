# Practical Streaming Transformation Examples
## DBT Models for Stock Market Pipeline

**Purpose**: Concrete examples showing how to handle streaming data characteristics in DBT models  
**Context**: Supplements the corrected DBT Implementation Plan  
**Target**: Address VARIANT parsing, batch processing, and incremental strategies

---

## 🎯 **Core Streaming Challenges & Solutions**

### **Challenge 1: VARIANT Column Parsing from Kafka Connect**

**Current Snowflake Structure**:
```sql
-- STAGING.FACT_STOCK_PRICES_STAGING
CREATE TABLE FACT_STOCK_PRICES_STAGING (
    RECORD_METADATA VARIANT,  -- Kafka metadata (partition, offset, timestamp)
    RECORD_CONTENT VARIANT    -- JSON payload from Spark processor
);

-- Sample data:
RECORD_METADATA: {"partition": 0, "offset": 12345, "timestamp": "2024-01-15T10:30:00Z"}
RECORD_CONTENT: {"symbol": "AAPL", "current_price": 150.25, "volume": 1000000, "processing_timestamp": "2024-01-15T10:29:45Z"}
```

**DBT Solution**:
```sql
-- models/staging/stg_stock_prices.sql
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'processing_timestamp'],
    cluster_by=['symbol', 'processing_date']
) }}

WITH parsed_data AS (
    SELECT
        -- Parse Kafka metadata
        RECORD_METADATA:partition::INTEGER as kafka_partition,
        RECORD_METADATA:offset::INTEGER as kafka_offset,
        RECORD_METADATA:timestamp::TIMESTAMP_NTZ as kafka_timestamp,
        
        -- Parse business data from JSON
        RECORD_CONTENT:symbol::STRING as symbol,
        RECORD_CONTENT:current_price::FLOAT as current_price,
        RECORD_CONTENT:high_price::FLOAT as high_price,
        RECORD_CONTENT:low_price::FLOAT as low_price,
        RECORD_CONTENT:volume::INTEGER as volume,
        RECORD_CONTENT:change_percent::FLOAT as change_percent,
        RECORD_CONTENT:processing_timestamp::TIMESTAMP_NTZ as processing_timestamp,
        
        -- Create processing date for partitioning
        DATE(RECORD_CONTENT:processing_timestamp::TIMESTAMP_NTZ) as processing_date,
        
        -- Create surrogate key for deduplication
        {{ dbt_utils.generate_surrogate_key([
            'RECORD_CONTENT:symbol', 
            'RECORD_CONTENT:processing_timestamp'
        ]) }} as record_key
        
    FROM {{ source('staging', 'fact_stock_prices_staging') }}
    
    {% if is_incremental() %}
        -- Handle micro-batch processing from Spark
        WHERE RECORD_CONTENT:processing_timestamp::TIMESTAMP_NTZ > (
            SELECT COALESCE(MAX(processing_timestamp), '2020-01-01'::TIMESTAMP_NTZ) 
            FROM {{ this }}
        )
    {% endif %}
),

deduplicated AS (
    -- Handle duplicate records from Kafka exactly-once semantics issues
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY record_key 
            ORDER BY kafka_timestamp DESC, kafka_offset DESC
        ) as rn
    FROM parsed_data
)

SELECT 
    symbol,
    current_price,
    high_price,
    low_price,
    volume,
    change_percent,
    processing_timestamp,
    processing_date,
    kafka_partition,
    kafka_offset,
    record_key
FROM deduplicated 
WHERE rn = 1  -- Keep only the latest version of each record
```

---

### **Challenge 2: Data Quality Validation (Replicate Stored Procedures)**

**Current Stored Procedure Logic**:
```sql
-- SP_TRANSFORM_STOCK_PRICES_AND_INDICATORS validates:
-- 1. Price: 0 < current_price <= 100,000
-- 2. Volume: 0 <= volume <= 1,000,000,000  
-- 3. Change: -100% <= change_percent <= 1000%
-- 4. Required fields not null
```

**DBT Solution**:
```sql
-- models/intermediate/int_data_quality_validation.sql
{{ config(
    materialized='incremental',
    unique_key='record_key',
    tags=['streaming']
) }}

SELECT *,
    -- Replicate exact validation logic from stored procedures
    CASE 
        WHEN symbol IS NULL THEN 'MISSING_SYMBOL'
        WHEN current_price IS NULL THEN 'MISSING_PRICE'
        WHEN current_price <= 0 OR current_price > 100000 THEN 'INVALID_PRICE_RANGE'
        WHEN volume IS NULL THEN 'MISSING_VOLUME'
        WHEN volume < 0 OR volume > 1000000000 THEN 'INVALID_VOLUME_RANGE'
        WHEN change_percent IS NULL THEN 'MISSING_CHANGE_PERCENT'
        WHEN ABS(change_percent) > 1000 THEN 'INVALID_CHANGE_PERCENT'
        WHEN processing_timestamp IS NULL THEN 'MISSING_TIMESTAMP'
        ELSE 'VALID'
    END as data_quality_status,
    
    -- Create quality score (0-100) for monitoring
    CASE 
        WHEN symbol IS NOT NULL 
             AND current_price > 0 AND current_price <= 100000
             AND volume >= 0 AND volume <= 1000000000
             AND ABS(COALESCE(change_percent, 0)) <= 1000
             AND processing_timestamp IS NOT NULL
        THEN 100
        ELSE 0
    END as data_quality_score,
    
    -- Flag for downstream processing
    CASE 
        WHEN symbol IS NOT NULL 
             AND current_price > 0 AND current_price <= 100000
             AND volume >= 0 AND volume <= 1000000000
             AND ABS(COALESCE(change_percent, 0)) <= 1000
             AND processing_timestamp IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END as is_valid_record

FROM {{ ref('stg_stock_prices') }}

{% if is_incremental() %}
    WHERE processing_timestamp > (
        SELECT COALESCE(MAX(processing_timestamp), '2020-01-01'::TIMESTAMP_NTZ) 
        FROM {{ this }}
    )
{% endif %}
```

**Data Quality Tests**:
```yaml
# models/intermediate/schema.yml
version: 2

models:
  - name: int_data_quality_validation
    description: "Stock price data with quality validation flags"
    columns:
      - name: record_key
        description: "Unique record identifier"
        tests:
          - unique
          - not_null
      
      - name: current_price
        description: "Current stock price"
        tests:
          - not_null:
              where: "data_quality_status = 'VALID'"
          - dbt_utils.accepted_range:
              min_value: 0.01
              max_value: 100000
              where: "data_quality_status = 'VALID'"
      
      - name: data_quality_score
        description: "Quality score 0-100"
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100

    tests:
      - dbt_utils.expression_is_true:
          expression: "data_quality_score = 100"
          condition: "data_quality_status = 'VALID'"
      
      # Business rule: Valid records should have positive prices
      - dbt_utils.expression_is_true:
          expression: "current_price > 0"
          condition: "is_valid_record = TRUE"
```

---

### **Challenge 3: Simplified Company Dimension (No Complex SCD)**

**Current Approach**: Complex SCD Type 2 with stored procedures  
**DBT Approach**: Simple incremental model for 5 companies

```sql
-- models/dimensions/dim_company.sql
{{ config(
    materialized='incremental',
    unique_key='symbol',
    cluster_by=['symbol']
) }}

WITH company_discovery AS (
    -- Discover companies from validated streaming data
    SELECT DISTINCT 
        symbol,
        MIN(processing_timestamp) as first_seen,
        MAX(processing_timestamp) as last_seen,
        COUNT(*) as total_records
    FROM {{ ref('int_data_quality_validation') }}
    WHERE is_valid_record = TRUE
      AND symbol IS NOT NULL
    {% if is_incremental() %}
        AND processing_timestamp > (
            SELECT COALESCE(MAX(last_seen), '2020-01-01'::TIMESTAMP_NTZ) 
            FROM {{ this }}
        )
    {% endif %}
    GROUP BY symbol
),

company_enrichment AS (
    SELECT 
        symbol,
        first_seen,
        last_seen,
        total_records,
        
        -- Static company data for resume project (5 companies)
        CASE symbol
            WHEN 'AAPL' THEN 'Apple Inc.'
            WHEN 'GOOGL' THEN 'Alphabet Inc.'
            WHEN 'MSFT' THEN 'Microsoft Corporation' 
            WHEN 'AMZN' THEN 'Amazon.com Inc.'
            WHEN 'TSLA' THEN 'Tesla Inc.'
            ELSE 'Unknown Company'
        END as company_name,
        
        CASE symbol
            WHEN 'AAPL' THEN 'Technology'
            WHEN 'GOOGL' THEN 'Technology'
            WHEN 'MSFT' THEN 'Technology'
            WHEN 'AMZN' THEN 'Consumer Discretionary'
            WHEN 'TSLA' THEN 'Consumer Discretionary'
            ELSE 'Unknown'
        END as sector,
        
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['symbol']) }} as company_key
        
    FROM company_discovery
)

SELECT 
    company_key,
    symbol,
    company_name,
    sector,
    first_seen as effective_date,
    last_seen as last_updated,
    total_records,
    TRUE as is_current,
    CURRENT_TIMESTAMP as dbt_updated_at
    
FROM company_enrichment

{% if is_incremental() %}
-- Update existing records with new last_seen timestamps
UNION ALL

SELECT 
    existing.company_key,
    existing.symbol,
    existing.company_name,
    existing.sector,
    existing.effective_date,
    GREATEST(existing.last_updated, new_data.last_seen) as last_updated,
    existing.total_records + new_data.total_records as total_records,
    TRUE as is_current,
    CURRENT_TIMESTAMP as dbt_updated_at
    
FROM {{ this }} existing
JOIN company_enrichment new_data ON existing.symbol = new_data.symbol
{% endif %}
```

---

### **Challenge 4: Fact Table with Streaming-Optimized Performance**

```sql
-- models/facts/fact_stock_prices_and_indicators.sql
{{ config(
    materialized='incremental',
    unique_key=['company_key', 'processing_timestamp'],
    cluster_by=['company_key', 'processing_date', 'processing_hour'],
    tags=['streaming'],
    incremental_strategy='merge'
) }}

WITH enriched_stock_data AS (
    SELECT 
        sqv.*,
        -- Add time-based partitioning columns
        HOUR(sqv.processing_timestamp) as processing_hour,
        MINUTE(sqv.processing_timestamp) as processing_minute,
        
        -- Business calculations (replicate stored procedure logic)
        LAG(sqv.current_price, 1) OVER (
            PARTITION BY sqv.symbol 
            ORDER BY sqv.processing_timestamp
        ) as previous_price,
        
        -- Calculate price change
        CASE 
            WHEN LAG(sqv.current_price, 1) OVER (
                PARTITION BY sqv.symbol 
                ORDER BY sqv.processing_timestamp
            ) IS NOT NULL
            THEN sqv.current_price - LAG(sqv.current_price, 1) OVER (
                PARTITION BY sqv.symbol 
                ORDER BY sqv.processing_timestamp
            )
            ELSE NULL
        END as price_change,
        
        -- Market session logic
        CASE 
            WHEN HOUR(sqv.processing_timestamp) BETWEEN 9 AND 16 
            THEN 'REGULAR_HOURS'
            WHEN HOUR(sqv.processing_timestamp) BETWEEN 4 AND 9 
            THEN 'PRE_MARKET'
            ELSE 'AFTER_HOURS'
        END as trading_session
        
    FROM {{ ref('int_data_quality_validation') }} sqv
    WHERE sqv.is_valid_record = TRUE
    {% if is_incremental() %}
        AND sqv.processing_timestamp > (
            SELECT COALESCE(MAX(processing_timestamp), '2020-01-01'::TIMESTAMP_NTZ) 
            FROM {{ this }}
        )
    {% endif %}
),

dimensional_joins AS (
    SELECT 
        esd.*,
        dc.company_key,
        dd.date_key,
        
        -- Create time key (HHMM format for dimension lookup)
        (esd.processing_hour * 100 + esd.processing_minute) as time_key
        
    FROM enriched_stock_data esd
    
    -- Company dimension lookup
    JOIN {{ ref('dim_company') }} dc 
        ON esd.symbol = dc.symbol 
        AND dc.is_current = TRUE
    
    -- Date dimension lookup  
    JOIN {{ ref('dim_date') }} dd 
        ON esd.processing_date = dd.date_actual
)

SELECT 
    -- Surrogate key for fact table
    {{ dbt_utils.generate_surrogate_key([
        'company_key', 
        'processing_timestamp'
    ]) }} as stock_price_key,
    
    -- Dimension foreign keys
    company_key,
    date_key,
    time_key,
    
    -- Fact measures
    current_price,
    high_price,
    low_price,
    volume,
    change_percent,
    price_change,
    previous_price,
    
    -- Derived measures
    CASE 
        WHEN volume > 0 THEN current_price * volume 
        ELSE 0 
    END as dollar_volume,
    
    -- Metadata
    processing_timestamp,
    processing_date,
    processing_hour,
    processing_minute,
    trading_session,
    kafka_partition,
    kafka_offset,
    
    -- Audit columns
    CURRENT_TIMESTAMP as dbt_created_at,
    '{{ invocation_id }}' as dbt_batch_id

FROM dimensional_joins
```

---

### **Challenge 5: Cron Job Configuration with Error Handling**

**Production Cron Setup** (`/etc/cron.d/dbt-streaming`):
```bash
# DBT Stock Market Streaming Pipeline
# Runs every 5 minutes aligned with Spark batch processing

# Environment variables
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
DBT_PROJECT_DIR=/opt/dbt/stock_market_dbt
DBT_PROFILES_DIR=/opt/dbt/profiles
LOG_DIR=/var/log/dbt

# Main streaming pipeline - every 5 minutes
*/5 * * * * dbt_user cd $DBT_PROJECT_DIR && source ~/.profile && dbt run --select tag:streaming --target prod >> $LOG_DIR/streaming_$(date +\%Y\%m\%d).log 2>&1 || echo "$(date): DBT streaming pipeline failed" >> $LOG_DIR/errors.log

# Company dimension updates - every hour (new companies are rare)  
0 * * * * dbt_user cd $DBT_PROJECT_DIR && source ~/.profile && dbt run --select dim_company --target prod >> $LOG_DIR/dimensions_$(date +\%Y\%m\%d).log 2>&1

# Data quality tests - every 30 minutes
*/30 * * * * dbt_user cd $DBT_PROJECT_DIR && source ~/.profile && dbt test --select tag:streaming --target prod >> $LOG_DIR/tests_$(date +\%Y\%m\%d).log 2>&1 || echo "$(date): DBT tests failed" >> $LOG_DIR/test_failures.log

# Log rotation - daily at midnight
0 0 * * * root find $LOG_DIR -name "*.log" -mtime +7 -delete
```

**Error Monitoring Script** (`scripts/monitor_dbt_pipeline.sh`):
```bash
#!/bin/bash

LOG_DIR="/var/log/dbt"
ERROR_LOG="$LOG_DIR/errors.log" 
TEST_FAILURE_LOG="$LOG_DIR/test_failures.log"
ALERT_EMAIL="admin@company.com"

# Check for recent errors (last 10 minutes)
RECENT_ERRORS=$(tail -n 100 $ERROR_LOG | grep "$(date -d '10 minutes ago' '+%Y-%m-%d %H:%M')" | wc -l)

if [ $RECENT_ERRORS -gt 0 ]; then
    echo "DBT Streaming Pipeline Alert: $RECENT_ERRORS errors in last 10 minutes" | \
    mail -s "DBT Pipeline Failures" $ALERT_EMAIL
fi

# Check data freshness
LATEST_RUN=$(tail -n 1 $LOG_DIR/streaming_$(date +%Y%m%d).log | grep "Completed successfully")
if [ -z "$LATEST_RUN" ]; then
    echo "DBT Streaming Pipeline Alert: No successful runs detected recently" | \
    mail -s "DBT Pipeline Stalled" $ALERT_EMAIL
fi
```

---

## 🎯 **Key Implementation Differences from Original Plan**

### **1. Phase Reordering**
- **Original**: Dimensions first → Business logic later
- **Fixed**: Staging + Business logic first → Understand data → Build dimensions

### **2. SCD Strategy**
- **Original**: Complex snapshots for 5 companies
- **Fixed**: Simple incremental model with static company data

### **3. Orchestration Focus**
- **Original**: Buried cron setup in Phase 4.3
- **Fixed**: Cron strategy central to architecture from Phase 1

### **4. Streaming-Specific Logic**
- **Original**: Missing VARIANT parsing, batch awareness
- **Fixed**: Explicit handling of Kafka metadata, micro-batch processing

### **5. Data Quality**
- **Original**: Generic DBT tests
- **Fixed**: Exact replication of stored procedure business rules

---

**This practical guide provides concrete, copy-paste examples for implementing the streaming-aware DBT models that handle the real complexities of your Kafka + Spark + Snowflake pipeline.**
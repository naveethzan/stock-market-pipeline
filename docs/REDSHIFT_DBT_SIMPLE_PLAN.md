# Simplified Redshift + DBT Implementation Plan
## 3-Layer Architecture with SCD Type 2

---

## Overview

**Architecture**: Streaming → Staging → Marts (3 layers only)
**Processing**: Incremental using `this.max(timestamp)`
**Frequency**: DBT runs every 5 minutes
**Migration**: Big bang - Remove Snowflake → Deploy Redshift → Restart

---

## Phase 1: Remove Snowflake Components

### 1.1 Files to Delete
```bash
# Remove Snowflake-specific files
rm -rf snowflake/
rm -rf scripts/snowflake_setup.sql
rm -rf config/snowflake_config.json
rm docker-compose.snowflake.yml
```

### 1.2 Update Configuration Files

**config/.env** - Remove Snowflake variables, add Redshift:
```bash
# Remove all SNOWFLAKE_* variables
# Add Redshift configuration
REDSHIFT_ENDPOINT=your-workgroup.region.redshift-serverless.amazonaws.com
REDSHIFT_DATABASE=stockmarket
REDSHIFT_PORT=5439
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=YourSecurePassword123!
REDSHIFT_IAM_ROLE=arn:aws:iam::account:role/RedshiftStreamingRole

# Existing Kafka config remains unchanged
KAFKA_BROKER=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8085
```

**Makefile** - Remove Snowflake targets:
```makefile
# Remove these targets:
# - snowflake-setup
# - snowflake-test  
# - deploy-snowflake-connectors
```

---

## Phase 2: Setup Redshift Serverless

### 2.1 Create Redshift Infrastructure

```bash
# Create namespace
aws redshift-serverless create-namespace \
    --namespace-name stock-market-namespace \
    --db-name stockmarket \
    --admin-username admin \
    --admin-user-password ${REDSHIFT_PASSWORD}

# Create workgroup
aws redshift-serverless create-workgroup \
    --workgroup-name stock-market-workgroup \
    --namespace-name stock-market-namespace \
    --base-capacity 32
```

### 2.2 Create Redshift Schemas and Streaming Tables

**scripts/create_redshift_schemas.sql:**
```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS streaming;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Streaming Layer: Raw Kafka data (3 topics → 3 tables)

-- 1. Processed Stock Data
CREATE TABLE streaming.processed_stock_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON from Spark
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(kafka_key)
SORTKEY(kafka_timestamp)
ENCODE AUTO;

-- 2. Technical Indicators
CREATE TABLE streaming.technical_indicators_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON from Spark
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(kafka_key)
SORTKEY(kafka_timestamp)
ENCODE AUTO;

-- 3. Aggregated Data
CREATE TABLE streaming.aggregated_data_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON from Spark
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(kafka_key)
SORTKEY(kafka_timestamp)
ENCODE AUTO;
```

---

## Phase 3: Kafka Connect for Redshift

### 3.1 Redshift Sink Connector Configuration

**scripts/deploy-redshift-connectors.sh:**
```bash
#!/bin/bash

CONNECT_HOST="localhost:8084"

# Wait for Kafka Connect
echo "Waiting for Kafka Connect..."
while ! curl -s "$CONNECT_HOST/connectors" > /dev/null; do
    sleep 5
done

# Deploy 3 connectors for 3 topics
for topic in "stock-quotes-processed" "technical-indicators" "stock-aggregations"; do
    table_name=$(echo $topic | sed 's/-/_/g')_stream
    
    curl -X PUT "$CONNECT_HOST/connectors/redshift-$topic/config" \
      -H "Content-Type: application/json" \
      -d '{
        "connector.class": "io.confluent.connect.aws.redshift.RedshiftSinkConnector",
        "tasks.max": "1",
        "topics": "'$topic'",
        "aws.redshift.domain": "'${REDSHIFT_ENDPOINT}'",
        "aws.redshift.port": "'${REDSHIFT_PORT}'",
        "aws.redshift.database": "'${REDSHIFT_DATABASE}'",
        "aws.redshift.user": "'${REDSHIFT_USER}'",
        "aws.redshift.password": "'${REDSHIFT_PASSWORD}'",
        "table.name.format": "streaming.'$table_name'",
        "auto.create": "false",
        "batch.size": "100",
        "pk.mode": "kafka"
      }'
done

echo "Redshift connectors deployed!"
```

---

## Phase 4: DBT Project Setup

### 4.1 DBT Docker Configuration

**dbt/Dockerfile:**
```dockerfile
FROM python:3.10-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install dbt-core==1.7.0 dbt-redshift==1.7.0

WORKDIR /usr/app/dbt
COPY . .

ENV DBT_PROFILES_DIR=/usr/app/dbt

ENTRYPOINT ["dbt"]
```

### 4.2 DBT Profile Configuration

**dbt/profiles.yml:**
```yaml
stock_market:
  target: prod
  outputs:
    prod:
      type: redshift
      host: "{{ env_var('REDSHIFT_ENDPOINT') }}"
      port: 5439
      user: "{{ env_var('REDSHIFT_USER') }}"
      password: "{{ env_var('REDSHIFT_PASSWORD') }}"
      dbname: stockmarket
      schema: staging
      threads: 4
      keepalives_idle: 240
```

### 4.4 DBT Project Configuration Updates

**dbt_project.yml additions:**
```yaml
# Add snapshot configuration
snapshot-paths: ["snapshots"]

snapshots:
  stock_market_dbt:
    +target_database: stockmarket
    +target_schema: marts
    +materialized: snapshot
    +strategy: check
    +check_cols: ['company_name', 'exchange', 'market_cap']
    +invalidate_hard_deletes: true
```

### 4.3 DBT Project Structure

```bash
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── _staging.yml
│   │   ├── stg_processed_stock.sql
│   │   ├── stg_technical_indicators.sql
│   │   └── stg_aggregated_data.sql
│   └── marts/
│       ├── _marts.yml
│       ├── dimensions/
│       │   ├── dim_company.sql
│       │   ├── dim_date.sql
│       │   └── dim_time.sql
│       └── facts/
│           ├── fact_stock_prices.sql
│           └── fact_trading_volume.sql
├── snapshots/
│   └── company_snapshot.sql
├── macros/
│   └── incremental_helper.sql
└── tests/
    └── row_count_validation.sql
```

---

## Phase 5: DBT Models Implementation

### 5.1 Staging Layer - Parse JSON to Columns

**models/staging/_staging.yml:**
```yaml
version: 2

sources:
  - name: streaming
    database: stockmarket
    schema: streaming
    tables:
      - name: processed_stock_stream
      - name: technical_indicators_stream
      - name: aggregated_data_stream

models:
  - name: stg_processed_stock
    description: "Parsed processed stock data"
    config:
      materialized: incremental
      unique_key: record_id
      on_schema_change: append_new_columns
      
  - name: stg_technical_indicators
    description: "Parsed technical indicators"
    config:
      materialized: incremental
      unique_key: record_id
      on_schema_change: append_new_columns
      
  - name: stg_aggregated_data
    description: "Parsed aggregated data"
    config:
      materialized: incremental
      unique_key: record_id
      on_schema_change: append_new_columns
```

**models/staging/stg_processed_stock.sql:**
```sql
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
```

**models/staging/stg_technical_indicators.sql:**
```sql
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
```

**models/staging/stg_aggregated_data.sql:**
```sql
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
```

### 5.2 Marts Layer - Dimensional Model

**models/marts/_marts.yml:**
```yaml
version: 2

models:
  - name: dim_company
    description: "Company dimension with SCD Type 2 via DBT snapshots"
    columns:
      - name: company_key
        description: "Unique identifier for company dimension"
        tests:
          - not_null
          - unique
      - name: symbol
        description: "Stock ticker symbol"
        tests:
          - not_null
      - name: is_current
        description: "Flag indicating current active record"
        
  - name: dim_date
    description: "Date dimension for date-based analysis"
    columns:
      - name: date_key
        description: "Date key in YYYYMMDD format"
        tests:
          - not_null
          - unique
          
  - name: dim_time
    description: "Time dimension for intraday analysis"
    columns:
      - name: time_key
        description: "Minute of day (0-1439)"
        tests:
          - not_null
          - unique
          
  - name: fact_stock_prices
    description: "Stock price facts with technical indicators"
    columns:
      - name: stock_price_key
        description: "Unique identifier for stock price record"
        tests:
          - not_null
          - unique
          
  - name: fact_trading_volume
    description: "Trading volume facts from aggregated data"
    columns:
      - name: trading_volume_key
        description: "Unique identifier for trading volume record"
        tests:
          - not_null
          - unique
```

#### 5.2.1 Company Snapshot (SCD Type 2 using DBT Snapshots)

**snapshots/company_snapshot.sql:**
```sql
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
```

**models/marts/dimensions/dim_company.sql (View over snapshot):**
```sql
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
```

#### 5.2.2 Date Dimension (Static)

**models/marts/dimensions/dim_date.sql (Static dimension):**
```sql
{{
    config(
        materialized='table',
        dist='date_key',
        sort=['date_actual']
    )
}}

WITH dates AS (
    -- Generate dates from 2020 to 2030
    SELECT 
        DATEADD(day, seq, '2020-01-01'::DATE) AS date_actual
    FROM 
        (SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS seq 
         FROM stl_scan 
         LIMIT 4018)  -- ~11 years
),

date_dimension AS (
    SELECT
        TO_CHAR(date_actual, 'YYYYMMDD')::INTEGER AS date_key,
        date_actual,
        EXTRACT(YEAR FROM date_actual) AS year,
        EXTRACT(QUARTER FROM date_actual) AS quarter,
        EXTRACT(MONTH FROM date_actual) AS month,
        EXTRACT(DAY FROM date_actual) AS day,
        EXTRACT(DOW FROM date_actual) AS day_of_week,
        TO_CHAR(date_actual, 'Month') AS month_name,
        TO_CHAR(date_actual, 'Day') AS day_name,
        CASE 
            WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
            ELSE TRUE
        END AS is_weekday,
        CASE 
            WHEN EXTRACT(DOW FROM date_actual) NOT IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_trading_day  -- Simplified, would need holiday calendar
    FROM dates
)

SELECT * FROM date_dimension
```

#### 5.2.3 Time Dimension (Static)

**models/marts/dimensions/dim_time.sql (Static dimension):**
```sql
{{
    config(
        materialized='table',
        dist='time_key',
        sort=['hour', 'minute']
    )
}}

WITH minutes AS (
    -- Generate 1440 minutes (24 hours * 60 minutes)
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 1) - 1 AS minute_of_day
    FROM stl_scan
    LIMIT 1440
),

time_dimension AS (
    SELECT
        minute_of_day AS time_key,
        minute_of_day / 60 AS hour,
        MOD(minute_of_day, 60) AS minute,
        LPAD((minute_of_day / 60)::VARCHAR, 2, '0') || ':' || 
        LPAD(MOD(minute_of_day, 60)::VARCHAR, 2, '0') || ':00' AS time_string,
        CASE
            WHEN minute_of_day >= 570 AND minute_of_day < 960 THEN TRUE  -- 9:30 AM - 4:00 PM
            ELSE FALSE
        END AS is_trading_hours,
        CASE
            WHEN minute_of_day / 60 < 12 THEN 'AM'
            ELSE 'PM'
        END AS am_pm
    FROM minutes
)

SELECT * FROM time_dimension
```

#### 5.2.4 Fact Tables

**models/marts/facts/fact_stock_prices.sql:**
```sql
{{
    config(
        materialized='incremental',
        unique_key='stock_price_key',
        dist='symbol',
        sort=['date_key', 'time_key']
    )
}}

WITH stock_data AS (
    SELECT 
        s.*,
        t.rsi,
        t.macd,
        t.sma_20,
        t.sma_50,
        t.vwap
    FROM {{ ref('stg_processed_stock') }} s
    LEFT JOIN {{ ref('stg_technical_indicators') }} t
        ON s.symbol = t.symbol 
        AND s.timestamp = t.timestamp
    {% if is_incremental() %}
        WHERE s.dbt_loaded_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Keys
    MD5(symbol || '::' || timestamp::VARCHAR) AS stock_price_key,
    symbol AS company_symbol,
    TO_CHAR(DATE(timestamp), 'YYYYMMDD')::INTEGER AS date_key,
    EXTRACT(HOUR FROM timestamp) * 60 + EXTRACT(MINUTE FROM timestamp) AS time_key,
    
    -- Degenerate dimensions
    symbol,
    timestamp,
    
    -- Measures (all pre-calculated by Spark)
    price AS current_price,
    open_price,
    high_price,
    low_price,
    close_price,
    previous_close,
    change_amount,
    change_percentage,
    volume,
    market_cap,
    pe_ratio,
    
    -- Technical indicators (from joined data)
    rsi,
    macd,
    sma_20,
    sma_50,
    vwap,
    
    -- Simple categorizations
    CASE 
        WHEN change_percentage > 2 THEN 'Strong Up'
        WHEN change_percentage > 0 THEN 'Up'
        WHEN change_percentage < -2 THEN 'Strong Down'
        WHEN change_percentage < 0 THEN 'Down'
        ELSE 'Flat'
    END AS price_trend,
    
    CASE
        WHEN rsi > 70 THEN 'Overbought'
        WHEN rsi < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal,
    
    -- Audit
    CURRENT_TIMESTAMP AS created_at
FROM stock_data
```

**models/marts/facts/fact_trading_volume.sql:**
```sql
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
```

---

## Phase 6: DBT Execution & Operations

### 6.1 Simplified Execution Approach

Instead of complex automation scripts, we use a **simple, structured approach** for DBT operations.

**📋 Complete Execution Guide:** See `docs/DBT_EXECUTION_GUIDE.md`

### 6.2 Essential Commands Summary

#### One-Time Setup
```bash
# 1. Test connection
dbt debug

# 2. Initialize static dimensions
dbt run --models dim_date dim_time

# 3. Initialize company snapshot (SCD Type 2)
dbt snapshot --select company_snapshot

# 4. Create company dimension view
dbt run --models dim_company
```

#### Regular Operations (Every 5 Minutes)
```bash
# Single command for regular runs
dbt snapshot --select company_snapshot && dbt run && dbt test
```

#### Development & Debugging
```bash
# Compile models (check syntax)
dbt compile

# Run specific model
dbt run --models fact_stock_prices

# Run with dependencies
dbt run --models +fact_stock_prices

# Full refresh (recreate incrementals)
dbt run --full-refresh
```

### 6.3 Simple Automation Options

#### Option A: Basic Cron
```bash
# Edit crontab
crontab -e

# Add line for every 5 minutes
*/5 * * * * cd /path/to/dbt/project && dbt snapshot --select company_snapshot && dbt run && dbt test
```

#### Option B: Manual Development
```bash
# Run every 5 minutes during development
watch -n 300 'dbt snapshot --select company_snapshot && dbt run'
```

### 6.4 Health Checks

#### Quick Status Check
```bash
# Check recent data loads
psql -h $REDSHIFT_ENDPOINT -U $REDSHIFT_USER -d stockmarket -c "
SELECT 
    'staging.stg_processed_stock' as table_name, 
    COUNT(*) as row_count,
    MAX(dbt_loaded_at) as latest_load
FROM staging.stg_processed_stock
UNION ALL
SELECT 
    'marts.fact_stock_prices' as table_name, 
    COUNT(*) as row_count,
    MAX(created_at) as latest_load  
FROM marts.fact_stock_prices;
"
```

#### Generate Documentation
```bash
# Create and serve DBT docs
dbt docs generate
dbt docs serve  # Opens at localhost:8080
```

---

## Phase 7: Migration Execution

### 7.1 Pre-Migration
```bash
# 1. Stop current pipeline
make stop

# 2. Backup Kafka topic offsets
docker exec -it kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --all-groups \
    --describe > kafka_offsets_backup.txt
```

### 7.2 Migration Steps
```bash
# 1. Remove Snowflake
rm -rf snowflake/
rm docker-compose.snowflake.yml

# 2. Setup Redshift
aws redshift-serverless create-namespace --namespace-name stock-market-namespace ...
psql -h $REDSHIFT_ENDPOINT -f scripts/create_redshift_schemas.sql

# 3. Deploy Kafka Connect for Redshift
./scripts/deploy-redshift-connectors.sh

# 4. Test DBT connection
dbt debug

# 5. Initialize DBT (one-time setup)
dbt run --models dim_date dim_time
dbt snapshot --select company_snapshot
dbt run --models dim_company

# 6. Start pipeline
make start

# 7. Start regular DBT operations
# Option A: Manual runs
dbt snapshot --select company_snapshot && dbt run && dbt test

# Option B: Basic cron (optional)
crontab -e  # Add: */5 * * * * cd /path/to/dbt && dbt snapshot --select company_snapshot && dbt run && dbt test
```

### 7.3 Post-Migration Validation
```sql
-- Check streaming ingestion
SELECT COUNT(*), MAX(kafka_timestamp) 
FROM streaming.processed_stock_stream;

-- Check staging layer
SELECT COUNT(*), MAX(dbt_loaded_at) 
FROM staging.stg_processed_stock;

-- Check fact table
SELECT COUNT(*), MAX(created_at) 
FROM marts.fact_stock_prices;

-- Verify SCD2 is working
SELECT symbol, COUNT(*) as versions
FROM marts.dim_company
GROUP BY symbol
HAVING COUNT(*) > 1;
```

---

## Phase 8: Monitoring & Validation

### 8.1 Built-in DBT Monitoring

#### Use DBT Documentation
```bash
# Generate and serve docs (includes data lineage, tests, etc.)
dbt docs generate
dbt docs serve  # View at localhost:8080
```

#### Use DBT Tests
```bash
# Run all data quality tests
dbt test

# Check specific test results
dbt test --models staging
dbt test --models marts
```

### 8.2 Manual Health Checks

#### Quick Data Validation
```bash
# Check recent data loads (from Phase 6.4)
psql -h $REDSHIFT_ENDPOINT -U $REDSHIFT_USER -d stockmarket -c "
SELECT 
    'staging.stg_processed_stock' as table_name, 
    COUNT(*) as row_count,
    MAX(dbt_loaded_at) as latest_load
FROM staging.stg_processed_stock
UNION ALL
SELECT 
    'marts.fact_stock_prices' as table_name, 
    COUNT(*) as row_count,
    MAX(created_at) as latest_load  
FROM marts.fact_stock_prices;
"
```

---

## Summary

This simplified implementation provides:

✅ **3-Layer Architecture**: Streaming → Staging → Marts (no intermediate layer)
✅ **SCD Type 2**: DBT Snapshots tracking 3 important company attributes (name, exchange, market_cap)
✅ **Incremental Processing**: Using `this.max(timestamp)` for watermarks
✅ **5-Minute Runs**: Simple command execution (manual or basic cron)
✅ **Simple Operations**: Single command for regular runs (`dbt snapshot && dbt run && dbt test`)
✅ **Easy Debugging**: Clear, structured commands for troubleshooting
✅ **Big Bang Migration**: Clean cutover from Snowflake to Redshift
✅ **KISS Principle**: No complex scripts, just essential DBT commands

The entire pipeline:
1. Spark calculates everything and sends to Kafka
2. Kafka Connect streams to Redshift
3. DBT parses JSON (staging) and creates dimensional model (marts)
4. No recalculations, just structuring pre-calculated data

# Gold Layer Snowflake Connector

The Gold layer Snowflake connector loads processed dimensional data directly from Kafka topics into Snowflake using Kafka Connect's direct ingestion method, implementing the final layer of the medallion architecture.

## Overview

- **Purpose**: Load processed dimensional data from Kafka topics into Snowflake for analytics
- **Method**: Direct ingestion from Kafka to Snowflake (no intermediate staging)
- **Format**: JSON with automatic schema detection and evolution
- **Topics**: `processed-stock-prices`, `processed-trading-volume`, `processed-technical-indicators`
- **Destination**: Snowflake staging tables for dimensional model

## Configuration

### Key Settings

- **Connector Class**: `com.snowflake.kafka.connector.SnowflakeSinkConnector`
- **Ingestion Method**: Direct ingestion from Kafka to Snowflake
- **Buffer Settings**: 1,000 records, 5MB, or 60 seconds
- **Schema Evolution**: Automatic with schematization enabled
- **Error Handling**: Dead letter queue (`gold-dlq`)

### Topic to Table Mapping

- `processed-stock-prices` → `FACT_STOCK_PRICES_STAGING`
- `processed-trading-volume` → `FACT_TRADING_VOLUME_STAGING`
- `processed-technical-indicators` → `TECHNICAL_INDICATORS_STAGING`

### Environment Variables Required

```bash
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-username
export SNOWFLAKE_PASSWORD=your-password
export SNOWFLAKE_DATABASE=your-database
export SNOWFLAKE_SCHEMA=your-schema
export SNOWFLAKE_WAREHOUSE=your-warehouse
export SNOWFLAKE_ROLE=your-role
```

## Deployment

### 1. Set Environment Variables

Choose either password or private key authentication and set the required variables.

### 2. Create Snowflake Objects

The Kafka Connect connector will automatically create the staging tables if they don't exist:

```sql
-- Tables will be created automatically by Kafka Connect with this structure:
-- FACT_STOCK_PRICES_STAGING (
--     RECORD_CONTENT VARIANT,
--     RECORD_METADATA VARIANT
-- );

-- Optionally, you can pre-create them for better control:
CREATE TABLE IF NOT EXISTS FACT_STOCK_PRICES_STAGING (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT
);

CREATE TABLE IF NOT EXISTS FACT_TRADING_VOLUME_STAGING (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT
);

CREATE TABLE IF NOT EXISTS TECHNICAL_INDICATORS_STAGING (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT
);
```

### 3. Deploy Connector

```bash
./scripts/deploy-gold-connector.sh
```

### 4. Verify Deployment

```bash
python scripts/test-gold-connector.py
```

## Monitoring

### Check Connector Status

```bash
python scripts/kafka-connect-manager.py status gold-snowflake-sink-connector
```

### View Connector Logs

```bash
docker logs kafka-connect | grep gold-snowflake-sink-connector
```

### Monitor Snowflake Data Loading

```sql
-- Check record counts
SELECT COUNT(*) FROM FACT_STOCK_PRICES_STAGING;
SELECT COUNT(*) FROM FACT_TRADING_VOLUME_STAGING;
SELECT COUNT(*) FROM TECHNICAL_INDICATORS_STAGING;

-- Check recent data
SELECT RECORD_CONTENT:symbol, RECORD_CONTENT:processing_timestamp
FROM FACT_STOCK_PRICES_STAGING
ORDER BY RECORD_METADATA:CreateTime DESC
LIMIT 10;
```

### Monitor Data Loading

```sql
-- Check table row counts
SELECT COUNT(*) as record_count FROM FACT_STOCK_PRICES_STAGING;
SELECT COUNT(*) as record_count FROM FACT_TRADING_VOLUME_STAGING;
SELECT COUNT(*) as record_count FROM TECHNICAL_INDICATORS_STAGING;

-- Check recent data loads
SELECT 
    RECORD_METADATA:topic::STRING as topic,
    RECORD_METADATA:partition::INTEGER as partition,
    RECORD_METADATA:offset::INTEGER as offset,
    RECORD_METADATA:CreateTime::TIMESTAMP as load_time,
    COUNT(*) as record_count
FROM FACT_STOCK_PRICES_STAGING
GROUP BY 1,2,3,4
ORDER BY load_time DESC
LIMIT 10;
```

## Data Format

### Snowflake Table Structure

Each staging table contains:
- `RECORD_CONTENT`: VARIANT column with the actual data
- `RECORD_METADATA`: VARIANT column with Kafka metadata

### Example Data Structure

```json
{
  "RECORD_CONTENT": {
    "symbol": "AAPL",
    "date": "2024-08-18",
    "time": "09:30:00",
    "open_price": 150.00,
    "high_price": 152.50,
    "low_price": 149.75,
    "close_price": 151.25,
    "volume": 1500000,
    "sma_20": 150.85,
    "processing_timestamp": 1692345600000,
    "ingestion_layer": "gold"
  },
  "RECORD_METADATA": {
    "topic": "processed-stock-prices",
    "partition": 0,
    "offset": 12345,
    "CreateTime": "2024-08-18T09:30:00.000Z"
  }
}
```

### Querying Data

```sql
-- Extract data from VARIANT columns
SELECT 
    RECORD_CONTENT:symbol::STRING as symbol,
    RECORD_CONTENT:close_price::FLOAT as close_price,
    RECORD_CONTENT:volume::INTEGER as volume,
    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_time
FROM FACT_STOCK_PRICES_STAGING
WHERE RECORD_CONTENT:symbol = 'AAPL';
```

## Error Handling

### Dead Letter Queue

Failed records are sent to the `gold-dlq` topic with:
- Original record data
- Snowflake error details
- Timestamp of failure
- Connection context

### Common Issues

1. **Authentication Failures**: Check credentials and network connectivity
2. **Schema Mismatches**: Ensure staging tables exist with correct structure
3. **Warehouse Suspension**: Ensure warehouse is running and has sufficient credits
4. **Network Issues**: Check connectivity to Snowflake from Kafka Connect

### Troubleshooting

```bash
# Check connector status
curl http://localhost:8083/connectors/gold-snowflake-sink-connector/status

# Restart connector
python scripts/kafka-connect-manager.py restart gold-snowflake-sink-connector

# Check dead letter queue
kafka-console-consumer --bootstrap-server localhost:29092 --topic gold-dlq --from-beginning

# Check Snowflake query history
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%FACT_STOCK_PRICES_STAGING%'
ORDER BY START_TIME DESC;
```

## Performance Tuning

### Buffer Settings

- `buffer.count.records`: Number of records before flush (default: 1,000)
- `buffer.size.bytes`: Buffer size in bytes (default: 5MB)
- `buffer.flush.time`: Time-based flush in seconds (default: 60)

### Direct Ingestion Settings

- Data flows directly from Kafka to Snowflake
- No intermediate staging required
- Real-time data loading with configurable buffering
- Built-in retry and error handling

### Parallelism

- `tasks.max`: Number of parallel tasks (default: 3)
- Increase for higher throughput
- Consider Snowflake concurrency limits

### Warehouse Sizing

- Use appropriate warehouse size for expected load
- Consider auto-suspend and auto-resume settings
- Monitor credit usage and query performance

## Data Pipeline Integration

### Staging to Production

Create stored procedures to move data from staging to dimensional tables:

```sql
-- Example procedure to load fact table
CREATE OR REPLACE PROCEDURE LOAD_FACT_STOCK_PRICES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert new records into fact table
    INSERT INTO FACT_STOCK_PRICES (
        company_key, date_key, time_key,
        open_price, high_price, low_price, close_price,
        volume, sma_20, sma_50, rsi_14,
        processing_timestamp, data_source
    )
    SELECT 
        dc.company_key,
        dd.date_key,
        dt.time_key,
        s.RECORD_CONTENT:open_price::FLOAT,
        s.RECORD_CONTENT:high_price::FLOAT,
        s.RECORD_CONTENT:low_price::FLOAT,
        s.RECORD_CONTENT:close_price::FLOAT,
        s.RECORD_CONTENT:volume::INTEGER,
        s.RECORD_CONTENT:sma_20::FLOAT,
        s.RECORD_CONTENT:sma_50::FLOAT,
        s.RECORD_CONTENT:rsi_14::FLOAT,
        s.RECORD_CONTENT:processing_timestamp::INTEGER,
        s.RECORD_CONTENT:data_source::STRING
    FROM FACT_STOCK_PRICES_STAGING s
    JOIN DIM_COMPANY dc ON dc.symbol = s.RECORD_CONTENT:symbol::STRING
    JOIN DIM_DATE dd ON dd.date_value = s.RECORD_CONTENT:date::DATE
    JOIN DIM_TIME dt ON dt.time_value = s.RECORD_CONTENT:time::TIME
    WHERE s.RECORD_CONTENT:processing_timestamp::INTEGER > 
          (SELECT COALESCE(MAX(processing_timestamp), 0) FROM FACT_STOCK_PRICES);
    
    -- Clean up processed staging records
    DELETE FROM FACT_STOCK_PRICES_STAGING 
    WHERE RECORD_CONTENT:processing_timestamp::INTEGER <= 
          (SELECT MAX(processing_timestamp) FROM FACT_STOCK_PRICES);
    
    RETURN 'SUCCESS';
END;
$$;
```

### Scheduling

Use Snowflake tasks to schedule data movement:

```sql
-- Create task to run every 5 minutes
CREATE OR REPLACE TASK LOAD_STOCK_PRICES_TASK
    WAREHOUSE = 'STREAMING_WH'
    SCHEDULE = 'USING CRON 0/5 * * * * UTC'
AS
    CALL LOAD_FACT_STOCK_PRICES();

-- Start the task
ALTER TASK LOAD_STOCK_PRICES_TASK RESUME;
```

## Maintenance

### Schema Evolution

- Snowflake connector handles schema evolution automatically
- New fields are added as new columns in VARIANT structure
- No downtime required for schema changes
- Direct ingestion maintains data freshness and simplicity

### Data Retention

- Configure Time Travel retention for staging tables
- Set up data lifecycle policies
- Monitor storage costs

### Monitoring Checklist

- [ ] Connector status is RUNNING
- [ ] All tasks are RUNNING
- [ ] No errors in dead letter queue
- [ ] Data is flowing into staging tables
- [ ] Direct connection to Snowflake is working
- [ ] Warehouse is running and has sufficient credits
- [ ] Data transformation procedures are working
- [ ] Query performance meets SLA requirements
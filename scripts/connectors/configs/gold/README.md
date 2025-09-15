# Gold Layer Connector

Gold layer connector for storing analytics data from Kafka topics to Redshift for real-time analytics.

## Configuration

**File:** `redshift-streaming-connector.json`

**Purpose:** Stores processed analytics data from Kafka topics to Redshift for real-time dashboards and analytics.

## Topics

- `processed-stock-prices` - Processed stock price data
- `processed-trading-volume` - Processed trading volume data
- `processed-technical-indicators` - Processed technical indicators data

## Features

- **Format:** JSON for flexible analytics
- **Database:** Amazon Redshift
- **Tables:** Streaming tables for real-time analytics
- **Error Handling:** Dead letter queue for failed records
- **Transformations:** Kafka record wrapping and timestamp conversion

## Configuration Details

- **Batch Size:** 100 records
- **Max Retries:** 3 with 3-second backoff
- **Insert Mode:** INSERT (no upserts)
- **Error Tolerance:** ALL (with DLQ)
- **Auto Create:** Disabled (tables must exist)

## Redshift Tables

- `streaming.processed_stock_prices_stream`
- `streaming.processed_trading_volume_stream`
- `streaming.processed_technical_indicators_stream`

## Table Structure

Each table has the following structure:
```sql
CREATE TABLE streaming.processed_stock_prices_stream (
    kafka_key VARCHAR(256),
    kafka_value SUPER,  -- JSON data
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    refresh_time TIMESTAMP DEFAULT GETDATE()
);
```

## Environment Variables Required

- `REDSHIFT_ENDPOINT` - Redshift cluster endpoint
- `REDSHIFT_DATABASE` - Redshift database name
- `REDSHIFT_USER` - Redshift username
- `REDSHIFT_PASSWORD` - Redshift password
- `REDSHIFT_PORT` - Redshift port (default: 5439)

## Monitoring

- **Dead Letter Queue:** `redshift-streaming-dlq`
- **Error Logging:** Enabled with message details
- **Metrics:** Available via Kafka Connect REST API

## Prerequisites

1. Redshift cluster must be running
2. Tables must be created using `scripts/database/create_redshift_schemas.sql`
3. Required topics must exist in Kafka
4. Redshift credentials must be configured

## Data Flow

1. Kafka topics receive processed data
2. Connector transforms data to include Kafka metadata
3. Data is inserted into Redshift streaming tables
4. Analytics queries can access real-time data

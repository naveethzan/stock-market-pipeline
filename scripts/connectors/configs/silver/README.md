# Silver Layer Connector

Silver layer connector for storing processed data from Kafka topics to S3 in Parquet format.

## Configuration

**File:** `silver-s3-connector.json`

**Purpose:** Stores processed, cleaned, and validated data from Kafka topics to S3 for analytics and reporting.

## Topics

- `processed-stock-prices` - Processed stock price data
- `processed-trading-volume` - Processed trading volume data
- `processed-technical-indicators` - Processed technical indicators data

## Features

- **Format:** Parquet for efficient analytics
- **Partitioning:** Time-based partitioning (hourly)
- **Compression:** S3 server-side encryption (AES256)
- **Error Handling:** Dead letter queue for failed records
- **Data Quality:** Null value handling and validation

## Configuration Details

- **Partition Duration:** 1 hour (3600000 ms)
- **Flush Size:** 2000 records
- **Rotate Interval:** 2 minutes
- **Schema Compatibility:** BACKWARD_TRANSITIVE
- **Error Tolerance:** ALL (with DLQ)
- **Null Handling:** Ignore null values

## S3 Structure

```
s3://bucket-name/silver/stock-data/
├── year=2024/month=01/day=15/hour=10/
│   ├── processed-stock-prices-2024-01-15-10-00001.parquet
│   ├── processed-trading-volume-2024-01-15-10-00001.parquet
│   └── processed-technical-indicators-2024-01-15-10-00001.parquet
└── year=2024/month=01/day=15/hour=11/
    └── ...
```

## Environment Variables Required

- `AWS_DEFAULT_REGION` - AWS region for S3
- `S3_BUCKET_NAME` - S3 bucket name
- `SCHEMA_REGISTRY_URL` - Schema Registry URL

## Monitoring

- **Dead Letter Queue:** `silver-dlq`
- **Error Logging:** Enabled with message details
- **Metrics:** Available via Kafka Connect REST API

## Data Quality

- Null values are ignored to maintain data quality
- Schema evolution is backward compatible
- Records are validated before storage

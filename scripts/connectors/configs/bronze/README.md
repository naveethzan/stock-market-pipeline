# Bronze Layer Connector

Bronze layer connector for storing raw data from Kafka topics to S3 in Avro format.

## Configuration

**File:** `bronze-s3-connector.json`

**Purpose:** Stores raw, unprocessed data from Kafka topics to S3 for long-term retention and backup.

## Topics

- `stock-quotes-realtime` - Real-time stock quote data
- `stock-intraday-data` - Intraday stock data

## Features

- **Format:** Avro with schema registry integration
- **Partitioning:** Time-based partitioning (hourly)
- **Compression:** S3 server-side encryption (AES256)
- **Error Handling:** Dead letter queue for failed records
- **Retention:** Long-term storage for raw data

## Configuration Details

- **Partition Duration:** 1 hour (3600000 ms)
- **Flush Size:** 1000 records
- **Rotate Interval:** 30 seconds
- **Schema Compatibility:** BACKWARD
- **Error Tolerance:** ALL (with DLQ)

## S3 Structure

```
s3://bucket-name/bronze/streaming-data/
├── year=2024/month=01/day=15/hour=10/
│   ├── stock-quotes-realtime-2024-01-15-10-00001.avro
│   └── stock-intraday-data-2024-01-15-10-00001.avro
└── year=2024/month=01/day=15/hour=11/
    └── ...
```

## Environment Variables Required

- `AWS_DEFAULT_REGION` - AWS region for S3
- `S3_BUCKET_NAME` - S3 bucket name
- `SCHEMA_REGISTRY_URL` - Schema Registry URL

## Monitoring

- **Dead Letter Queue:** `bronze-dlq`
- **Error Logging:** Enabled with message details
- **Metrics:** Available via Kafka Connect REST API

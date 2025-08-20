# Silver Layer S3 Connector

The Silver layer S3 connector stores processed data from Kafka topics to S3 in Parquet format, implementing the second layer of the medallion architecture.

## Overview

- **Purpose**: Store processed, cleaned, and enriched data from Kafka topics to S3
- **Format**: Parquet with Snappy compression (optimized for analytics)
- **Partitioning**: Field-based partitioning by symbol and date
- **Topics**: `processed-stock-prices`, `processed-trading-volume`, `processed-technical-indicators`
- **Destination**: S3 bucket under `silver/stock-data/` prefix

## Configuration

### Key Settings

- **Connector Class**: `io.confluent.connect.s3.S3SinkConnector`
- **Format**: Parquet with Snappy compression
- **Partitioning**: Field-based (by symbol and date)
- **Flush Size**: 2000 records or 5 minutes
- **Compression**: Snappy (built into Parquet)
- **Error Handling**: Dead letter queue (`silver-dlq`)

### S3 Path Structure

```
s3://{bucket}/silver/stock-data/{topic}/symbol={symbol}/date={date}/
```

Example:
```
s3://my-bucket/silver/stock-data/processed-stock-prices/symbol=AAPL/date=2024-08-18/
```

### Environment Variables Required

- `S3_BUCKET_NAME`: Target S3 bucket name
- `AWS_DEFAULT_REGION`: AWS region for S3 bucket
- `AWS_ACCESS_KEY_ID`: AWS access key (or use IAM roles)
- `AWS_SECRET_ACCESS_KEY`: AWS secret key (or use IAM roles)

## Deployment

### 1. Set Environment Variables

```bash
export S3_BUCKET_NAME=your-bucket-name
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

### 2. Deploy Connector

```bash
./scripts/deploy-silver-connector.sh
```

### 3. Verify Deployment

```bash
python scripts/test-silver-connector.py
```

## Monitoring

### Check Connector Status

```bash
python scripts/kafka-connect-manager.py status silver-s3-sink-connector
```

### View Connector Logs

```bash
docker logs kafka-connect | grep silver-s3-sink-connector
```

### Monitor S3 Objects

```bash
aws s3 ls s3://$S3_BUCKET_NAME/silver/stock-data/ --recursive
```

## Data Format

### Parquet Schema

The connector uses Parquet format optimized for analytical queries. Example schema for processed stock prices:

```json
{
  "type": "record",
  "name": "ProcessedStockPrice",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "date", "type": "string"},
    {"name": "open_price", "type": "double"},
    {"name": "high_price", "type": "double"},
    {"name": "low_price", "type": "double"},
    {"name": "close_price", "type": "double"},
    {"name": "volume", "type": "long"},
    {"name": "adjusted_close", "type": "double"},
    {"name": "sma_20", "type": ["null", "double"]},
    {"name": "sma_50", "type": ["null", "double"]},
    {"name": "rsi_14", "type": ["null", "double"]},
    {"name": "processing_timestamp", "type": "long"},
    {"name": "data_quality_score", "type": "double"},
    {"name": "ingestion_layer", "type": "string", "default": "silver"}
  ]
}
```

### Partitioning Strategy

Data is partitioned by:
1. **Symbol**: Stock symbol (e.g., AAPL, GOOGL)
2. **Date**: Trading date (YYYY-MM-DD format)

This partitioning strategy optimizes for:
- Symbol-specific queries
- Date range queries
- Parallel processing by symbol
- Efficient data pruning

### Metadata Fields

The connector adds metadata fields:
- `ingestion_layer`: Set to "silver"
- `processing_timestamp`: When the data was processed
- `data_quality_score`: Quality score from processing pipeline

## Error Handling

### Dead Letter Queue

Failed records are sent to the `silver-dlq` topic with:
- Original processed record data
- Error details in headers
- Timestamp of failure
- Processing context

### Common Issues

1. **Schema Evolution**: Parquet requires careful schema evolution
2. **Partitioning Errors**: Invalid partition field values
3. **S3 Permissions**: Ensure proper IAM permissions
4. **Data Quality**: Invalid processed data format

### Troubleshooting

```bash
# Check connector status
curl http://localhost:8083/connectors/silver-s3-sink-connector/status

# Restart connector
python scripts/kafka-connect-manager.py restart silver-s3-sink-connector

# Check dead letter queue
kafka-console-consumer --bootstrap-server localhost:29092 --topic silver-dlq --from-beginning

# Validate Parquet files
aws s3 cp s3://$S3_BUCKET_NAME/silver/stock-data/processed-stock-prices/symbol=AAPL/date=2024-08-18/file.parquet - | parquet-tools head
```

## Performance Tuning

### Flush Settings

- `flush.size`: Number of records before flush (default: 2000)
- `rotate.interval.ms`: Time-based flush interval (default: 300000ms = 5min)
- `s3.part.size`: S3 multipart upload size (default: 10MB)

### Parallelism

- `tasks.max`: Number of parallel tasks (default: 3)
- Increase for higher throughput
- Consider S3 rate limits and partition distribution

### Compression

- Uses Snappy compression within Parquet format
- Provides good balance of compression ratio and query performance
- Alternative: GZIP for higher compression, LZ4 for faster decompression

### Partitioning Optimization

- Field partitioning by symbol and date
- Enables partition pruning in analytics queries
- Optimal for time-series analysis by symbol

## Analytics Integration

### Query Optimization

The Silver layer is optimized for analytical workloads:

```sql
-- Efficient query with partition pruning
SELECT symbol, avg(close_price) as avg_price
FROM silver_stock_prices 
WHERE symbol = 'AAPL' 
  AND date BETWEEN '2024-08-01' AND '2024-08-31'
GROUP BY symbol;
```

### Data Catalog Integration

- Parquet files include embedded schema
- Compatible with AWS Glue Data Catalog
- Can be queried with Amazon Athena, Spark, Presto

### Schema Evolution

When updating schemas:
1. Use backward-compatible changes only
2. Add new fields as optional (nullable)
3. Test schema compatibility before deployment
4. Update downstream consumers accordingly

## Maintenance

### Partition Management

- Partitions are created automatically based on data
- Monitor partition count to avoid small file problems
- Consider compaction for heavily partitioned data

### Data Lifecycle

- Implement S3 lifecycle policies for cost optimization
- Archive old partitions to cheaper storage classes
- Set up automated cleanup for test data

### Monitoring Checklist

- [ ] Connector status is RUNNING
- [ ] All tasks are RUNNING  
- [ ] No errors in dead letter queue
- [ ] Parquet files are being created with correct partitioning
- [ ] Schema Registry connectivity
- [ ] Data quality scores are within acceptable range
- [ ] Query performance meets SLA requirements
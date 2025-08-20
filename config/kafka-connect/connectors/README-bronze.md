# Bronze Layer S3 Connector

The Bronze layer S3 connector stores raw data from Kafka topics to S3 in Avro format, implementing the first layer of the medallion architecture.

## Overview

- **Purpose**: Store raw, unprocessed data from Kafka topics to S3
- **Format**: Avro (with schema evolution support)
- **Partitioning**: Time-based partitioning by hour
- **Topics**: `stock-quotes-realtime`, `stock-intraday-data`
- **Destination**: S3 bucket under `bronze/stock-data/` prefix

## Configuration

### Key Settings

- **Connector Class**: `io.confluent.connect.s3.S3SinkConnector`
- **Format**: Avro with Schema Registry integration
- **Partitioning**: Time-based (hourly partitions)
- **Flush Size**: 1000 records or 60 seconds
- **Compression**: GZIP
- **Error Handling**: Dead letter queue (`bronze-dlq`)

### S3 Path Structure

```
s3://{bucket}/bronze/stock-data/{topic}/year=YYYY/month=MM/day=dd/hour=HH/
```

Example:
```
s3://my-bucket/bronze/stock-data/stock-quotes-realtime/year=2024/month=08/day=18/hour=14/
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
./scripts/deploy-bronze-connector.sh
```

### 3. Verify Deployment

```bash
python scripts/test-bronze-connector.py
```

## Monitoring

### Check Connector Status

```bash
python scripts/kafka-connect-manager.py status bronze-s3-sink-connector
```

### View Connector Logs

```bash
docker logs kafka-connect | grep bronze-s3-sink-connector
```

### Monitor S3 Objects

```bash
aws s3 ls s3://$S3_BUCKET_NAME/bronze/stock-data/ --recursive
```

## Data Format

### Avro Schema

The connector uses Avro format with schemas managed by Schema Registry. Example schema for stock quotes:

```json
{
  "type": "record",
  "name": "StockQuote",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "volume", "type": "long"},
    {"name": "timestamp", "type": "long"},
    {"name": "exchange", "type": "string"},
    {"name": "ingestion_layer", "type": "string", "default": "bronze"}
  ]
}
```

### Metadata Fields

The connector adds metadata fields:
- `ingestion_layer`: Set to "bronze"
- Kafka metadata (partition, offset) in file names

## Error Handling

### Dead Letter Queue

Failed records are sent to the `bronze-dlq` topic with:
- Original record data
- Error details in headers
- Timestamp of failure

### Common Issues

1. **S3 Permissions**: Ensure proper IAM permissions for S3 bucket
2. **Schema Evolution**: Use backward-compatible schema changes
3. **Network Issues**: Check connectivity to S3 and Schema Registry
4. **Disk Space**: Monitor connector disk usage for buffering

### Troubleshooting

```bash
# Check connector status
curl http://localhost:8083/connectors/bronze-s3-sink-connector/status

# Restart connector
python scripts/kafka-connect-manager.py restart bronze-s3-sink-connector

# Check dead letter queue
kafka-console-consumer --bootstrap-server localhost:29092 --topic bronze-dlq --from-beginning
```

## Performance Tuning

### Flush Settings

- `flush.size`: Number of records before flush (default: 1000)
- `rotate.interval.ms`: Time-based flush interval (default: 60000ms)
- `s3.part.size`: S3 multipart upload size (default: 5MB)

### Parallelism

- `tasks.max`: Number of parallel tasks (default: 3)
- Increase for higher throughput, but consider S3 rate limits

### Compression

- Uses GZIP compression to reduce storage costs
- Can be changed to other formats if needed

## Maintenance

### Schema Evolution

When updating schemas:
1. Ensure backward compatibility
2. Update Schema Registry first
3. Restart connector if needed

### Partition Management

- Partitions are created automatically based on timestamp
- Old partitions can be archived or deleted based on retention policy

### Monitoring Checklist

- [ ] Connector status is RUNNING
- [ ] All tasks are RUNNING
- [ ] No errors in dead letter queue
- [ ] S3 objects are being created
- [ ] Schema Registry connectivity
- [ ] Disk space on Connect workers
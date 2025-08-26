# Snowflake Data Warehouse Integration

This module provides Snowflake integration for the Kafka Connect streaming pipeline, including schema management and monitoring.

## Overview

The Snowflake integration consists of key components for Kafka Connect streaming:

- **SnowflakeClient**: Core client for database operations
- **SchemaManager**: Manages database schemas, tables, and DDL operations for Kafka Connect
- **SnowflakeIntegration**: Orchestrates schema setup and monitoring

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka Topics  │───▶│   Kafka Connect  │───▶│   Snowflake     │
│   (Processed)   │    │   Snowflake Sink │    │   Staging Tables│
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Monitoring    │◀───│   Snowflake      │    │   Dimensional   │
│   & Analytics   │    │   Data Warehouse │    │   Model         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Data Model

The integration implements a dimensional model optimized for Kafka Connect streaming:

### Staging Tables (Kafka Connect Targets)
- **FACT_STOCK_PRICES_STAGING**: Real-time stock price data from Kafka
- **FACT_TRADING_VOLUME_STAGING**: Trading volume metrics from Kafka
- **TECHNICAL_INDICATORS_STAGING**: Technical analysis data from Kafka

### Dimension Tables
- **DIM_COMPANY**: Company information with SCD Type 2
- **DIM_DATE**: Date dimension with business calendar
- **DIM_TIME**: Time dimension with market sessions

### Monitoring Tables
- **STREAMING_OPERATIONS_LOG**: Kafka Connect operation tracking
- **PIPELINE_MONITORING**: Pipeline execution monitoring

## Configuration

### Environment Variables

```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=STOCK_WH
SNOWFLAKE_DATABASE=STOCK_MARKET
SNOWFLAKE_SCHEMA=STREAMING
SNOWFLAKE_ROLE=SYSADMIN
```

### Kafka Connect Configuration

The Snowflake Sink Connector configuration:

```json
{
  "name": "gold-snowflake-sink-connector",
  "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
  "topics": "processed-stock-prices,processed-trading-volume,processed-technical-indicators",
  "snowflake.topic2table.map": "processed-stock-prices:FACT_STOCK_PRICES_STAGING,processed-trading-volume:FACT_TRADING_VOLUME_STAGING,processed-technical-indicators:TECHNICAL_INDICATORS_STAGING"
}
```

## Usage Examples

### Basic Integration

```python
from src.streaming_pipeline.warehouse import SnowflakeIntegration

# Initialize integration
integration = SnowflakeIntegration()

# Set up warehouse schema for Kafka Connect
integration.initialize_warehouse()

# Check Kafka Connect status
kafka_status = integration.get_kafka_connect_status()
print(f"Kafka Connect status: {kafka_status}")

# Get streaming table statistics
table_stats = integration.get_streaming_table_stats()
print(f"Streaming data: {table_stats}")
```

### Individual Components

```python
from src.streaming_pipeline.warehouse import (
    SnowflakeClient, SchemaManager
)

# Use individual components
client = SnowflakeClient()
schema_manager = SchemaManager(client)

# Create tables for Kafka Connect
schema_manager.create_dimension_tables()
schema_manager.create_fact_tables()

# Check table exists
exists = client.check_table_exists("FACT_STOCK_PRICES_STAGING", "STREAMING")
print(f"Staging table exists: {exists}")
```

### Monitoring and Maintenance

```python
# Get pipeline health
health = integration.get_pipeline_health()
print(f"Overall status: {health['overall_status']}")
print(f"Kafka Connect: {health['kafka_connect']}")
print(f"Streaming tables: {health['streaming_tables']}")

# Optimize tables
optimization_results = integration.optimize_tables()

# Clean up old streaming data
cleanup_results = integration.cleanup_old_data(days_to_keep=30)

# Get usage report
usage_report = integration.get_warehouse_usage_report(days=7)
```

## Data Loading Process

1. **Kafka Topics**: Spark processes streaming data and publishes to processed topics
2. **Kafka Connect**: Snowflake Sink Connector continuously consumes from topics
3. **Staging Tables**: Data is inserted into Snowflake staging tables in real-time
4. **Monitoring**: Pipeline health and performance metrics are tracked
5. **Analytics**: Data is available for immediate querying and analysis

## Kafka Connect Data Flow

```
processed-stock-prices topic → FACT_STOCK_PRICES_STAGING table
processed-trading-volume topic → FACT_TRADING_VOLUME_STAGING table  
processed-technical-indicators topic → TECHNICAL_INDICATORS_STAGING table
```

## Table Structure

### Staging Tables

Kafka Connect automatically creates tables with:
- **Data columns**: All fields from Kafka messages
- **Metadata columns**: 
  - `RECORD_METADATA`: Kafka metadata (topic, partition, offset, timestamp)
  - Automatic schema evolution support

### Example Query

```sql
-- Query recent streaming data
SELECT 
    symbol,
    current_price,
    volume,
    RECORD_METADATA:CreateTime as ingestion_time,
    RECORD_METADATA:topic as source_topic
FROM STREAMING.FACT_STOCK_PRICES_STAGING
WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY RECORD_METADATA:CreateTime DESC;
```

## Error Handling

The integration provides error handling for:

- **Connection Failures**: Automatic retry with exponential backoff
- **Kafka Connect Issues**: Status monitoring and alerting
- **Schema Evolution**: Automatic schema updates via Kafka Connect
- **Data Quality**: Monitoring of streaming data patterns

## Monitoring

### Key Metrics
- **Throughput**: Records ingested per second via Kafka Connect
- **Latency**: End-to-end streaming latency (typically 30 seconds)
- **Data Freshness**: Time since last record ingestion
- **Resource Usage**: Snowflake credits and compute usage

### Health Checks
- Kafka Connect connector status
- Recent data ingestion rates
- Table record counts
- Schema evolution events

## Performance Optimization

### Best Practices
1. **Buffer Settings**: Optimize Kafka Connect buffer sizes
2. **Batch Processing**: Configure appropriate batch intervals
3. **Clustering**: Maintain clustering on frequently queried columns
4. **Warehouse Sizing**: Right-size warehouses based on query workload

### Kafka Connect Tuning
- `buffer.count.records`: 1000 (batch size)
- `buffer.size.bytes`: 5MB (buffer size)
- `buffer.flush.time`: 30 seconds (flush interval)

## Testing

Run the test suite:

```bash
# Unit tests
pytest src/streaming_pipeline/warehouse/test_snowflake_integration.py

# Integration tests (requires credentials)
pytest src/streaming_pipeline/warehouse/test_snowflake_integration.py -m integration
```

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Check network connectivity
   - Verify Snowflake credentials
   - Increase timeout settings

2. **Kafka Connect Not Loading**
   - Check connector status via REST API
   - Verify topic names and table mapping
   - Review connector logs

3. **Schema Evolution Issues**
   - Check Kafka Connect schema compatibility settings
   - Verify Avro schema registry connectivity
   - Review schema evolution policies

4. **Performance Issues**
   - Monitor Snowflake warehouse utilization
   - Optimize Kafka Connect buffer settings
   - Check for data skew in partitions

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('src.streaming_pipeline.warehouse').setLevel(logging.DEBUG)
```

Check Kafka Connect status:

```bash
curl http://localhost:8083/connectors/gold-snowflake-sink-connector/status
```

Check Snowflake query history:

```sql
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
AND QUERY_TEXT ILIKE '%STREAMING%'
ORDER BY START_TIME DESC;
```

## Security Considerations

- Use strong Snowflake passwords and MFA
- Implement least-privilege access policies
- Monitor query patterns and access logs
- Regularly rotate credentials
- Use network policies to restrict access

## Maintenance

### Regular Tasks
- Monitor Kafka Connect connector health daily
- Optimize tables weekly
- Clean up old staging data monthly
- Review usage and costs monthly
- Update schemas as needed for new data fields

### Backup and Recovery
- Snowflake provides automatic backups
- Kafka topic retention for replay capability
- Document recovery procedures
- Test disaster recovery scenarios

## Real-time Analytics

With Kafka Connect streaming, you can:

- Query data with ~30 second latency
- Build real-time dashboards
- Set up alerts on streaming data
- Perform continuous analytics
- Join streaming data with historical data

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review Kafka Connect documentation
3. Check Snowflake Kafka Connector documentation
4. Enable debug logging for detailed error information
5. Monitor Kafka Connect REST API endpoints
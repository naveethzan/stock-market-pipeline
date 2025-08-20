# Snowflake Data Warehouse Integration

This module provides comprehensive integration with Snowflake data warehouse for the streaming pipeline, including schema management, S3 staging, and Snowpipe automation.

## Overview

The Snowflake integration consists of several key components:

- **SnowflakeClient**: Core client for database operations
- **SchemaManager**: Manages database schemas, tables, and DDL operations
- **S3StagingManager**: Handles S3 staging operations for data loading
- **SnowpipeManager**: Manages Snowpipe operations for automatic data loading
- **SnowflakeIntegration**: Orchestrates the complete integration workflow

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Spark Stream  │───▶│   S3 Staging     │───▶│   Snowpipe      │
│   Processor     │    │   (Parquet)      │    │   Auto-Ingest   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Monitoring    │◀───│   Snowflake      │◀───│   Dimensional   │
│   & Alerting    │    │   Data Warehouse │    │   Model         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Data Model

The integration implements a dimensional model with the following structure:

### Dimension Tables
- **DIM_COMPANY**: Company information with SCD Type 2
- **DIM_DATE**: Date dimension with business calendar
- **DIM_TIME**: Time dimension with market sessions

### Fact Tables
- **FACT_STOCK_PRICES**: Stock price data with technical indicators
- **FACT_TRADING_VOLUME**: Trading volume metrics

### Utility Tables
- **DATA_QUALITY_RESULTS**: Data quality monitoring
- **PIPELINE_MONITORING**: Pipeline execution tracking
- **LOAD_HISTORY**: Data loading audit trail

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

# AWS S3 Configuration
S3_BUCKET_NAME=your-s3-bucket
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_ROLE_ARN=arn:aws:iam::account:role/snowflake-role
```

### AWS IAM Role Setup

Create an IAM role for Snowflake with the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket/*",
                "arn:aws:s3:::your-bucket"
            ]
        }
    ]
}
```

## Usage Examples

### Basic Integration

```python
from src.streaming_pipeline.warehouse import SnowflakeIntegration
import pandas as pd

# Initialize integration
integration = SnowflakeIntegration()

# Set up warehouse (one-time setup)
integration.initialize_warehouse()

# Load data
stock_data = pd.DataFrame({
    'company_key': [1, 2],
    'date_key': [20240118, 20240118],
    'close_price': [150.0, 2500.0],
    # ... other columns
})

result = integration.load_stock_prices_data(stock_data)
print(f"Loaded {result['records_processed']} records")
```

### Individual Components

```python
from src.streaming_pipeline.warehouse import (
    SnowflakeClient, SchemaManager, S3StagingManager, SnowpipeManager
)

# Use individual components
client = SnowflakeClient()
schema_manager = SchemaManager(client)
s3_staging = S3StagingManager()
snowpipe_manager = SnowpipeManager(client)

# Create tables
schema_manager.create_dimension_tables()
schema_manager.create_fact_tables()

# Upload data to S3
s3_key = s3_staging.upload_dataframe_as_parquet(df, "fact_stock_prices")

# Monitor pipes
health = snowpipe_manager.monitor_pipe_health("STOCK_PRICES_PIPE")
```

### Monitoring and Maintenance

```python
# Get pipeline health
health = integration.get_pipeline_health()
print(f"Overall status: {health['overall_status']}")

# Optimize tables
optimization_results = integration.optimize_tables()

# Clean up old data
cleanup_results = integration.cleanup_old_data(days_to_keep=30)

# Get usage report
usage_report = integration.get_warehouse_usage_report(days=7)
```

## Data Loading Process

1. **Data Preparation**: Spark processes streaming data and prepares it in the dimensional model format
2. **S3 Staging**: Data is uploaded to S3 in Parquet format with proper partitioning
3. **Snowpipe Ingestion**: Snowpipe automatically detects new files and loads them into Snowflake
4. **Data Quality**: Quality checks are performed and results are stored
5. **Monitoring**: Pipeline health and performance metrics are tracked

## File Organization

```
staging/streaming/
├── fact_stock_prices/
│   └── 2024/01/18/
│       └── 09/
│           └── fact_stock_prices_20240118_093000_123456.parquet
├── fact_trading_volume/
└── data_quality_results/

processed/streaming/
└── [same structure after successful loading]

errors/streaming/
└── [failed files with error metadata]
```

## Partitioning Strategy

- **Date Partitioning**: Files are organized by date (YYYY/MM/DD/HH)
- **Table Clustering**: Fact tables are clustered on (company_key, date_key, time_key)
- **Time-based Partitioning**: Snowflake tables use automatic clustering

## Error Handling

The integration provides comprehensive error handling:

- **Connection Failures**: Automatic retry with exponential backoff
- **Data Quality Issues**: Invalid records are quarantined
- **Snowpipe Failures**: Failed files are moved to error directory
- **Schema Evolution**: Backward-compatible schema changes are handled

## Monitoring

### Key Metrics
- **Throughput**: Records processed per second
- **Latency**: End-to-end processing time
- **Error Rates**: Failed loads and data quality issues
- **Resource Usage**: Snowflake credits and compute usage

### Health Checks
- Pipe execution status
- Recent load success rates
- Data freshness
- Storage utilization

## Performance Optimization

### Best Practices
1. **Batch Size**: Optimize Parquet file sizes (50-100MB recommended)
2. **Compression**: Use Snappy compression for balance of speed/size
3. **Clustering**: Maintain clustering on frequently queried columns
4. **Warehouse Sizing**: Right-size warehouses based on workload

### Tuning Parameters
- Spark batch intervals
- Snowpipe refresh frequency
- File format settings
- Connection pooling

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
   - Verify credentials
   - Increase timeout settings

2. **Snowpipe Not Loading**
   - Verify S3 permissions
   - Check notification channel setup
   - Review pipe execution history

3. **Data Quality Failures**
   - Check schema compatibility
   - Validate data types
   - Review transformation logic

4. **Performance Issues**
   - Monitor warehouse utilization
   - Optimize clustering keys
   - Adjust batch sizes

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('src.streaming_pipeline.warehouse').setLevel(logging.DEBUG)
```

Check Snowflake query history:

```sql
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```

## Security Considerations

- Use IAM roles instead of access keys when possible
- Encrypt data in transit and at rest
- Implement least-privilege access policies
- Regularly rotate credentials
- Monitor access patterns

## Maintenance

### Regular Tasks
- Monitor pipe health daily
- Optimize tables weekly
- Clean up old files monthly
- Review usage and costs monthly
- Update schemas as needed

### Backup and Recovery
- Snowflake provides automatic backups
- S3 versioning for staged files
- Document recovery procedures
- Test disaster recovery scenarios

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review Snowflake documentation
3. Check AWS S3 and IAM configurations
4. Enable debug logging for detailed error information
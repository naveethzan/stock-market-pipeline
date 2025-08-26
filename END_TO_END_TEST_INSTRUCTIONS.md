# End-to-End Streaming Pipeline Test Instructions

## Overview

This document provides comprehensive instructions for testing the end-to-end streaming pipeline as specified in task 16 of the project cleanup specification.

**Requirements Addressed:**
- 7.1: Verify that streaming pipeline can start successfully
- 7.2: Confirm data flows from Alpha Vantage through Kafka to Snowflake

## Test Components Validated

✅ **All pipeline components have been validated and are present:**

1. **Project Structure** - All required directories exist
2. **Alpha Vantage Client** - API integration with rate limiting and error handling
3. **Kafka Producers** - Multiple producer implementations with serialization
4. **Spark Processors** - Structured streaming with watermarks and checkpoints
5. **Kafka Connect Configuration** - Bronze, Silver, and Gold connectors configured
6. **Snowflake Integration** - Client, schema manager, and Snowpipe setup
7. **Dimensional Models** - Fact and dimension table definitions
8. **Configuration Files** - Environment variables and Docker configurations
9. **Docker Configuration** - All required Dockerfiles present
10. **Schemas and Serialization** - Avro schemas and registry integration

## Prerequisites

### 1. Docker Environment
```bash
# Ensure Docker Desktop is running
docker --version
docker info

# If Docker is not running, start Docker Desktop application
```

### 2. Environment Configuration
```bash
# Ensure environment file exists with proper values
cp config/.env.streaming.template config/.env

# Update the following critical values in config/.env:
# - ALPHA_VANTAGE_API_KEY=your_actual_api_key
# - SNOWFLAKE_ACCOUNT=your_account
# - SNOWFLAKE_USER=your_username  
# - SNOWFLAKE_PASSWORD=your_password
# - AWS_ACCESS_KEY_ID=your_aws_key
# - AWS_SECRET_ACCESS_KEY=your_aws_secret
```

## Test Execution Steps

### Phase 1: Service Startup and Health Checks

1. **Start all services:**
```bash
# Start the complete streaming pipeline
docker-compose up -d

# Check service status
docker-compose ps
```

2. **Verify service health:**
```bash
# Run the service status checker
python3 check_services_status.py
```

Expected healthy services:
- ✅ Zookeeper (port 2181)
- ✅ Kafka (port 9092)
- ✅ Schema Registry (port 8085)
- ✅ Kafka Connect (port 8083)
- ✅ Spark Master (port 18080)
- ✅ Spark Worker
- ✅ Streaming Producer (port 8081)
- ✅ Streaming Processor (port 8082)
- ✅ Kafka UI (port 8090)

### Phase 2: Alpha Vantage to Kafka Data Flow Test

1. **Verify Alpha Vantage API integration:**
```bash
# Check producer logs
docker-compose logs -f streaming-producer

# Expected: API calls to Alpha Vantage and successful data retrieval
```

2. **Verify Kafka topic creation and data flow:**
```bash
# List Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Expected topics:
# - stock-quotes-realtime
# - stock-intraday-data
# - processed-stock-prices
# - processed-trading-volume
# - processed-technical-indicators
```

3. **Monitor incoming data:**
```bash
# Consume messages from input topics
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-quotes-realtime --from-beginning --max-messages 5

# Expected: JSON messages with stock price data from Alpha Vantage
```

### Phase 3: Spark Structured Streaming Processing Test

1. **Verify Spark processing:**
```bash
# Check Spark processor logs
docker-compose logs -f streaming-processor

# Expected: Structured streaming job startup and processing messages
```

2. **Check Spark UI:**
```bash
# Open Spark Master UI
open http://localhost:18080

# Expected: Running streaming applications with active jobs
```

3. **Verify processed data output:**
```bash
# Consume messages from processed topics
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic processed-stock-prices --from-beginning --max-messages 5

# Expected: Processed and enriched stock data
```

### Phase 4: Kafka Connect to S3 Test

1. **Check Kafka Connect status:**
```bash
# List active connectors
curl -s http://localhost:8083/connectors | jq

# Check connector status
curl -s http://localhost:8083/connectors/bronze-s3-sink/status | jq
curl -s http://localhost:8083/connectors/silver-s3-sink/status | jq
```

2. **Verify S3 data delivery:**
```bash
# Check S3 bucket for data files (requires AWS CLI)
aws s3 ls s3://your-bucket-name/streaming-pipeline/ --recursive

# Expected: Parquet files organized by date and topic
```

### Phase 5: Kafka Connect to Snowflake Test

1. **Check Snowflake connector:**
```bash
# Check Snowflake connector status
curl -s http://localhost:8083/connectors/gold-snowflake-sink/status | jq

# Expected: RUNNING state with no errors
```

2. **Verify Snowflake connection:**
```sql
-- Connect to Snowflake and run:
USE DATABASE STOCK_MARKET;
USE SCHEMA STREAMING;

-- Check if data is arriving
SELECT COUNT(*) FROM FACT_STOCK_PRICES WHERE PRICE_DATE >= CURRENT_DATE;
```

### Phase 6: Dimensional Model Validation

1. **Verify dimensional tables:**
```sql
-- Check dimension tables
SELECT COUNT(*) FROM DIM_STOCK;
SELECT COUNT(*) FROM DIM_DATE;
SELECT COUNT(*) FROM DIM_TIME;

-- Check fact tables
SELECT COUNT(*) FROM FACT_STOCK_PRICES;
SELECT COUNT(*) FROM FACT_TRADING_VOLUME;
```

2. **Validate data quality:**
```sql
-- Sample data quality checks
SELECT 
    symbol,
    COUNT(*) as record_count,
    MIN(price_date) as earliest_date,
    MAX(price_date) as latest_date,
    AVG(close_price) as avg_price
FROM FACT_STOCK_PRICES 
WHERE price_date >= CURRENT_DATE - 7
GROUP BY symbol
ORDER BY record_count DESC;
```

## Automated Test Execution

### Run Complete End-to-End Test
```bash
# Execute the comprehensive test suite
python3 test_end_to_end_streaming_pipeline.py

# This will test all components automatically and generate a report
```

### Expected Test Results
The automated test should verify:

1. ✅ **Service Health** - All services responding
2. ✅ **Alpha Vantage to Kafka** - Data flowing from API to topics
3. ✅ **Spark Processing** - Messages being processed and enriched
4. ✅ **Kafka Connect S3** - Data being delivered to S3 storage
5. ✅ **Kafka Connect Snowflake** - Data being loaded into Snowflake
6. ✅ **Dimensional Model** - Proper data structure in warehouse

## Troubleshooting

### Common Issues and Solutions

1. **Docker Services Not Starting:**
```bash
# Check Docker daemon
docker info

# Restart services
docker-compose down
docker-compose up -d
```

2. **Alpha Vantage API Issues:**
```bash
# Check API key configuration
grep ALPHA_VANTAGE_API_KEY config/.env

# Check rate limiting in logs
docker-compose logs streaming-producer | grep -i "rate"
```

3. **Kafka Connection Issues:**
```bash
# Check Kafka broker health
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Recreate topics if needed
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic stock-quotes-realtime
```

4. **Spark Processing Issues:**
```bash
# Check Spark logs
docker-compose logs streaming-processor

# Check checkpoint directory
ls -la checkpoints/
```

5. **Snowflake Connection Issues:**
```bash
# Test connection manually
python3 -c "
import snowflake.connector
conn = snowflake.connector.connect(
    user='your_user',
    password='your_password', 
    account='your_account'
)
print('Connection successful')
"
```

## Success Criteria

The end-to-end test is considered successful when:

1. ✅ All Docker services are healthy and running
2. ✅ Alpha Vantage API data is flowing to Kafka topics
3. ✅ Spark Structured Streaming is processing data successfully
4. ✅ Kafka Connect is delivering data to both S3 and Snowflake
5. ✅ Dimensional model tables contain recent data
6. ✅ Data quality checks pass
7. ✅ No critical errors in service logs
8. ✅ End-to-end latency is within acceptable limits

## Monitoring and Observability

### Key Metrics to Monitor

1. **Throughput Metrics:**
   - Messages per second through Kafka topics
   - Records processed by Spark per batch
   - Data volume delivered to S3 and Snowflake

2. **Latency Metrics:**
   - End-to-end latency from API to warehouse
   - Spark processing latency
   - Kafka Connect delivery latency

3. **Error Metrics:**
   - API call failures
   - Kafka producer/consumer errors
   - Spark job failures
   - Connector errors

### Monitoring Tools

- **Kafka UI:** http://localhost:8090
- **Spark UI:** http://localhost:18080
- **Kafka Connect REST API:** http://localhost:8083
- **Prometheus Metrics:** http://localhost:9090
- **Grafana Dashboards:** http://localhost:3000

## Test Report Generation

The automated test generates a comprehensive JSON report with:

- Service health status
- Data flow validation results
- Performance metrics
- Error logs and troubleshooting information
- Recommendations for optimization

## Next Steps

After successful end-to-end testing:

1. ✅ Mark task 16 as complete
2. ✅ Proceed to task 17: Performance and functionality validation
3. ✅ Document any issues found and resolutions applied
4. ✅ Update monitoring and alerting based on test results

---

**Note:** This test validates the complete streaming pipeline from data ingestion through to the dimensional model in Snowflake, ensuring all components work together as designed in the cleaned architecture.
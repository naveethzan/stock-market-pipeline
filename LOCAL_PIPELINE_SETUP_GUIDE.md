# 🚀 Local Pipeline Setup & Validation Guide

This guide will help you run the complete streaming pipeline locally and validate each component step by step.

## 📋 **Prerequisites**

### **Required Accounts & Keys:**
- ✅ Alpha Vantage API Key (you have this)
- ✅ Snowflake Account (trial account is fine)
- ✅ Docker Desktop installed

### **System Requirements:**
- Docker Desktop with at least 8GB RAM allocated
- Python 3.8+ (for testing scripts)
- At least 10GB free disk space

## 🏗️ **Step 1: Environment Setup**

### **1.1 Create Environment File**

Copy the template and fill in your credentials:

```bash
cp config/.env.streaming.template config/.env.streaming
```

Edit `config/.env.streaming`:

```bash
# Alpha Vantage API
ALPHA_VANTAGE_API_KEY=your_actual_api_key_here

# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STOCK_MARKET
SNOWFLAKE_SCHEMA=STREAMING
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8085

# Stock Symbols to Track
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA
```

### **1.2 Verify Docker Resources**

```bash
# Check Docker has enough resources
docker system info | grep -E "CPUs|Total Memory"

# Should show at least:
# CPUs: 4
# Total Memory: 8GB+
```

## 🗄️ **Step 2: Snowflake Setup**

### **2.1 Manual Schema Creation**

You need to **manually run** the SQL setup file in Snowflake:

1. **Login to Snowflake Web UI**
2. **Open a new worksheet**
3. **Copy and paste** the entire content from `src/streaming_pipeline/warehouse/streaming_pipeline_setup.sql`
4. **Execute the script** (it will create all tables, views, and sample data)

**Important Notes:**
- The script creates the database `STOCK_MARKET` and schema `STREAMING`
- It populates dimension tables with sample data
- It creates Kafka Connect staging tables
- **No S3 or Snowpipe setup needed** (we removed that)

### **2.2 Verify Snowflake Setup**

Run these queries in Snowflake to verify:

```sql
-- Check database and schemas
SHOW DATABASES LIKE 'STOCK_MARKET';
SHOW SCHEMAS IN DATABASE STOCK_MARKET;

-- Check tables were created
USE DATABASE STOCK_MARKET;
USE SCHEMA STREAMING;
SHOW TABLES;

-- Check sample data
SELECT COUNT(*) FROM DIM_COMPANY;  -- Should show 8 companies
SELECT COUNT(*) FROM DIM_DATE;     -- Should show ~4000 dates
SELECT COUNT(*) FROM DIM_TIME;     -- Should show 1440 time entries

-- Check Kafka Connect staging tables
SELECT COUNT(*) FROM FACT_STOCK_PRICES_STAGING;  -- Should be 0 initially
```

## 🐳 **Step 3: Start Docker Infrastructure**

### **3.1 Start Core Services**

```bash
# Start Kafka, Zookeeper, and Schema Registry
make -f Makefile.streaming-docker deps-up

# Wait for services to be ready (30-60 seconds)
make -f Makefile.streaming-docker health
```

### **3.2 Initialize Schema Registry**

```bash
# Register Avro schemas
make -f Makefile.streaming-docker schema-registry-up
python scripts/init-schema-registry.py
```

### **3.3 Verify Infrastructure**

```bash
# Check all services are running
docker ps

# Should show containers:
# - zookeeper (port 2181)
# - kafka (port 9092) 
# - schema-registry (port 8085)

# Test Kafka connectivity
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## 📊 **Step 4: Start Data Producer**

### **4.1 Build and Start Producer**

```bash
# Build producer container
make -f Makefile.streaming-docker build-producer

# Start producer (will fetch data every 60 seconds)
make -f Makefile.streaming-docker producer-up
```

### **4.2 Validate Producer**

```bash
# Check producer logs
make -f Makefile.streaming-docker producer-logs

# Should see logs like:
# "Successfully published stock quote for AAPL"
# "Successfully published stock quote for GOOGL"

# Check producer health
curl http://localhost:8081/health

# Check topics were created
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
# Should show: stock-quotes-realtime, stock-intraday-data

# Check messages in topics
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-quotes-realtime \
  --from-beginning \
  --max-messages 5
```

## ⚡ **Step 5: Start Spark Processor**

### **5.1 Build and Start Processor**

```bash
# Build processor container
make -f Makefile.streaming-docker build-processor

# Start processor
make -f Makefile.streaming-docker processor-up
```

### **5.2 Validate Processor**

```bash
# Check processor logs
make -f Makefile.streaming-docker processor-logs

# Should see logs like:
# "Stock quotes streaming query started"
# "Successfully wrote batch X to topic processed-stock-prices"

# Check processor health
curl http://localhost:8082/health

# Check processed topics were created
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
# Should show: processed-stock-prices, processed-trading-volume, processed-technical-indicators

# Check processed messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic processed-stock-prices \
  --from-beginning \
  --max-messages 3
```

## 🔗 **Step 6: Setup Kafka Connect**

### **6.1 Start Kafka Connect**

```bash
# Start Kafka Connect service
docker-compose up -d kafka-connect

# Wait for Kafka Connect to be ready
sleep 30

# Check Kafka Connect is running
curl http://localhost:8083/
```

### **6.2 Deploy Snowflake Connector**

```bash
# Deploy the Snowflake sink connector
python scripts/kafka-connect-manager.py create config/kafka-connect/connectors/gold-snowflake-connector.json

# Check connector status
curl http://localhost:8083/connectors/gold-snowflake-sink-connector/status
```

## ✅ **Step 7: End-to-End Validation**

### **7.1 Wait for Data Flow**

```bash
# Wait 5-10 minutes for complete data flow:
# Alpha Vantage → Producer → Kafka → Processor → Kafka → Kafka Connect → Snowflake

# Monitor the pipeline
make -f Makefile.streaming-docker logs
```

### **7.2 Validate in Snowflake**

Run these queries in Snowflake to verify data is flowing:

```sql
-- Check Kafka Connect staging tables have data
SELECT COUNT(*) FROM STREAMING.FACT_STOCK_PRICES_STAGING;
SELECT COUNT(*) FROM STREAMING.FACT_TRADING_VOLUME_STAGING;
SELECT COUNT(*) FROM STREAMING.TECHNICAL_INDICATORS_STAGING;

-- Check recent data ingestion
SELECT * FROM STREAMING.V_KAFKA_CONNECT_MONITORING;

-- View actual streaming data
SELECT 
    RECORD_METADATA:topic as TOPIC,
    RECORD_METADATA:CreateTime as INGESTION_TIME,
    RECORD_CONTENT:symbol as SYMBOL,
    RECORD_CONTENT:current_price as PRICE,
    RECORD_CONTENT:volume as VOLUME,
    RECORD_CONTENT:sma_5min as SMA_5MIN
FROM STREAMING.FACT_STOCK_PRICES_STAGING
ORDER BY RECORD_METADATA:CreateTime DESC
LIMIT 10;
```

### **7.3 Validate Data Quality**

```sql
-- Check data quality
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT RECORD_CONTENT:symbol) as unique_symbols,
    MIN(RECORD_METADATA:CreateTime) as first_record,
    MAX(RECORD_METADATA:CreateTime) as latest_record
FROM STREAMING.FACT_STOCK_PRICES_STAGING;

-- Check for any processing errors
SELECT * FROM STREAMING.STREAMING_OPERATIONS_LOG 
WHERE STATUS = 'ERROR'
ORDER BY CREATED_AT DESC;
```

## 🔍 **Step 8: Troubleshooting**

### **8.1 Common Issues & Solutions**

#### **Producer Not Getting Data:**
```bash
# Check Alpha Vantage API key
curl "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=YOUR_API_KEY"

# Check producer logs for API errors
make -f Makefile.streaming-docker producer-logs | grep -i error
```

#### **Kafka Issues:**
```bash
# Restart Kafka services
make -f Makefile.streaming-docker deps-down
make -f Makefile.streaming-docker deps-up

# Check Kafka logs
docker logs kafka
```

#### **Spark Processor Issues:**
```bash
# Check Spark logs for errors
make -f Makefile.streaming-docker processor-logs | grep -i error

# Restart processor
make -f Makefile.streaming-docker processor-down
make -f Makefile.streaming-docker processor-up
```

#### **Kafka Connect Issues:**
```bash
# Check connector status
curl http://localhost:8083/connectors/gold-snowflake-sink-connector/status

# Check connector logs
docker logs kafka-connect

# Restart connector
curl -X POST http://localhost:8083/connectors/gold-snowflake-sink-connector/restart
```

#### **Snowflake Connection Issues:**
```bash
# Test Snowflake connection
python -c "
import snowflake.connector
conn = snowflake.connector.connect(
    account='your_account',
    user='your_user',
    password='your_password'
)
print('Connection successful!')
conn.close()
"
```

### **8.2 Monitoring Commands**

```bash
# Overall pipeline health
make -f Makefile.streaming-docker health

# Individual service logs
make -f Makefile.streaming-docker producer-logs
make -f Makefile.streaming-docker processor-logs
docker logs kafka-connect

# Kafka topic monitoring
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group streaming-pipeline
```

## 📈 **Step 9: Performance Validation**

### **9.1 Check Throughput**

```sql
-- Check ingestion rate (records per minute)
SELECT 
    DATE_TRUNC('MINUTE', RECORD_METADATA:CreateTime) as minute,
    COUNT(*) as records_per_minute
FROM STREAMING.FACT_STOCK_PRICES_STAGING
WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('MINUTE', RECORD_METADATA:CreateTime)
ORDER BY minute DESC;
```

### **9.2 Check Latency**

```sql
-- Check end-to-end latency
SELECT 
    RECORD_CONTENT:symbol as SYMBOL,
    RECORD_CONTENT:producer_timestamp as PRODUCER_TIME,
    RECORD_METADATA:CreateTime as SNOWFLAKE_TIME,
    DATEDIFF(SECOND, 
        TO_TIMESTAMP(RECORD_CONTENT:producer_timestamp), 
        RECORD_METADATA:CreateTime
    ) as LATENCY_SECONDS
FROM STREAMING.FACT_STOCK_PRICES_STAGING
ORDER BY RECORD_METADATA:CreateTime DESC
LIMIT 10;
```

## 🎯 **Expected Results**

If everything is working correctly, you should see:

1. **Producer**: Fetching data from Alpha Vantage every 60 seconds
2. **Kafka Topics**: Raw data in `stock-quotes-realtime`
3. **Spark Processor**: Processing data every 10 seconds
4. **Processed Topics**: Enriched data in `processed-stock-prices`
5. **Kafka Connect**: Streaming data to Snowflake every 30 seconds
6. **Snowflake**: Real-time data in staging tables with ~1-2 minute latency

## 🛑 **Cleanup**

When you're done testing:

```bash
# Stop all services
make -f Makefile.streaming-docker down

# Clean up Docker resources
docker system prune -f
```

## 🎉 **Success Criteria**

Your pipeline is working correctly when:

- ✅ Producer fetches data from Alpha Vantage
- ✅ Raw data appears in Kafka topics
- ✅ Spark processes and enriches the data
- ✅ Processed data appears in processed topics
- ✅ Kafka Connect streams data to Snowflake
- ✅ Data appears in Snowflake staging tables
- ✅ End-to-end latency is under 2 minutes
- ✅ No errors in any component logs

You now have a fully functional real-time financial data streaming pipeline! 🚀📈
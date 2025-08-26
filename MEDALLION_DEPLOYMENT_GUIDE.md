# 🏗️ Medallion Architecture - Automatic Deployment

## 📊 Current Status
- ✅ **Streaming Processor**: All 10 queries running successfully
- ✅ **Kafka Topics**: All required topics exist (including processed topics)
- ✅ **Configuration**: All credentials already in `config/.env`
- ✅ **Auto-Deployment**: Integrated into `make start` and `make start-mock`

## 🎯 Automatic Integration

The medallion architecture deployment is now **automatically integrated** into the standard startup commands:

- **`make start-mock`**: Starts cluster + deploys medallion architecture
- **`make start`**: Starts cluster + deploys medallion architecture  
- **`make deploy-medallion`**: Deploy connectors independently

## 🚀 Quick Start (No Manual Setup Required!)

### Option 1: Development Mode (Recommended)
```bash
make setup-mock && make start-mock
```

### Option 2: Production Mode  
```bash
make setup && make start
```

**That's it!** The medallion architecture will be deployed automatically.

## 📋 What Happens Automatically

1. **Environment Loading**: Credentials loaded from `config/.env`
2. **Cluster Startup**: Spark cluster with distributed processing
3. **Bronze Layer**: Raw Kafka data → S3 (Avro format)
4. **Silver Layer**: Processed data → S3 (Parquet format)  
5. **Gold Layer**: Analytics data → Snowflake (Dimensional tables)

## 📂 Expected Data Structure (After 5-10 minutes)

### Bronze Layer (S3)
```
s3://stock-market-pipeline-zan/bronze/streaming-data/
├── stock-quotes-realtime/year=2024/month=08/day=25/hour=11/
└── stock-intraday-data/year=2024/month=08/day=25/hour=11/
```

### Silver Layer (S3)  
```
s3://stock-market-pipeline-zan/silver/stock-data/
├── processed-stock-prices/symbol=AAPL/date=2024-08-25/
├── processed-trading-volume/symbol=AAPL/date=2024-08-25/
└── processed-technical-indicators/symbol=AAPL/date=2024-08-25/
```

### Gold Layer (Snowflake)
```sql
-- Database: STOCK_MARKET
-- Schema: STREAMING
FACT_STOCK_PRICES_STAGING
FACT_TRADING_VOLUME_STAGING  
TECHNICAL_INDICATORS_STAGING
```

## 🔧 Troubleshooting

### Common Issues:

1. **AWS Permission Errors**:
   - Verify S3 bucket exists and has proper permissions
   - Check AWS credentials are valid

2. **Snowflake Connection Errors**:
   - Verify Snowflake URL format: `account.region.snowflakecomputing.com`
   - Check database and schema exist
   - Ensure warehouse is running

3. **Connector Failures**:
   - Check logs: `docker logs kafka-connect`
   - Verify topics have data: Check Kafka UI at http://localhost:8090

4. **Schema Registry Issues**:
   - Ensure Schema Registry is healthy: `curl http://localhost:8081/subjects`

## 📊 Monitoring Dashboard

Once deployed, you can monitor the complete pipeline:
- **Kafka UI**: http://localhost:8090 (topics and messages)
- **Spark Master**: http://localhost:8080 (cluster status)
- **Health Endpoint**: http://localhost:8082/health (streaming processor)
- **Kafka Connect**: http://localhost:8083/connectors (connector status)

## 🎉 Success Indicators

You'll know everything is working when:
- ✅ All 3 connectors show "RUNNING" status
- ✅ Files appear in S3 bronze and silver layers
- ✅ Data appears in Snowflake staging tables
- ✅ No errors in connector logs
- ✅ Continuous data flow from streaming processor → Kafka → S3/Snowflake
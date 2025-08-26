# Kafka Streaming Pipeline

A real-time streaming data pipeline that fetches stock market data from Alpha Vantage, streams it through Kafka, processes it with Spark, and stores it in S3 and Snowflake.

## 🏗️ Architecture

```
Alpha Vantage → Streaming Producer → Kafka → Spark Processor → S3/Snowflake
```

## 🚀 Quick Start

### 1. Start Kafka Infrastructure

```bash
# Start Kafka, Zookeeper, and other services
docker-compose up -d

# Verify Kafka is running
docker-compose ps
```

### 2. Set Environment Variables

Copy and configure the environment template:

```bash
cp config/.env.streaming.template config/.env
```

Edit the `.env` file with your configuration:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_STOCK_QUOTES_TOPIC=stock-quotes-realtime
KAFKA_STOCK_INTRADAY_TOPIC=stock-intraday-data

# AWS S3 Configuration
S3_BUCKET_NAME=your-stock-data-bucket
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key

# Alpha Vantage API (for real-time data)
ALPHA_VANTAGE_API_KEY=your-api-key

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
SNOWFLAKE_WAREHOUSE=your-warehouse
```

### 3. Start the Streaming Pipeline

```bash
# Start all streaming services
make -f Makefile.streaming-docker up

# Run the streaming producer
python -m streaming_pipeline.producers.example_usage

# Run the streaming processor
python -m streaming_pipeline.processors.example_usage
```

## 📊 Streaming Components

### Alpha Vantage Producer

Real-time stock data producer that fetches data from Alpha Vantage API:

```bash
# Run the producer with specific symbols
python -m streaming_pipeline.producers.alpha_vantage_producer --symbols AAPL,GOOGL,MSFT
```

### Spark Structured Streaming Processor

Processes streaming data in real-time:

```bash
# Start the streaming processor
python -m streaming_pipeline.processors.stream_processor --output-path /path/to/output
```

### Kafka Connect Integration

Automatically delivers processed data to S3 and Snowflake:

```bash
# Deploy Kafka Connect connectors
./scripts/deploy-medallion-connectors.sh
```

## 🔧 Configuration

### Kafka Topics
- **Real-time Quotes**: `stock-quotes-realtime`
- **Intraday Data**: `stock-intraday-data`
- **Processed Prices**: `processed-stock-prices`
- **Trading Volume**: `processed-trading-volume`

### S3 Storage Structure (Medallion Architecture)
```
s3://your-bucket/
├── bronze/stock-data/
│   └── stock-quotes-realtime/year=2024/month=08/day=18/hour=14/
├── silver/stock-data/
│   └── processed-stock-prices/symbol=AAPL/date=2024-08-18/
└── gold/dimensional/
    ├── fact_stock_prices/
    └── dim_company/
```

### Data Formats

#### Real-time Quote Data (Alpha Vantage)
```json
{
  "01. symbol": "AAPL",
  "02. open": "150.0000",
  "03. high": "152.5000",
  "04. low": "149.0000",
  "05. price": "151.2500",
  "06. volume": "50000000",
  "07. latest trading day": "2024-08-18",
  "08. previous close": "150.5000",
  "09. change": "0.7500",
  "10. change percent": "0.4987%",
  "_metadata": {
    "symbol": "AAPL",
    "request_timestamp": "2024-08-18T12:00:00+00:00",
    "data_source": "alpha_vantage",
    "function": "GLOBAL_QUOTE"
  }
}
```

#### Processed Data (Silver Layer)
```json
{
  "symbol": "AAPL",
  "date": "2024-08-18",
  "time": "09:30:00",
  "open_price": 150.00,
  "high_price": 152.50,
  "low_price": 149.75,
  "close_price": 151.25,
  "volume": 1500000,
  "sma_20": 150.85,
  "sma_50": 149.32,
  "rsi_14": 65.4,
  "processing_timestamp": 1692345600000,
  "data_quality_score": 0.98
}
```

## 🧪 Testing

### Component Testing
```bash
# Test Alpha Vantage producer
python src/streaming_pipeline/producers/test_data_producer.py

# Test Spark processor
python src/streaming_pipeline/processors/test_stream_processor.py

# Test Kafka Connect setup
python scripts/test-kafka-connect-setup.py
```

### Integration Testing
```bash
# Run producer integration test
python src/streaming_pipeline/producers/integration_test.py

# Test end-to-end pipeline
python test_streaming_validation.py
```

### Manual Testing
```bash
# Test producer only
python -m streaming_pipeline.producers.example_usage

# Test processor only
python -m streaming_pipeline.processors.example_usage /tmp/output
```

## 📁 File Structure

```
src/streaming_pipeline/
├── __init__.py
├── config/                 # Configuration management
├── clients/               # Alpha Vantage API client
├── producers/             # Kafka producers
├── processors/            # Spark streaming processors
├── models/                # Data models and transformations
├── warehouse/             # Snowflake integration
├── monitoring/            # Logging and metrics
└── schemas/               # Avro schemas
```

## 🔍 Monitoring

### Logs
- **Development**: Console logging
- **Production**: File + console logging with timestamps

### Consumer Statistics
- Messages processed
- Messages failed
- Processing duration
- Success/failure rates

### S3 Storage
- Data stored successfully
- Storage failures
- File organization and structure

## 🚨 Troubleshooting

### Common Issues

#### Kafka Connection Failed
```bash
# Check if Kafka is running
docker-compose ps

# Check Kafka logs
docker-compose logs kafka

# Verify bootstrap servers
echo $KAFKA_BOOTSTRAP_SERVERS
```

#### S3 Storage Failed
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify S3 bucket exists
aws s3 ls s3://your-bucket-name

# Check environment variables
echo $AWS_ACCESS_KEY_ID
echo $S3_BUCKET_NAME
```

#### No Data Received
```bash
# Check producer logs
python src/kafka_pipeline/main.py --mode batch --symbols AAPL --period 1d

# Check consumer logs
python src/kafka_pipeline/main.py --mode batch-consumer --continuous
```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python -m streaming_pipeline.producers.example_usage
```

## 🔄 Data Flow Verification

### 1. Check Kafka Topics
```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check topic details
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic stock-quotes-realtime
```

### 2. Monitor Messages
```bash
# Consume messages from topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-quotes-realtime --from-beginning
```

### 3. Verify S3 Storage (Medallion Architecture)
```bash
# List Bronze layer objects
aws s3 ls s3://your-bucket/bronze/stock-data/ --recursive

# List Silver layer objects
aws s3 ls s3://your-bucket/silver/stock-data/ --recursive

# Check Snowflake data loading
python scripts/test-gold-connector.py
```

## 🎯 Next Steps

1. **Monitoring**: Enhanced metrics collection and alerting with Prometheus/Grafana
2. **Scaling**: Implement horizontal scaling for producers and processors
3. **Data Quality**: Advanced data validation and quality monitoring
4. **Machine Learning**: Add real-time feature engineering and model inference
5. **Cost Optimization**: Implement data lifecycle policies and resource optimization

## 📚 Dependencies

- **Streaming**: `pyspark`, `confluent-kafka`
- **Data Sources**: `alpha_vantage`
- **Storage**: `boto3` (AWS S3), `snowflake-connector-python`
- **Monitoring**: `prometheus-client`, `structlog`
- **Utilities**: `pandas`, `requests`, `python-dotenv`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

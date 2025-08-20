# Kafka Stock Market Data Pipeline

A complete data pipeline that fetches stock market data, streams it through Kafka, and stores it in AWS S3.

## 🏗️ Architecture

```
Yahoo Finance/Alpha Vantage → Producer → Kafka → Consumer → AWS S3
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

Create a `.env` file in the root directory:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_BATCH_TOPIC=stock-data-batch
KAFKA_STREAM_TOPIC=stock-data-stream

# AWS S3 Configuration
S3_BUCKET_NAME=your-stock-data-bucket
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key

# Alpha Vantage API (for real-time data)
ALPHA_VANTAGE_API_KEY=your-api-key
```

### 3. Test the Pipeline

```bash
# Test all components
python test_pipeline.py

# Run the complete demo
python demo_pipeline.py
```

## 📊 Pipeline Modes

### Producer Modes

#### Batch Data Production
```bash
# Fetch historical data from Yahoo Finance
python src/kafka_pipeline/main.py --mode batch --symbols AAPL GOOGL MSFT --period 1d
```

#### Stream Data Production
```bash
# Fetch real-time data from Alpha Vantage
python src/kafka_pipeline/main.py --mode stream --symbols AAPL GOOGL MSFT --interval 60
```



### Consumer Modes

#### Batch Consumer
```bash
# Option 1: Via main pipeline
python src/kafka_pipeline/main.py --mode batch-consumer --continuous

# Option 2: Direct execution (recommended)
python src/kafka_pipeline/consumers/batch_consumer.py --continuous

# Consume limited messages
python src/kafka_pipeline/consumers/batch_consumer.py --max-messages 100
```

#### Stream Consumer
```bash
# Option 1: Via main pipeline
python src/kafka_pipeline/main.py --mode stream-consumer --continuous

# Option 2: Direct execution (recommended)
python src/kafka_pipeline/consumers/stream_consumer.py --continuous

# Consume limited messages
python src/kafka_pipeline/consumers/stream_consumer.py --max-messages 50
```

### Complete Pipeline

#### End-to-End Data Flow
```bash
# Complete pipeline: produce → consume → S3
python src/kafka_pipeline/main.py --mode complete --symbols AAPL GOOGL MSFT
```

## 🔧 Configuration

### Kafka Topics
- **Batch Topic**: `stock-data-batch` (default)
- **Stream Topic**: `stock-data-stream` (default)

### S3 Storage Structure
```
s3://your-bucket/
├── raw-data/
│   ├── batch/
│   │   └── AAPL/2025/01/15/batch_AAPL_20250115_143022.json
│   └── stream/
│       └── AAPL/2025/01/15/stream_AAPL_20250115_143022.json
```

### Data Formats

#### Batch Data (Yahoo Finance)
```json
{
  "symbol": "AAPL",
  "source": "yahoo_finance",
  "timestamp": "2025-01-15T14:30:22",
  "data_type": "batch",
  "period": "1d",
  "data": [
    {
      "Date": "2025-01-15",
      "Open": 150.25,
      "High": 152.80,
      "Low": 149.90,
      "Close": 151.75,
      "Volume": 45678900
    }
  ]
}
```

#### Stream Data (Alpha Vantage)
```json
{
  "symbol": "AAPL",
  "source": "alpha_vantage",
  "timestamp": "2025-01-15T14:30:22",
  "data_type": "stream",
  "data": {
    "01. symbol": "AAPL",
    "02. open": "150.25",
    "03. high": "152.80",
    "04. low": "149.90",
    "05. price": "151.75",
    "06. volume": "45678900"
  }
}
```

## 🧪 Testing

### Component Testing
```bash
# Test all pipeline components
python test_pipeline.py
```

### Demo Pipeline
```bash
# Run complete end-to-end demo
python demo_pipeline.py
```

### Manual Testing
```bash
# Test producer only
python src/kafka_pipeline/main.py --mode batch --symbols AAPL --period 1d

# Test consumer only (in another terminal)
python src/kafka_pipeline/main.py --mode batch-consumer --continuous
```

## 📁 File Structure

```
src/kafka_pipeline/
├── __init__.py
├── config.py              # Configuration management
├── main.py               # Main pipeline entry point
├── producers/
│   ├── batch_producer.py    # Yahoo Finance batch producer
│   └── stream_producer.py   # Alpha Vantage stream producer
└── consumers/
    ├── base_consumer.py     # Base consumer with S3 integration
    ├── batch_consumer.py    # Batch data consumer (runnable independently)
    └── stream_consumer.py   # Stream data consumer (runnable independently)
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
export PYTHONPATH=src
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from kafka_pipeline.main import run_batch_pipeline
run_batch_pipeline(['AAPL'], '1d')
"
```

## 🔄 Data Flow Verification

### 1. Check Kafka Topics
```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check topic details
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic stock-data-batch
```

### 2. Monitor Messages
```bash
# Consume messages from topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-data-batch --from-beginning
```

### 3. Verify S3 Storage
```bash
# List S3 objects
aws s3 ls s3://your-bucket/raw-data/batch/ --recursive

# Download and inspect a file
aws s3 cp s3://your-bucket/raw-data/batch/AAPL/2025/01/15/batch_AAPL_20250115_143022.json ./sample_data.json
cat sample_data.json
```

## 🎯 Next Steps

1. **Add Data Processing**: Implement data transformation and analytics
2. **Monitoring**: Add metrics collection and alerting
3. **Scaling**: Implement multiple consumer instances
4. **Error Handling**: Add dead letter queues and retry mechanisms
5. **Data Quality**: Implement data validation and quality checks

## 📚 Dependencies

- **Kafka**: `kafka-python`
- **Data Sources**: `yfinance`, `alpha_vantage`
- **Storage**: `boto3` (AWS S3)
- **Utilities**: `pandas`, `requests`, `python-dotenv`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

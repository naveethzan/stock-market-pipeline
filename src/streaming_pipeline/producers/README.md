# AvroDataProducer - Kafka Producer for Streaming Financial Data

The `AvroDataProducer` class is a comprehensive Kafka producer implementation for streaming financial data from Alpha Vantage API. It provides robust error handling, Avro message serialization with Schema Registry integration, and comprehensive logging and metrics tracking.

## Features

- **Alpha Vantage Integration**: Seamlessly integrates with Alpha Vantage API client for real-time stock data
- **Avro Serialization**: Automatic Avro serialization of stock data with Schema Registry integration
- **Error Handling**: Comprehensive error handling with retry logic and graceful degradation
- **Metrics Tracking**: Built-in performance metrics and monitoring capabilities
- **Kafka Integration**: Uses confluent-kafka for high-performance message production
- **Context Manager Support**: Proper resource management with context manager pattern
- **Logging**: Structured logging with correlation IDs and detailed context

## Requirements

The AvroDataProducer satisfies the following requirements from the streaming pipeline specification:

- **Requirement 2.1**: Publishes messages to Kafka topics when data is received from Alpha Vantage
- **Requirement 2.2**: Uses appropriate partitioning strategy based on stock symbol and implements Avro schema for data consistency

## Usage

### Basic Usage

```python
from streaming_pipeline.config.settings import ConfigManager
from streaming_pipeline.clients.alpha_vantage import AlphaVantageClient
from streaming_pipeline.producers.kafka_avro_producer import AvroDataProducer

# Initialize configuration
config = ConfigManager()

# Create Alpha Vantage client
alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)

# Create and use Avro data producer
with AvroDataProducer(config, alpha_vantage_client) as producer:
    # Produce real-time quotes with Avro serialization
    symbols = ['AAPL', 'GOOGL', 'MSFT']
    results = producer.produce_real_time_quotes_avro(symbols)
    
    # Check results
    for symbol, success in results.items():
        print(f"{symbol}: {'Success' if success else 'Failed'}")
```

### Real-time Quote Production

```python
# Produce real-time quotes for multiple symbols with Avro serialization
symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
results = producer.produce_real_time_quotes_avro(symbols)

# Results is a dictionary mapping symbols to success status
successful_count = sum(1 for success in results.values() if success)
print(f"Successfully produced quotes for {successful_count}/{len(symbols)} symbols")
```

### Intraday Data Production

```python
# Produce intraday data with Avro serialization
symbols = ['AAPL', 'GOOGL']
interval = '1min'  # Options: '1min', '5min', '15min', '30min', '60min'
# Note: Intraday data production methods would need to be implemented in AvroDataProducer
```

### Metrics and Monitoring

```python
# Get performance metrics
metrics = producer.get_metrics()
print(f"Messages sent: {metrics['messages']['sent']}")
print(f"Success rate: {metrics['messages']['success_rate']:.2%}")
print(f"Throughput: {metrics['throughput']['messages_per_second']:.2f} msg/sec")
print(f"API requests: {metrics['api']['requests']}")
print(f"API error rate: {metrics['api']['error_rate']:.2%}")
```

## Configuration

The DataProducer uses the `ConfigManager` for configuration. Key configuration options include:

### Kafka Configuration
- `bootstrap_servers`: Kafka broker addresses
- `stock_quotes_topic`: Topic for real-time quotes (default: "stock-quotes-realtime")
- `stock_intraday_topic`: Topic for intraday data (default: "stock-intraday-data")
- `market_events_topic`: Topic for market events (default: "market-events")
- Producer settings: acks, retries, compression, etc.

### Alpha Vantage Configuration
- `api_key`: Alpha Vantage API key
- `rate_limit_per_minute`: API rate limit (default: 5)
- `timeout_seconds`: Request timeout (default: 30)
- `retry_attempts`: Number of retry attempts (default: 3)

## Message Format

### Real-time Quote Message
```json
{
  "01. symbol": "AAPL",
  "02. open": "150.0000",
  "03. high": "152.5000",
  "04. low": "149.0000",
  "05. price": "151.2500",
  "06. volume": "50000000",
  "07. latest trading day": "2025-01-01",
  "08. previous close": "150.5000",
  "09. change": "0.7500",
  "10. change percent": "0.4987%",
  "_metadata": {
    "symbol": "AAPL",
    "request_timestamp": "2025-01-01T12:00:00+00:00",
    "data_source": "alpha_vantage",
    "function": "GLOBAL_QUOTE"
  },
  "_producer_metadata": {
    "producer_timestamp": "2025-01-01T12:00:01+00:00",
    "producer_version": "1.0.0",
    "serialization_format": "json"
  }
}
```

### Intraday Data Message
```json
{
  "Meta Data": {
    "1. Information": "Intraday (1min) open, high, low, close prices and volume",
    "2. Symbol": "AAPL",
    "3. Last Refreshed": "2025-01-01 16:00:00",
    "4. Interval": "1min",
    "5. Output Size": "Compact",
    "6. Time Zone": "US/Eastern"
  },
  "Time Series": {
    "2025-01-01 16:00:00": {
      "1. open": "151.0000",
      "2. high": "151.5000",
      "3. low": "150.8000",
      "4. close": "151.2500",
      "5. volume": "1000000"
    }
  },
  "_metadata": {
    "symbol": "AAPL",
    "interval": "1min",
    "data_points": 100,
    "request_timestamp": "2025-01-01T12:00:00+00:00",
    "data_source": "alpha_vantage",
    "function": "TIME_SERIES_INTRADAY"
  },
  "_producer_metadata": {
    "producer_timestamp": "2025-01-01T12:00:01+00:00",
    "producer_version": "1.0.0",
    "serialization_format": "json"
  }
}
```

## Error Handling

The DataProducer implements comprehensive error handling:

### Alpha Vantage API Errors
- Rate limit handling with exponential backoff
- API quota exceeded handling
- Invalid symbol or API call errors
- Network connectivity issues

### Kafka Producer Errors
- Buffer full errors with retry logic
- Connection failures
- Serialization errors
- Delivery confirmation handling

### Logging
All errors are logged with structured context including:
- Request/response correlation IDs
- Symbol and operation context
- Error details and stack traces
- Performance metrics

## Testing

### Unit Tests
```bash
python src/streaming_pipeline/producers/test_data_producer.py
```

### Integration Tests
```bash
python src/streaming_pipeline/producers/integration_test.py
```

### Example Usage
```bash
python src/streaming_pipeline/producers/example_usage.py
```

## Performance Considerations

- **Batching**: Uses Kafka producer batching for improved throughput
- **Compression**: Supports message compression (snappy, gzip, lz4)
- **Async Delivery**: Non-blocking message production with delivery callbacks
- **Connection Pooling**: Reuses HTTP connections for Alpha Vantage API calls
- **Rate Limiting**: Respects Alpha Vantage API rate limits

## Monitoring and Metrics

The DataProducer provides comprehensive metrics:

- **Message Metrics**: Sent, failed, pending, success rate
- **Throughput Metrics**: Messages per second, bytes sent, runtime
- **API Metrics**: Requests, errors, error rate
- **Performance Metrics**: Latency, queue depth, delivery time

These metrics can be integrated with monitoring systems like Prometheus for alerting and dashboards.

## Dependencies

- `confluent-kafka`: High-performance Kafka client
- `requests`: HTTP client for Alpha Vantage API
- `json`: JSON serialization
- `logging`: Structured logging
- `dataclasses`: Configuration and metrics data structures

## Thread Safety

The DataProducer is designed to be thread-safe for single-producer scenarios. For multi-threaded usage, create separate DataProducer instances per thread or implement additional synchronization.
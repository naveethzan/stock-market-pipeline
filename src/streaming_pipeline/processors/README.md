# Spark Structured Streaming Processors

This module contains Spark Structured Streaming processors for real-time financial data processing.

## Overview

The streaming processor consumes financial data from Kafka topics, applies various transformations and calculations, and outputs the processed data to Parquet format for further analysis.

## Features

- **Kafka Integration**: Consumes data from Kafka topics with configurable settings
- **Data Transformations**: Applies price calculations, moving averages, and technical indicators
- **Fault Tolerance**: Implements checkpointing and watermarking for reliable processing
- **Parquet Output**: Writes processed data to Parquet format with partitioning
- **Monitoring**: Provides query status monitoring and metrics
- **Error Handling**: Comprehensive error handling and logging

## Components

### StreamProcessor

The main class that orchestrates the streaming pipeline:

```python
from streaming_pipeline.processors import StreamProcessor
from streaming_pipeline.config.settings import ConfigManager

# Initialize
config = ConfigManager()
processor = StreamProcessor(config)

# Start processing
query = processor.process_stock_quotes_stream("/path/to/output")

# Monitor
status = processor.get_query_status("stock_quotes")
print(f"Processing rate: {status['processed_rows_per_second']} rows/sec")

# Stop
processor.stop_query("stock_quotes")
processor.close()
```

### Key Methods

- `create_kafka_stream(topic)`: Creates a streaming DataFrame from Kafka
- `parse_kafka_messages(kafka_df)`: Parses JSON messages from Kafka
- `apply_data_transformations(df)`: Applies price calculations and moving averages
- `write_to_parquet(df, output_path, checkpoint_path)`: Writes to Parquet with checkpointing
- `process_stock_quotes_stream(output_path)`: End-to-end processing pipeline

## Data Transformations

The processor applies several transformations to the raw data:

### Price Metrics
- Price change (absolute and percentage)
- Price volatility
- Volume-weighted price
- Market cap indicators

### Moving Averages
- 5-minute simple moving average
- 20-minute simple moving average
- Volume moving averages
- Price trend indicators

### Market Classification
- Trading session (pre-market, regular, after-hours)
- Market cap category (large, medium, small)
- Volume categories

### Technical Indicators (Optional)
- RSI (Relative Strength Index)
- Bollinger Bands
- MACD indicators

## Configuration

The processor uses the ConfigManager for configuration:

```python
# Environment variables
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_STOCK_QUOTES_TOPIC=stock-quotes-realtime
SPARK_CHECKPOINT_LOCATION=/tmp/spark-checkpoints
SPARK_TRIGGER_PROCESSING_TIME=10 seconds
SPARK_WATERMARK_DELAY=1 minute
```

## Output Format

The processor outputs data to Parquet format with the following structure:

```
output_path/
├── symbol=AAPL/
│   ├── trading_session=regular/
│   │   ├── part-00000-xxx.parquet
│   │   └── part-00001-xxx.parquet
│   └── trading_session=after_hours/
│       └── part-00000-xxx.parquet
└── symbol=GOOGL/
    └── trading_session=regular/
        └── part-00000-xxx.parquet
```

## Schema

The output schema includes:

- **Basic Data**: symbol, prices, volume, timestamps
- **Calculated Metrics**: price changes, volatility, moving averages
- **Classifications**: trading session, market cap category
- **Technical Indicators**: RSI, Bollinger Bands (if enabled)
- **Metadata**: Kafka offset, partition, processing timestamps

## Error Handling

The processor includes comprehensive error handling:

- **Kafka Connection Errors**: Automatic retry with exponential backoff
- **Data Quality Issues**: Invalid data filtering and logging
- **Processing Failures**: Checkpointing for recovery
- **Schema Evolution**: Graceful handling of schema changes

## Monitoring

Monitor the streaming pipeline using:

```python
# Get query status
status = processor.get_query_status("stock_quotes")
print(f"Batch ID: {status['batch_id']}")
print(f"Input rate: {status['input_rows_per_second']} rows/sec")
print(f"Processing rate: {status['processed_rows_per_second']} rows/sec")

# Check if query is active
if not query.isActive:
    print(f"Query stopped: {query.exception()}")
```

## Example Usage

See `example_usage.py` for a complete example of running the streaming pipeline:

```bash
python -m streaming_pipeline.processors.example_usage /path/to/output
```

## Testing

Run the tests using pytest:

```bash
pytest src/streaming_pipeline/processors/test_stream_processor.py -v
```

## Performance Tuning

For optimal performance:

1. **Partitioning**: Data is partitioned by symbol and trading session
2. **Checkpointing**: Configure appropriate checkpoint location
3. **Trigger Interval**: Adjust processing time based on latency requirements
4. **Watermarking**: Set appropriate delay for late data handling
5. **Spark Configuration**: Tune memory and core settings based on workload

## Dependencies

- PySpark 3.4.1+
- Kafka integration packages
- Python 3.8+

## Troubleshooting

Common issues and solutions:

1. **Kafka Connection Issues**: Check bootstrap servers and security settings
2. **Checkpoint Errors**: Ensure checkpoint directory is writable
3. **Memory Issues**: Increase driver/executor memory settings
4. **Schema Errors**: Verify Kafka message format matches expected schema
5. **Performance Issues**: Check trigger interval and watermark settings
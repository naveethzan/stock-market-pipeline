# Avro Serialization in Streaming Pipeline

## Overview

The streaming pipeline uses Apache Avro for consistent serialization throughout the medallion architecture. This ensures schema evolution, type safety, and efficient binary serialization across all data layers.

## Architecture

```
Producer (Alpha Vantage) → [Avro] → Kafka (Bronze) → Spark → [Avro] → Kafka (Silver) → Consumers
```

## Serialization Flow

### 1. Bronze Layer (Raw Data)
- **Input**: Alpha Vantage API responses (JSON)
- **Schemas**: `stock_quote`, `intraday_data`
- **Topics**: `stock-quotes-realtime`, `stock-intraday-data`
- **Serialization**: AvroSerializer transforms API responses to Avro binary format

### 2. Silver Layer (Processed Data)
- **Input**: Transformed Spark DataFrames
- **Schemas**: `processed_stock_prices`, `processed_trading_volume`, `processed_technical_indicators`
- **Topics**: `processed-stock-prices`, `processed-trading-volume`, `processed-technical-indicators`
- **Serialization**: StreamProcessor uses AvroSerializer for consistent format

### 3. Data Quality Alerts
- **Schema**: `data_quality_alert`
- **Topic**: `data-quality-alerts`
- **Serialization**: Quality validation results serialized to Avro

## Schema Definitions

### Processed Stock Prices Schema
```json
{
  "type": "record",
  "name": "ProcessedStockPrices",
  "namespace": "com.streaming.pipeline.processed",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "current_price", "type": "double"},
    {"name": "sma_5min", "type": ["null", "double"]},
    {"name": "sma_20min", "type": ["null", "double"]},
    {"name": "price_trend_5min", "type": ["null", "string"]},
    {"name": "processing_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "data_layer", "type": "string", "default": "silver"},
    // ... additional fields
  ]
}
```

### Data Quality Alert Schema
```json
{
  "type": "record", 
  "name": "DataQualityAlert",
  "namespace": "com.streaming.pipeline.quality",
  "fields": [
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "layer", "type": "string"},
    {"name": "rule_name", "type": "string"},
    {"name": "severity", "type": "string"},
    {"name": "message", "type": "string"},
    {"name": "failure_rate", "type": "double"},
    // ... additional fields
  ]
}
```

## Implementation Details

### StreamProcessor Integration

The `StreamProcessor` class integrates Avro serialization:

```python
class StreamProcessor:
    def __init__(self, config, spark_session=None):
        self.avro_serializer = AvroSerializer()
        # ... other initialization
    
    def write_to_kafka_with_validation(self, df, topic, checkpoint_path, data_type):
        # Validates data quality and serializes to Avro
        def serialize_row_to_avro(row_data, data_type):
            if data_type == "stock_prices":
                return self.avro_serializer.serialize_processed_stock_prices(row_data)
            elif data_type == "trading_volume":
                return self.avro_serializer.serialize_processed_trading_volume(row_data)
            # ... other data types
```

### Serialization Methods

The `AvroSerializer` provides type-specific serialization:

- `serialize_processed_stock_prices()` - Silver layer stock price data
- `serialize_processed_trading_volume()` - Silver layer volume data  
- `serialize_processed_technical_indicators()` - Silver layer indicators
- `serialize_data_quality_alert()` - Data quality alerts

### Timestamp Handling

All timestamps are converted to epoch milliseconds for consistency:

```python
# Convert ISO string to epoch millis
if isinstance(timestamp_str, str):
    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    epoch_millis = int(dt.timestamp() * 1000)
```

## Benefits

### 1. Schema Evolution
- Forward and backward compatibility
- Schema Registry integration
- Versioned schema management

### 2. Type Safety
- Compile-time schema validation
- Prevents data corruption
- Clear data contracts

### 3. Performance
- Efficient binary serialization
- Smaller message sizes compared to JSON
- Faster serialization/deserialization

### 4. Consistency
- Same serialization format across all layers
- Unified data processing pipeline
- Simplified consumer implementation

## Schema Registry Integration

### Subject Naming Convention
- `{topic-name}-value` for value schemas
- `{topic-name}-key` for key schemas (if needed)

### Registered Subjects
- `stock-quotes-realtime-value`
- `processed-stock-prices-value`
- `processed-trading-volume-value`
- `processed-technical-indicators-value`
- `data-quality-alerts-value`

## Consumer Implementation

Consumers should use the same Avro schemas for deserialization:

```python
from streaming_pipeline.schemas.avro_serializer import AvroSerializer

serializer = AvroSerializer()
deserialized_data = serializer.deserialize(message_bytes, "processed_stock_prices")
```

## Migration from JSON

The previous JSON serialization approach has been replaced with Avro for:

1. **Better Performance**: Binary format is more efficient
2. **Schema Validation**: Prevents malformed data
3. **Evolution Support**: Schema changes without breaking consumers
4. **Consistency**: Same format throughout the pipeline

## Testing

Run Avro integration tests:

```bash
python src/streaming_pipeline/processors/test_avro_integration.py
```

## Configuration

Schema Registry URL can be configured via environment:

```bash
export SCHEMA_REGISTRY_URL=http://localhost:8085
```

## Troubleshooting

### Common Issues

1. **Schema Not Found**: Ensure schema is registered in Schema Registry
2. **Serialization Errors**: Check data types match schema definitions
3. **Timestamp Issues**: Verify timestamp conversion to epoch millis

### Debugging

Enable debug logging for serialization details:

```python
logging.getLogger('streaming_pipeline.schemas.avro_serializer').setLevel(logging.DEBUG)
```
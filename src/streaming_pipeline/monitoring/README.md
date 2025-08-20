# Streaming Pipeline Monitoring

Comprehensive monitoring and testing framework for the medallion architecture streaming pipeline.

## Overview

This monitoring framework provides:

- **Structured Logging**: Layer-aware logging with correlation ID tracking across Bronze → Silver → Gold
- **Health Checks**: Automated health monitoring for Kafka Connect connectors and pipeline components
- **Data Lineage**: Complete data lineage tracking across medallion architecture layers
- **Metrics Collection**: Performance, quality, and system metrics with Prometheus export

## Components

### 1. Structured Logging (`logger.py`)

Provides layer-aware logging with correlation ID tracking:

```python
from streaming_pipeline.monitoring import PipelineLogger, MedallionLayer

logger = PipelineLogger("my_component")

# Use layer context for automatic tracking
with logger.layer_context(
    layer=MedallionLayer.BRONZE,
    component="data_producer",
    operation="ingest_data",
    metadata={"record_count": 1000}
) as correlation_id:
    # Your processing logic here
    logger.info("Processing data", correlation_id=correlation_id)
```

**Features:**
- Automatic correlation ID generation and tracking
- Layer transition tracking (Bronze → Silver → Gold)
- Structured JSON logging with metadata
- Error context preservation
- Performance timing

### 2. Health Checks (`health_checks.py`)

Monitors Kafka Connect connectors and pipeline health:

```python
from streaming_pipeline.monitoring import KafkaConnectHealthChecker, PipelineHealthChecker

# Check specific connector
connector_checker = KafkaConnectHealthChecker()
result = connector_checker.check_connector_health("bronze-s3-connector")

# Comprehensive pipeline health check
pipeline_checker = PipelineHealthChecker()
health_report = pipeline_checker.run_comprehensive_health_check()
```

**Features:**
- Kafka Connect cluster health monitoring
- Individual connector status checking
- Medallion layer health validation
- Response time tracking
- Continuous monitoring capabilities

### 3. Data Lineage (`lineage.py`)

Tracks data flow across medallion architecture:

```python
from streaming_pipeline.monitoring import DataLineageTracker, MedallionLayer

tracker = DataLineageTracker()

# Track data flow between layers
tracker.track_medallion_flow(
    correlation_id="my-correlation-id",
    source_layer=MedallionLayer.BRONZE,
    target_layer=MedallionLayer.SILVER,
    transformation="data_cleansing",
    component="spark_processor",
    record_count=1000,
    quality_metrics={"completeness": 0.95}
)
```

**Features:**
- Asset registry for data sources and targets
- Cross-layer lineage tracking
- Quality metrics integration
- Lineage graph export for visualization
- Integrity validation

### 4. Metrics Collection (`metrics.py`)

Collects performance and quality metrics:

```python
from streaming_pipeline.monitoring import MetricsCollector, MedallionLayer

collector = MetricsCollector()

# Record processing metrics
collector.record_medallion_processing(
    layer=MedallionLayer.SILVER,
    record_count=1000,
    processing_time_ms=500,
    quality_score=0.95
)

# Export metrics
prometheus_metrics = collector.export_metrics("prometheus")
```

**Features:**
- Counter, gauge, histogram, and timer metrics
- Medallion layer-specific metrics
- System resource monitoring
- Prometheus format export
- Data quality metrics tracking

## Usage Examples

### Basic Integration

```python
from streaming_pipeline.monitoring import (
    PipelineLogger, MedallionLayer, DataLineageTracker, MetricsCollector
)

# Initialize components
logger = PipelineLogger("my_pipeline")
lineage_tracker = DataLineageTracker(logger)
metrics_collector = MetricsCollector(logger)

# Process data with full monitoring
with logger.layer_context(
    layer=MedallionLayer.BRONZE,
    component="data_producer",
    operation="ingest_data"
) as correlation_id:
    
    # Start timing
    timer_id = metrics_collector.start_timer("bronze_processing")
    
    # Your processing logic here
    record_count = process_data()
    
    # Record metrics
    processing_time = metrics_collector.end_timer(timer_id)
    metrics_collector.record_medallion_processing(
        layer=MedallionLayer.BRONZE,
        record_count=record_count,
        processing_time_ms=processing_time
    )
    
    # Track lineage
    lineage_tracker.track_data_flow(
        correlation_id=correlation_id,
        source_asset_ids=["api_source"],
        target_asset_ids=["bronze_topic"],
        transformation="raw_ingestion",
        component="data_producer",
        operation="ingest_data",
        record_count=record_count
    )
```

### Health Monitoring

```python
from streaming_pipeline.monitoring import PipelineHealthChecker

checker = PipelineHealthChecker()

# Run comprehensive health check
health_report = checker.run_comprehensive_health_check()

if health_report['overall_status'] != 'healthy':
    print(f"Pipeline issues detected: {health_report['summary']}")

# Continuous monitoring
checker.monitor_continuously(interval_seconds=60)
```

### Complete Monitoring Dashboard

```python
from streaming_pipeline.monitoring.example_usage import MedallionPipelineMonitor

monitor = MedallionPipelineMonitor()

# Get comprehensive dashboard data
dashboard = monitor.get_monitoring_dashboard()

print(f"Health Status: {dashboard['health_status']['overall_status']}")
print(f"Total Lineage Events: {dashboard['lineage_summary']['total_events']}")
print(f"System CPU: {dashboard['system_metrics']['cpu_percent']}%")
```

## Configuration

### Environment Variables

```bash
# Kafka Connect URL for health checks
KAFKA_CONNECT_URL=http://localhost:8083

# Logging configuration
LOG_LEVEL=INFO
LOG_FORMAT=json

# Metrics configuration
METRICS_EXPORT_INTERVAL=30
PROMETHEUS_PORT=9090
```

### Logging Configuration

The monitoring framework uses structured JSON logging. Configure via `config/logging.yaml`:

```yaml
version: 1
formatters:
  json:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    formatter: json
loggers:
  streaming_pipeline.monitoring:
    level: INFO
    handlers: [console]
```

## Testing

Run the comprehensive test suite:

```bash
# Run all monitoring tests
python -m pytest src/streaming_pipeline/monitoring/test_monitoring.py -v

# Run specific test categories
python -m pytest src/streaming_pipeline/monitoring/test_monitoring.py::TestPipelineLogger -v
python -m pytest src/streaming_pipeline/monitoring/test_monitoring.py::TestKafkaConnectHealthChecker -v
python -m pytest src/streaming_pipeline/monitoring/test_monitoring.py::TestDataLineageTracker -v
python -m pytest src/streaming_pipeline/monitoring/test_monitoring.py::TestMetricsCollector -v
```

## Integration with Existing Pipeline

### 1. Update Producers

```python
# In your Alpha Vantage producer
from streaming_pipeline.monitoring import PipelineLogger, MedallionLayer

logger = PipelineLogger("alpha_vantage_producer")

with logger.layer_context(
    layer=MedallionLayer.BRONZE,
    component="alpha_vantage_producer",
    operation="fetch_stock_data"
) as correlation_id:
    # Your existing producer logic
    pass
```

### 2. Update Spark Processors

```python
# In your Spark processor
from streaming_pipeline.monitoring import PipelineLogger, MedallionLayer, MetricsCollector

logger = PipelineLogger("spark_processor")
metrics = MetricsCollector(logger)

with logger.layer_context(
    layer=MedallionLayer.SILVER,
    component="spark_processor",
    operation="transform_data"
) as correlation_id:
    # Your existing Spark logic
    pass
```

### 3. Update Kafka Connect Monitoring

```python
# Add to your deployment scripts
from streaming_pipeline.monitoring import KafkaConnectHealthChecker

checker = KafkaConnectHealthChecker()

# Check all medallion connectors
results = checker.check_all_medallion_connectors()
for layer, layer_results in results.items():
    for result in layer_results:
        if result.status != 'healthy':
            print(f"Warning: {result.component} is {result.status}")
```

## Metrics Reference

### Medallion Layer Metrics

- `{layer}_records_processed` (counter): Total records processed
- `{layer}_errors` (counter): Total processing errors
- `{layer}_processing_latency_ms` (gauge): Processing latency
- `{layer}_throughput_records_per_sec` (gauge): Processing throughput
- `{layer}_quality_score` (gauge): Data quality score (0-1)

### System Metrics

- `system_cpu_percent` (gauge): CPU utilization
- `system_memory_percent` (gauge): Memory utilization
- `system_disk_percent` (gauge): Disk utilization
- `system_network_bytes_sent` (counter): Network bytes sent
- `system_network_bytes_recv` (counter): Network bytes received

### Health Check Metrics

- `kafka_connect_cluster_health` (gauge): Cluster health status
- `connector_{name}_health` (gauge): Individual connector health
- `pipeline_overall_health` (gauge): Overall pipeline health

## Troubleshooting

### Common Issues

1. **Missing correlation IDs**: Ensure you're using `layer_context()` for automatic tracking
2. **Health check failures**: Verify Kafka Connect URL and network connectivity
3. **Missing lineage**: Register custom assets with `DataLineageTracker.register_asset()`
4. **High memory usage**: Use `clear_old_metrics()` and `clear_lineage_history()` for cleanup

### Debug Logging

Enable debug logging for detailed monitoring information:

```python
import logging
logging.getLogger('streaming_pipeline.monitoring').setLevel(logging.DEBUG)
```

## Performance Considerations

- Metrics collection has minimal overhead (~1-2ms per operation)
- Lineage tracking stores events in memory (configure cleanup intervals)
- Health checks run asynchronously to avoid blocking pipeline
- System monitoring runs in separate thread

## Future Enhancements

- Integration with external monitoring systems (Grafana, DataDog)
- Real-time alerting via webhooks or email
- Advanced lineage visualization
- Machine learning-based anomaly detection
- Custom metric aggregations and dashboards
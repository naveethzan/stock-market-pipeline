# Query Health Monitoring Guide

This guide shows you how to use the `check_query_health()` method to monitor your streaming pipeline and diagnose issues.

## 🏥 What is Query Health Monitoring?

The `check_query_health()` method provides real-time information about all active streaming queries in your pipeline, including:

- **Query Status**: Whether each query is active or failed
- **Progress Metrics**: Input/output rates, batch processing times
- **Error Information**: Detailed exception messages for failed queries
- **Performance Data**: Processing rates and batch durations

## 🚀 Quick Start

### Basic Usage

```python
from src.streaming_pipeline.processors.stream_processor import StreamProcessor
from src.streaming_pipeline.config.settings import ConfigManager

# Initialize your processor
config = ConfigManager()
processor = StreamProcessor(config)

# Start your pipeline
main_query = processor.process_stock_quotes_stream()

# Check health
health_info = processor.check_query_health()

print(f"Active queries: {health_info['active_queries']}/{health_info['total_queries']}")
```

### Health Information Structure

The `check_query_health()` method returns a dictionary with:

```python
{
    "total_queries": 4,           # Total number of queries
    "active_queries": 3,          # Number of active queries
    "failed_queries": 1,          # Number of failed queries
    "query_details": {            # Detailed info for each query
        "query_name": {
            "active": True,       # Whether query is active
            "id": "12345-abcd",   # Spark query ID
            "exception": None,    # Error message if failed
            "last_progress": {    # Progress information
                "batch_id": 42,
                "input_rows_per_second": 150.5,
                "processed_rows_per_second": 148.2,
                "batch_duration": "2.5 seconds"
            }
        }
    }
}
```

## 📊 Monitoring Strategies

### 1. One-Time Health Check

```python
def check_pipeline_status(processor):
    health = processor.check_query_health()
    
    if health['failed_queries'] > 0:
        print("⚠️ Some queries have failed!")
        for name, info in health['query_details'].items():
            if not info.get('active', False):
                print(f"Failed: {name} - {info.get('exception', 'Unknown error')}")
    else:
        print("✅ All queries are healthy!")
```

### 2. Continuous Monitoring

```python
import threading
import time

def continuous_health_monitoring(processor, interval=60):
    def monitor():
        while True:
            health = processor.check_query_health()
            
            # Log health summary
            print(f"Health: {health['active_queries']}/{health['total_queries']} active")
            
            # Alert on failures
            if health['failed_queries'] > 0:
                print(f"🚨 ALERT: {health['failed_queries']} queries failed!")
                # Send alerts, restart queries, etc.
            
            time.sleep(interval)
    
    # Start monitoring in background
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    return monitor_thread
```

### 3. Performance Monitoring

```python
def monitor_performance(processor):
    health = processor.check_query_health()
    
    for query_name, info in health['query_details'].items():
        if info.get('active') and info.get('last_progress'):
            progress = info['last_progress']
            input_rate = progress.get('input_rows_per_second', 0)
            output_rate = progress.get('processed_rows_per_second', 0)
            
            # Check for processing lag
            if input_rate > 0 and output_rate < input_rate * 0.8:
                print(f"⚠️ {query_name} is lagging: {output_rate:.1f} < {input_rate:.1f} rows/sec")
            
            # Check batch duration
            duration = progress.get('batch_duration', '')
            if 'seconds' in duration:
                seconds = float(duration.split()[0])
                if seconds > 10:  # Alert if batch takes > 10 seconds
                    print(f"⚠️ {query_name} slow batch: {duration}")
```

## 🔧 Troubleshooting with Health Checks

### Common Error Patterns and Solutions

```python
def diagnose_and_fix(processor):
    health = processor.check_query_health()
    
    for query_name, info in health['query_details'].items():
        if not info.get('active') and info.get('exception'):
            exception_msg = info['exception'].lower()
            
            if 'kafka' in exception_msg:
                print(f"🔍 Kafka issue in {query_name}")
                print("Solutions:")
                print("- Check if Kafka is running")
                print("- Verify topic exists")
                print("- Check network connectivity")
                
            elif 'avro' in exception_msg:
                print(f"🔍 Avro issue in {query_name}")
                print("Solutions:")
                print("- Check schema registry")
                print("- Verify schema compatibility")
                print("- Check data format")
                
            elif 'checkpoint' in exception_msg:
                print(f"🔍 Checkpoint issue in {query_name}")
                print("Solutions:")
                print("- Clear corrupted checkpoints")
                print("- Check disk space")
                print("- Verify permissions")
```

## 🛠️ Integration Examples

### 1. With Alerting System

```python
import smtplib
from email.mime.text import MIMEText

def setup_health_alerting(processor, email_config):
    def send_alert(message):
        msg = MIMEText(message)
        msg['Subject'] = 'Streaming Pipeline Alert'
        msg['From'] = email_config['from']
        msg['To'] = email_config['to']
        
        with smtplib.SMTP(email_config['smtp_server']) as server:
            server.send_message(msg)
    
    def monitor_with_alerts():
        while True:
            health = processor.check_query_health()
            
            if health['failed_queries'] > 0:
                failed_queries = [
                    name for name, info in health['query_details'].items()
                    if not info.get('active', False)
                ]
                
                alert_message = f"Pipeline Alert: {len(failed_queries)} queries failed: {failed_queries}"
                send_alert(alert_message)
            
            time.sleep(300)  # Check every 5 minutes
    
    threading.Thread(target=monitor_with_alerts, daemon=True).start()
```

### 2. With Metrics Collection

```python
import json
import time

def collect_metrics(processor, metrics_file):
    def collect():
        while True:
            health = processor.check_query_health()
            
            # Create metrics record
            metrics = {
                'timestamp': time.time(),
                'total_queries': health['total_queries'],
                'active_queries': health['active_queries'],
                'failed_queries': health['failed_queries'],
                'query_metrics': {}
            }
            
            # Collect per-query metrics
            for name, info in health['query_details'].items():
                if info.get('last_progress'):
                    progress = info['last_progress']
                    metrics['query_metrics'][name] = {
                        'input_rate': progress.get('input_rows_per_second', 0),
                        'output_rate': progress.get('processed_rows_per_second', 0),
                        'batch_id': progress.get('batch_id', 0)
                    }
            
            # Write to metrics file
            with open(metrics_file, 'a') as f:
                f.write(json.dumps(metrics) + '\n')
            
            time.sleep(60)
    
    threading.Thread(target=collect, daemon=True).start()
```

### 3. With Auto-Recovery

```python
def setup_auto_recovery(processor):
    def auto_recover():
        while True:
            health = processor.check_query_health()
            
            for query_name, info in health['query_details'].items():
                if not info.get('active') and info.get('exception'):
                    exception_msg = info['exception']
                    
                    # Check if error is retryable
                    retryable_errors = [
                        'TimeoutException',
                        'Connection refused',
                        'NetworkException'
                    ]
                    
                    if any(error in exception_msg for error in retryable_errors):
                        print(f"🔄 Attempting to recover {query_name}")
                        
                        # Implement recovery logic here
                        # This might involve:
                        # - Stopping the failed query
                        # - Clearing checkpoints
                        # - Restarting the query
                        
                        # For now, just log the attempt
                        print(f"Recovery needed for {query_name}: {exception_msg}")
            
            time.sleep(120)  # Check every 2 minutes
    
    threading.Thread(target=auto_recover, daemon=True).start()
```

## 📋 Command Line Tools

### Using the Health Check Script

```bash
# One-time health check
python check_pipeline_health.py

# Continuous monitoring
python check_pipeline_health.py --continuous --interval 30

# JSON output for scripting
python check_pipeline_health.py --json

# Verbose output
python check_pipeline_health.py --verbose
```

### Using the Monitoring Script

```bash
# Monitor for 10 minutes
python monitor_streaming_queries.py
```

## 🎯 Best Practices

### 1. Regular Health Checks
- Check health every 30-60 seconds during active processing
- Increase frequency during critical periods
- Reduce frequency during stable operation

### 2. Proactive Monitoring
- Set up alerts for failed queries
- Monitor processing rates and batch durations
- Track trends over time

### 3. Error Handling
- Classify errors by type (retryable vs non-retryable)
- Implement automatic recovery for transient issues
- Log detailed error information for debugging

### 4. Performance Monitoring
- Track input vs output rates
- Monitor batch processing times
- Alert on performance degradation

### 5. Integration
- Integrate with existing monitoring systems
- Export metrics to time-series databases
- Set up dashboards for visualization

## 🚨 Common Issues and Solutions

| Issue | Symptoms | Solutions |
|-------|----------|-----------|
| **Kafka Connectivity** | TimeoutException, Connection refused | Check Kafka status, verify network, check topics |
| **Avro Serialization** | Schema errors, serialization failures | Check schema registry, verify schemas, validate data |
| **Checkpoint Issues** | Checkpoint corruption, permission errors | Clear checkpoints, check permissions, verify disk space |
| **Performance Lag** | Low processing rates, long batch times | Check resources, optimize queries, scale up |
| **Memory Issues** | OutOfMemoryError, GC pressure | Increase memory, optimize batch sizes, check for leaks |

## 📈 Monitoring Dashboard Example

```python
def create_health_dashboard(processor):
    """Create a simple text-based dashboard."""
    
    def refresh_dashboard():
        while True:
            os.system('clear')  # Clear screen
            
            health = processor.check_query_health()
            
            print("🏥 STREAMING PIPELINE DASHBOARD")
            print("=" * 50)
            print(f"Status: {'🟢 HEALTHY' if health['failed_queries'] == 0 else '🔴 ISSUES'}")
            print(f"Active Queries: {health['active_queries']}/{health['total_queries']}")
            print(f"Last Updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Query details
            for name, info in health['query_details'].items():
                status = "🟢" if info.get('active') else "🔴"
                print(f"{status} {name}")
                
                if info.get('last_progress'):
                    progress = info['last_progress']
                    rate = progress.get('processed_rows_per_second', 0)
                    print(f"    Rate: {rate:.1f} rows/sec")
            
            time.sleep(5)  # Refresh every 5 seconds
    
    threading.Thread(target=refresh_dashboard, daemon=True).start()
```

This comprehensive guide should help you effectively monitor your streaming pipeline using the `check_query_health()` method!
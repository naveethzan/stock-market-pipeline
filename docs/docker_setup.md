# Docker Setup for Streaming Pipeline

This document provides comprehensive instructions for setting up and running the streaming pipeline using Docker containers.

## Overview

The streaming pipeline consists of two main containerized components:

1. **Alpha Vantage Data Producer** (`streaming-producer`) - Fetches real-time stock data from Alpha Vantage API and publishes to Kafka
2. **Spark Structured Streaming Processor** (`streaming-processor`) - Consumes data from Kafka, processes it, and outputs to Parquet format

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 8GB RAM available for containers
- Alpha Vantage API key (free tier available)
- Optional: AWS credentials for S3 integration
- Optional: Snowflake credentials for data warehouse integration

## Quick Start

### 1. Environment Setup

```bash
# Copy environment template
cp config/.env.streaming.template config/.env

# Edit configuration with your actual values
nano config/.env
```

**Required Configuration:**
- `ALPHA_VANTAGE_API_KEY` - Your Alpha Vantage API key
- `STOCK_SYMBOLS` - Comma-separated list of stock symbols to track

### 2. Build and Start Services

```bash
# Build containers
make -f Makefile.streaming-docker build

# Start infrastructure dependencies
make -f Makefile.streaming-docker deps-up

# Start streaming services
make -f Makefile.streaming-docker up

# Or start everything at once
make -f Makefile.streaming-docker full-up
```

### 3. Verify Services

```bash
# Check service status
make -f Makefile.streaming-docker status

# Check health endpoints
make -f Makefile.streaming-docker health

# View logs
make -f Makefile.streaming-docker logs
```

## Container Details

### Alpha Vantage Data Producer

**Image:** `streaming-producer:latest`
**Base:** `python:3.9-slim`
**Port:** `8081` (health check endpoint)

**Features:**
- Fetches real-time stock quotes from Alpha Vantage API
- Publishes data to Kafka topics with JSON serialization
- Built-in rate limiting and retry logic
- Health check endpoint for monitoring
- Graceful shutdown handling

**Health Check:**
```bash
curl http://localhost:8081/health
```

**Metrics:**
```bash
curl http://localhost:8081/metrics
```

### Spark Structured Streaming Processor

**Image:** `streaming-processor:latest`
**Base:** `openjdk:11-jre-slim` with Python 3
**Port:** `8082` (health check endpoint)

**Features:**
- Consumes data from Kafka using Spark Structured Streaming
- Applies data transformations and quality checks
- Outputs processed data to Parquet format
- Supports watermarking for late-arriving data
- Built-in query monitoring and restart capabilities

**Health Check:**
```bash
curl http://localhost:8082/health
```

**Query Status:**
```bash
curl http://localhost:8082/queries
```

## Configuration

### Environment Variables

The containers are configured through environment variables defined in `config/.env`:

#### Alpha Vantage Configuration
```env
ALPHA_VANTAGE_API_KEY=your_api_key_here
ALPHA_VANTAGE_RATE_LIMIT=5
ALPHA_VANTAGE_TIMEOUT=30
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA
```

#### Kafka Configuration
```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_STOCK_QUOTES_TOPIC=stock-quotes-realtime
KAFKA_STOCK_INTRADAY_TOPIC=stock-intraday-data
```

#### Spark Configuration
```env
SPARK_MASTER=spark://spark-master:7077
SPARK_CHECKPOINT_LOCATION=/app/checkpoints
SPARK_TRIGGER_PROCESSING_TIME=10 seconds
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

#### Pipeline Configuration
```env
PRODUCTION_INTERVAL_SECONDS=60
OUTPUT_BASE_PATH=/app/data/output
LOG_LEVEL=INFO
```

### Volume Mounts

Both containers use several volume mounts:

- `./logs:/app/logs` - Application logs
- `./data:/app/data` - Data output directory
- `./checkpoints:/app/checkpoints` - Spark checkpoints (processor only)
- `./src:/app/src:ro` - Source code (read-only)
- `./config:/app/config:ro` - Configuration files (read-only)

## Docker Compose Files

### Main Configuration (`docker-compose.streaming.yaml`)
Contains all streaming pipeline services:
- Kafka and Zookeeper
- Spark master and worker
- Schema Registry
- Kafka Connect
- Monitoring stack (Prometheus, Grafana)
- `streaming-producer`
- `streaming-processor`

### Usage
```bash
# Start all streaming services
docker-compose -f docker-compose.streaming.yaml up -d

# Or use the Makefile
make -f Makefile.streaming-docker up
```

## Makefile Commands

The `Makefile.streaming-docker` provides convenient commands:

### Build Commands
```bash
make -f Makefile.streaming-docker build           # Build all containers
make -f Makefile.streaming-docker build-producer  # Build producer only
make -f Makefile.streaming-docker build-processor # Build processor only
```

### Service Management
```bash
make -f Makefile.streaming-docker up              # Start streaming services
make -f Makefile.streaming-docker down            # Stop streaming services
make -f Makefile.streaming-docker restart         # Restart services
make -f Makefile.streaming-docker full-up         # Start everything
make -f Makefile.streaming-docker full-down       # Stop everything
```

### Monitoring and Debugging
```bash
make -f Makefile.streaming-docker logs            # View all logs
make -f Makefile.streaming-docker logs-producer   # Producer logs only
make -f Makefile.streaming-docker health          # Check health
make -f Makefile.streaming-docker status          # Service status
make -f Makefile.streaming-docker shell-producer  # Shell into producer
```

### Development
```bash
make -f Makefile.streaming-docker dev-setup       # Setup dev environment
make -f Makefile.streaming-docker test-producer   # Test producer health
make -f Makefile.streaming-docker clean           # Clean up containers
```

## Monitoring and Health Checks

### Health Endpoints

Both containers expose health check endpoints:

**Producer Health:**
```bash
curl http://localhost:8081/health
```

Response:
```json
{
  "status": "healthy",
  "is_running": true,
  "last_health_check": "2024-01-15T10:30:00Z",
  "error_count": 0,
  "total_runs": 150,
  "metrics": {
    "messages": {
      "sent": 1500,
      "failed": 2,
      "success_rate": 0.998
    },
    "throughput": {
      "messages_per_second": 2.5,
      "runtime_seconds": 600
    }
  }
}
```

**Processor Health:**
```bash
curl http://localhost:8082/health
```

Response:
```json
{
  "status": "healthy",
  "is_running": true,
  "uptime_seconds": 3600,
  "active_queries": 1,
  "healthy_queries": 1,
  "query_statuses": {
    "stock_quotes": {
      "is_active": true,
      "batch_id": 45,
      "input_rows_per_second": 2.1,
      "processed_rows_per_second": 2.0
    }
  }
}
```

### Docker Health Checks

Both containers have built-in Docker health checks:

```bash
# Check container health status
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Logs

Application logs are written to both stdout (for Docker) and log files:

```bash
# Docker logs
docker-compose logs -f streaming-producer
docker-compose logs -f streaming-processor

# Log files
tail -f logs/producer.log
tail -f logs/processor.log
```

## Troubleshooting

### Common Issues

#### 1. Producer Not Starting
```bash
# Check logs
make -f Makefile.streaming-docker logs-producer

# Common causes:
# - Missing ALPHA_VANTAGE_API_KEY
# - Kafka not available
# - Port 8081 already in use
```

#### 2. Processor Failing to Connect to Spark
```bash
# Check Spark master status
curl http://localhost:18080

# Check processor logs
make -f Makefile.streaming-docker logs-processor

# Ensure Spark master is healthy
docker-compose ps spark-master
```

#### 3. Kafka Connection Issues
```bash
# Check Kafka status
docker-compose ps kafka

# Test Kafka connectivity
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

#### 4. Memory Issues
```bash
# Check container resource usage
docker stats

# Adjust memory limits in docker-compose.streaming.yaml
# Or increase Docker Desktop memory allocation
```

### Debug Commands

```bash
# Enter producer container
make -f Makefile.streaming-docker shell-producer

# Enter processor container
make -f Makefile.streaming-docker shell-processor

# Check Python environment
docker-compose exec streaming-producer python -c "import streaming_pipeline; print('OK')"

# Test API connectivity
docker-compose exec streaming-producer python -c "
from streaming_pipeline.clients.alpha_vantage import AlphaVantageClient
from streaming_pipeline.config.loader import load_config
config = load_config()
client = AlphaVantageClient(config.alpha_vantage)
print(client.get_real_time_quote('AAPL'))
"
```

### Performance Tuning

#### Producer Optimization
```env
# Increase batch size for higher throughput
KAFKA_PRODUCER_BATCH_SIZE=32768
KAFKA_PRODUCER_LINGER_MS=20

# Adjust production interval
PRODUCTION_INTERVAL_SECONDS=30
```

#### Processor Optimization
```env
# Increase Spark resources
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=4

# Adjust trigger interval
SPARK_TRIGGER_PROCESSING_TIME=5 seconds
```

## Security Considerations

### API Keys
- Store API keys in environment variables, not in code
- Use Docker secrets for production deployments
- Rotate API keys regularly

### Network Security
- Use custom Docker networks to isolate services
- Implement proper firewall rules for exposed ports
- Consider using TLS for Kafka in production

### Container Security
- Containers run as non-root users
- Use minimal base images
- Regularly update base images for security patches

## Production Deployment

### Resource Requirements
- **Minimum:** 4 CPU cores, 8GB RAM
- **Recommended:** 8 CPU cores, 16GB RAM
- **Storage:** 100GB+ for data and logs

### Scaling
```bash
# Scale producer instances
docker-compose -f docker-compose.streaming.yaml up -d --scale streaming-producer=3

# Scale Spark workers
docker-compose -f docker-compose.streaming.yaml up -d --scale spark-worker=3
```

### Monitoring
- Use Prometheus and Grafana for metrics
- Set up alerting for health check failures
- Monitor resource usage and performance metrics

### Backup and Recovery
- Backup Kafka topics and Spark checkpoints
- Implement data retention policies
- Test disaster recovery procedures

## Integration with CI/CD

### GitHub Actions Example
```yaml
name: Build and Deploy Streaming Pipeline

on:
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Build containers
        run: |
          make -f Makefile.streaming-docker build
          
      - name: Run tests
        run: |
          make -f Makefile.streaming-docker deps-up
          make -f Makefile.streaming-docker up
          sleep 30
          make -f Makefile.streaming-docker health
```

This completes the Docker containerization setup for the streaming pipeline, providing a robust, scalable, and maintainable deployment solution.
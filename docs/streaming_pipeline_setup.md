# Streaming Pipeline Setup Guide

This guide covers the setup and configuration of the real-time streaming data pipeline for financial market data.

## Overview

The streaming pipeline consists of the following components:

- **Alpha Vantage Data Producer**: Ingests real-time stock data from Alpha Vantage API
- **Kafka**: Message streaming platform for data flow
- **Spark Structured Streaming**: Real-time data processing and transformation
- **Snowflake**: Data warehouse with dimensional model
- **Monitoring**: Prometheus and Grafana for observability

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Alpha Vantage API key
- Snowflake account
- AWS S3 bucket (for Snowpipe)

## Quick Start

1. **Clone and setup environment**:
   ```bash
   git clone <repository>
   cd <project-directory>
   make setup-env
   ```

2. **Configure environment variables**:
   Edit the `.env` file with your actual configuration:
   ```bash
   # Required configurations
   ALPHA_VANTAGE_API_KEY=your_actual_api_key
   SNOWFLAKE_ACCOUNT=your_account.region
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   # ... other configurations
   ```

3. **Install dependencies**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   make install-dev
   ```

4. **Start services**:
   ```bash
   make -f Makefile.streaming-docker up
   ```

5. **Kafka topics are created automatically** by the `kafka-topics-init` service

6. **Verify setup**:
   ```bash
   make test
   make check-config
   ```

## Configuration

### Environment Variables

The pipeline uses environment variables for configuration. Key variables include:

#### Alpha Vantage API
- `ALPHA_VANTAGE_API_KEY`: Your Alpha Vantage API key (required)
- `ALPHA_VANTAGE_RATE_LIMIT`: API calls per minute (default: 5)
- `STOCK_SYMBOLS`: Comma-separated list of stock symbols to track

#### Kafka
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: localhost:9092)
- `KAFKA_SECURITY_PROTOCOL`: Security protocol (default: PLAINTEXT)
- `KAFKA_CONSUMER_GROUP_ID`: Consumer group ID (default: streaming-pipeline)

#### Snowflake
- `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE_USER`: Username
- `SNOWFLAKE_PASSWORD`: Password
- `SNOWFLAKE_WAREHOUSE`: Warehouse name
- `SNOWFLAKE_DATABASE`: Database name
- `SNOWFLAKE_SCHEMA`: Schema name

#### Spark
- `SPARK_MASTER`: Spark master URL (default: local[*])
- `SPARK_CHECKPOINT_LOCATION`: Checkpoint directory
- `SPARK_DRIVER_MEMORY`: Driver memory allocation
- `SPARK_EXECUTOR_MEMORY`: Executor memory allocation

### Configuration Files

- `config/.env.streaming.template`: Environment variable template
- `config/logging.yaml`: Logging configuration
- `config/prometheus.yml`: Prometheus monitoring configuration

## Project Structure

```
src/streaming_pipeline/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── settings.py          # Configuration management
│   └── loader.py            # Configuration loading utilities
├── producers/
│   ├── __init__.py
│   └── (future producer modules)
├── processors/
│   ├── __init__.py
│   └── (future processor modules)
├── models/
│   ├── __init__.py
│   └── (future data model modules)
└── utils/
    ├── __init__.py
    └── (future utility modules)

config/
├── .env.streaming.template
├── logging.yaml
└── prometheus.yml

docker/
├── Dockerfile.streaming-producer
├── Dockerfile.streaming-processor
└── (integrated into main docker-compose.yaml)
```

## Development Workflow

### Local Development

1. **Activate virtual environment**:
   ```bash
   source venv/bin/activate
   ```

2. **Start development services**:
   ```bash
   make -f Makefile.streaming-docker up
   ```

3. **Run tests**:
   ```bash
   make test
   ```

4. **Code formatting and linting**:
   ```bash
   make format
   make lint
   ```

### Docker Development

1. **Build images**:
   ```bash
   make -f Makefile.streaming-docker build
   ```

2. **Start all services**:
   ```bash
   make -f Makefile.streaming-docker up
   ```

3. **View logs**:
   ```bash
   make -f Makefile.streaming-docker logs
   ```

4. **Stop services**:
   ```bash
   make -f Makefile.streaming-docker down
   ```

## Monitoring and Observability

### Available Dashboards

- **Kafka UI**: http://localhost:8090 - Kafka cluster monitoring
- **Prometheus**: http://localhost:9090 - Metrics collection
- **Grafana**: http://localhost:3000 - Visualization dashboards (admin/admin)

### Key Metrics

The pipeline exposes the following metrics:

- **Throughput**: Messages processed per second
- **Latency**: End-to-end processing time
- **Error Rates**: Failed messages and API errors
- **Data Quality**: Validation failure rates
- **Resource Usage**: CPU, memory, and disk utilization

### Logging

Logs are structured and written to:
- Console output (INFO level)
- `logs/streaming_pipeline.log` (DEBUG level)
- `logs/streaming_pipeline_errors.log` (ERROR level)
- `logs/streaming_pipeline.json` (JSON format)

## Troubleshooting

### Common Issues

1. **Alpha Vantage API Rate Limiting**:
   - Reduce `ALPHA_VANTAGE_RATE_LIMIT` value
   - Check API quota usage
   - Implement longer retry delays

2. **Kafka Connection Issues**:
   - Verify `KAFKA_BOOTSTRAP_SERVERS` configuration
   - Check if Kafka service is running
   - Validate network connectivity

3. **Snowflake Connection Issues**:
   - Verify credentials and account information
   - Check network connectivity and firewall rules
   - Validate warehouse and database permissions

4. **Spark Processing Issues**:
   - Check checkpoint directory permissions
   - Verify memory allocation settings
   - Review Spark logs for detailed errors

### Debug Commands

```bash
# Check configuration
make check-config

# View service logs
make docker-logs

# List Kafka topics
make list-topics

# Test individual components
python -m streaming_pipeline.config.settings
```

## Production Deployment

### Kubernetes Deployment

For production deployment on Kubernetes:

1. Build and push Docker images to registry
2. Create Kubernetes secrets for sensitive configuration
3. Deploy using provided Kubernetes manifests
4. Configure horizontal pod autoscaling
5. Set up monitoring and alerting

### Security Considerations

- Use Kubernetes secrets for sensitive data
- Enable Kafka SASL/SSL authentication
- Configure Snowflake with service accounts
- Implement network policies for pod communication
- Regular security updates for base images

### Performance Tuning

- Adjust Kafka partition count based on throughput
- Tune Spark memory and core allocation
- Optimize Snowflake clustering keys
- Configure appropriate batch sizes and intervals

## Support

For issues and questions:

1. Check the troubleshooting section
2. Review logs for error details
3. Consult component-specific documentation
4. Create an issue in the project repository
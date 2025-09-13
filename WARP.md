# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is a real-time stock market data streaming pipeline built with Apache Kafka, Spark, and implementing the Medallion Architecture (Bronze/Silver/Gold). The system ingests stock market data from Alpha Vantage API, processes it through distributed Spark streaming, and stores it in S3 and Snowflake for analytics.

## Architecture Overview

The system follows a **Medallion Architecture** pattern:

- **Bronze Layer**: Raw data ingestion from Alpha Vantage API → Kafka topics → S3 (Avro format)
- **Silver Layer**: Cleaned, validated data with technical indicators → processed Kafka topics → S3 (Parquet format)  
- **Gold Layer**: Analytics-ready dimensional model → Snowflake warehouse

**Key Components:**
- **Alpha Vantage Producer** (`src/streaming_pipeline/producers/`) - Ingests stock data with rate limiting and mock mode support
- **Spark Processor** (`src/streaming_pipeline/processors/`) - Distributed stream processing with technical indicators
- **Kafka Connect** - Handles medallion architecture data flow to S3/Snowflake
- **Mock Client** (`src/streaming_pipeline/clients/alpha_vantage_mock.py`) - Unlimited realistic data for development

## Development Modes

The project supports two primary development approaches:

### 1. Mock Mode (Recommended for Development)
```bash
# Setup and start with unlimited mock data
make setup-mock && make start-mock

# Development with instant code reloading
make dev-mock-start
```

**Benefits:** No API limits, realistic data, instant development feedback

### 2. Production Mode (Real API Data)  
```bash
# Setup (requires Alpha Vantage API key in config/.env)
make setup && make start

# Monitor cluster
make status
make logs
```

**Note:** Requires proper API key configuration in `config/.env`

## Essential Commands

### Build & Setup
```bash
make setup              # Production setup with real API
make setup-mock         # Development setup with mock data  
make docker-build       # Build all containers
```

### Running Pipeline
```bash
make start              # Start with real API data
make start-mock         # Start with mock data (recommended)
make dev-mock-start     # Development mode with volume mounts
make stop               # Stop all services
make clean              # Complete cleanup
```

### Monitoring & Health
```bash
make status             # Check cluster health
make logs               # View streaming logs
make troubleshoot       # Debug pipeline issues

# Health endpoints
curl http://localhost:8081/health    # Producer health
curl http://localhost:8082/health    # Processor health
```

### Web Interfaces
- **Kafka UI**: http://localhost:8090 (topic monitoring, message inspection)
- **Spark Master UI**: http://localhost:8080 (cluster resources, job tracking)  
- **Worker UIs**: http://localhost:8181, http://localhost:8182

## Code Structure & Key Files

### Core Pipeline Components
- `src/streaming_pipeline/producers/alpha_vantage_app.py` - Main data producer with health checks
- `src/streaming_pipeline/processors/spark_processor.py` - Distributed stream processor
- `src/streaming_pipeline/clients/alpha_vantage_mock.py` - Mock client for development
- `src/streaming_pipeline/models/schemas.py` - Avro schemas for data contracts

### Configuration
- `docker-compose.yaml` - Main infrastructure setup
- `docker-compose.cluster.yml` - Spark cluster configuration
- `config/.env` - Production environment variables
- `.env.mock` - Mock mode configuration

### Infrastructure Scripts
- `scripts/start-cluster.sh` - Automated cluster startup
- **scripts/deploy-connectors.sh** - Unified connector deployment with enhanced Redshift support
- `scripts/kafka-connect-manager.py` - Connector management utilities

## Development Workflow

### Making Code Changes
```bash
# For instant code reloading (no rebuild needed)
make dev-mock-start

# Edit files in src/ - changes apply immediately
# Monitor with: make logs
```

### Testing Changes
```bash
# Check producer health
curl http://localhost:8081/health

# Verify Kafka topics have data  
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-quotes-realtime --max-messages 5

# Monitor Spark processing
# Visit Spark UI: http://localhost:8080
```

### Schema Changes
- Edit schemas in `src/streaming_pipeline/models/schemas.py`
- Restart services: `make stop && make start-mock`
- Avro schemas support backward-compatible evolution

## Key Development Patterns

### Error Handling
- Services implement health checks and graceful degradation
- Dead letter queues handle malformed data
- Exponential backoff for external API calls

### Data Quality
- Comprehensive validation rules in `src/streaming_pipeline/models/data_quality.py`
- Real-time data quality monitoring via `processed-data-quality-alerts` topic
- Technical indicators calculated in streaming processor

### AWS Glue 5.0 Integration
When working with AWS Glue deployments, follow these patterns:
- Use native Iceberg table format with Glue Data Catalog
- Configure S3 paths following: `s3://bucket/database/table/`
- Use DataFrame.writeTo() API for catalog integration
- Verify table creation with: `spark.sql("SHOW TABLES IN database_name")`

## Testing

### Unit Testing
```bash
# Test individual components
python -m pytest src/streaming_pipeline/clients/test_alpha_vantage.py
python -m pytest src/streaming_pipeline/models/test_dimensional.py
```

### Integration Testing
```bash
# Full pipeline testing with mock data
make start-mock
make logs  # Monitor data flow

# Verify data in topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Environment Files

### Production (.env)
- Requires real Alpha Vantage API key
- AWS credentials for S3/Snowflake connectors
- Production intervals (60+ seconds to respect API limits)

### Mock Development (.env.mock)  
- Pre-configured for immediate use
- 10-second intervals for fast development
- Supports 40+ stock symbols with realistic data

## Troubleshooting

### Common Issues
**Container startup failures**: Check environment variables and API keys
**No data in Kafka**: Verify producer health endpoint and logs  
**Spark connection issues**: Ensure cluster is running with `make status`
**Schema errors**: Check Schema Registry health at http://localhost:8085

### Resource Requirements
- **Memory**: ~6GB distributed across Spark cluster
- **Ports**: 8080-8090, 9092, 18080 (ensure no conflicts)
- **Storage**: Docker volumes for persistent data

## Data Flow Verification

1. **Producer**: Check health at http://localhost:8081/health
2. **Kafka Topics**: Monitor via Kafka UI at http://localhost:8090  
3. **Spark Processing**: View jobs at http://localhost:8080
4. **Output**: Check logs for successful processing events

The system is designed for continuous operation with comprehensive monitoring and self-healing capabilities.

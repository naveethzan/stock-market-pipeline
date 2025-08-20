# Kafka Connect Configuration for Medallion Architecture

This directory contains the configuration files and scripts for setting up Kafka Connect to implement the medallion architecture (Bronze, Silver, Gold layers) for the streaming pipeline.

## Overview

Kafka Connect serves as the data delivery mechanism for our medallion architecture:

- **Bronze Layer**: Raw data from Kafka topics → S3 (Avro format)
- **Silver Layer**: Processed data from Kafka topics → S3 (Parquet format)  
- **Gold Layer**: Dimensional data from Kafka topics → Snowflake

## Files

### Configuration Files

- `kafka-connect.properties` - Main Kafka Connect cluster configuration
- `log4j.properties` - Logging configuration for Kafka Connect
- `dead-letter-queue-topics.sh` - Script to create DLQ topics for error handling

### Connector Configurations

Connector configurations are stored in the `connectors/` subdirectory:

- `bronze-s3-connector.json` - S3 sink for raw data (Bronze layer)
- `silver-s3-connector.json` - S3 sink for processed data (Silver layer)
- `gold-snowflake-connector.json` - Snowflake sink for dimensional data (Gold layer)

## Setup

### 1. Start Kafka Connect

Kafka Connect is automatically started as part of the Docker Compose setup:

```bash
docker-compose up kafka-connect
```

### 2. Verify Setup

Use the test script to verify the setup:

```bash
python scripts/test-kafka-connect-setup.py
```

### 3. Create Connectors

Use the management script to create connectors:

```bash
# Create Bronze layer connector
python scripts/kafka-connect-manager.py create config/kafka-connect/connectors/bronze-s3-connector.json

# Create Silver layer connector  
python scripts/kafka-connect-manager.py create config/kafka-connect/connectors/silver-s3-connector.json

# Create Gold layer connector
python scripts/kafka-connect-manager.py create config/kafka-connect/connectors/gold-snowflake-connector.json
```

## Management

### List Connectors

```bash
python scripts/kafka-connect-manager.py list
```

### Check Connector Status

```bash
python scripts/kafka-connect-manager.py status <connector-name>
```

### Restart Connector

```bash
python scripts/kafka-connect-manager.py restart <connector-name>
```

### Pause/Resume Connector

```bash
python scripts/kafka-connect-manager.py pause <connector-name>
python scripts/kafka-connect-manager.py resume <connector-name>
```

## Error Handling

### Dead Letter Queues

The setup includes dead letter queue topics for error handling:

- `connect-dlq` - General connector errors
- `bronze-dlq` - Bronze layer specific errors
- `silver-dlq` - Silver layer specific errors  
- `gold-dlq` - Gold layer specific errors

### Monitoring

- Kafka Connect REST API: http://localhost:8083
- Kafka UI includes Connect monitoring: http://localhost:8090
- Connector logs are available in Docker logs

## Troubleshooting

### Common Issues

1. **Connector fails to start**
   - Check connector configuration JSON syntax
   - Verify required topics exist
   - Check AWS credentials for S3 connectors
   - Verify Snowflake credentials for Snowflake connector

2. **Data not flowing**
   - Check source topics have data
   - Verify connector is in RUNNING state
   - Check connector task status
   - Review connector logs

3. **Schema evolution issues**
   - Ensure Schema Registry is running
   - Check Avro schema compatibility
   - Verify converter configurations

### Useful Commands

```bash
# Check connector plugins
curl http://localhost:8083/connector-plugins

# Get connector status
curl http://localhost:8083/connectors/<connector-name>/status

# Get connector configuration
curl http://localhost:8083/connectors/<connector-name>/config

# Restart connector
curl -X POST http://localhost:8083/connectors/<connector-name>/restart
```
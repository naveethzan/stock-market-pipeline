# Docker Configuration

This directory contains all Docker-related configuration files organized for better maintainability.

## Directory Structure

```
docker/
├── services/           # Individual service Dockerfiles
│   ├── Dockerfile.kafka-connect      # Kafka Connect service
│   ├── Dockerfile.spark-base         # Base Spark image
│   ├── Dockerfile.spark-worker       # Spark worker configuration
│   ├── Dockerfile.streaming-processor # Main Spark streaming processor
│   └── Dockerfile.streaming-producer # Data producer service
├── compose/            # Docker Compose configurations
│   ├── docker-compose.yaml          # Core services (Kafka, Schema Registry, etc.)
│   └── docker-compose.cluster.yml   # Spark cluster services
└── README.md           # This file
```

## Services

### Core Services (`docker/services/`)

- **`Dockerfile.kafka-connect`**: Kafka Connect service for data ingestion
- **`Dockerfile.spark-base`**: Base Spark image with common dependencies
- **`Dockerfile.spark-worker`**: Spark worker node configuration
- **`Dockerfile.streaming-processor`**: Main Spark streaming processor
- **`Dockerfile.streaming-producer`**: Data producer service (Alpha Vantage API)

### Compose Files (`docker/compose/`)

- **`docker-compose.yaml`**: Core infrastructure services (Kafka, Zookeeper, Schema Registry, etc.)
- **`docker-compose.cluster.yml`**: Spark cluster services (Master, Workers, Streaming Processor)

## Usage

### Development
```bash
# Start core services
docker-compose -f docker/compose/docker-compose.yaml up -d

# Start with Spark cluster
docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml up -d
```

### Production
```bash
# Use the Makefile commands which reference the new paths
make start-dev
make start-prod
```

## Benefits of This Structure

1. **Separation of Concerns**: Services and compose files are clearly separated
2. **Easier Maintenance**: Related files are grouped together
3. **Scalability**: Easy to add new services or compose files
4. **Clean Root**: Root directory is cleaner and more professional
5. **Documentation**: Clear structure with README documentation

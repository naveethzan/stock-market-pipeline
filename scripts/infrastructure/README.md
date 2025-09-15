# Infrastructure Scripts

Scripts for managing infrastructure components (Docker, Spark, Kafka).

## Scripts

### `start-cluster.sh`
Starts the complete streaming pipeline with Spark cluster mode.
- Starts infrastructure services (Zookeeper, Kafka, Schema Registry)
- Initializes topics and schemas
- Starts Spark Master and Workers
- Starts application services (Producer, Processor, Kafka Connect)
- Deploys connectors automatically

**Usage:** Called by `make start-dev` or `make start-prod`

### `ultra-fast-build.sh`
Docker build optimization with Spark base caching.
- Uses Bitnami Spark base image for faster builds
- Implements build caching to avoid re-downloading Spark
- Builds all services in parallel
- Reduces build time from 30+ minutes to 2-3 minutes

**Usage:** Called by `make setup-dev` or `make setup-prod`

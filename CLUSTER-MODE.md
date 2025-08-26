# Spark Cluster Mode for Distributed Streaming

## Overview

The Spark Cluster Mode provides distributed processing capabilities for the stock market streaming pipeline, resolving resource conflicts that occur when running 10 parallel streaming queries in local mode.

## Problem Solved

**Original Issue**: Running 10 streaming queries simultaneously in local mode caused:
- `InterruptedException` errors
- Resource conflicts between queries
- Kafka write aborts
- Batch processing failures (all queries showing `batch_id=-1`)

**Solution**: Distribute queries across multiple Spark workers in a cluster configuration.

## Quick Start

```bash
# Setup cluster configuration
make cluster-setup

# Start with mock data (recommended for testing)
make cluster-mock-start

# Start with real API data
make cluster-start

# Monitor cluster health
make cluster-status

# Stop cluster
make cluster-stop
```

## Architecture

### Cluster Components
- **Spark Master**: Coordinates job distribution (1 instance)
- **Spark Workers**: Execute distributed tasks (2 instances)
- **Streaming Processor**: Acts as Spark driver submitting jobs

### Resource Allocation
- **Total Memory**: ~6GB distributed across cluster
- **Master**: 512MB RAM, 0.5 CPU
- **Worker 1**: 1GB RAM, 1 CPU
- **Worker 2**: 1GB RAM, 1 CPU
- **Driver**: 1GB RAM, 1 CPU

## Monitoring

### Web UIs
- **Spark Master UI**: http://localhost:8080 - Cluster overview and job tracking
- **Worker 1 UI**: http://localhost:8081 - Worker 1 details and tasks
- **Worker 2 UI**: http://localhost:8082 - Worker 2 details and tasks
- **Kafka UI**: http://localhost:8090 - Topic and message monitoring

### Health Checks
```bash
# Check cluster status
make cluster-status

# Monitor streaming logs
make logs

# Check individual containers
docker logs spark-master -f
docker logs spark-worker-1 -f
docker logs streaming-processor -f
```

## Configuration Files

### 1. docker-compose.cluster.yml
Defines the Spark cluster infrastructure with master and worker services.

### 2. spark-cluster/spark-defaults.conf
EMR-aligned Spark configuration optimized for:
- Distributed streaming processing
- Memory management
- Kafka integration
- Performance tuning

### 3. scripts/start-cluster.sh
Automated startup script that:
- Creates necessary network
- Starts services in correct order
- Performs health checks
- Provides monitoring endpoints

### 4. emr/cluster-config.json
Amazon EMR deployment template for production migration.

## Benefits

### Performance
- ✅ **Distributed Processing**: Queries run across multiple workers
- ✅ **Resource Isolation**: No more resource conflicts between queries
- ✅ **Better Throughput**: Improved data processing rates
- ✅ **Scalability**: Easy to add more workers

### Production Readiness
- ✅ **EMR Alignment**: Configuration matches Amazon EMR settings
- ✅ **Fault Tolerance**: Built-in Spark fault tolerance mechanisms
- ✅ **Monitoring**: Comprehensive cluster and job monitoring
- ✅ **Best Practices**: Industry-standard distributed processing patterns

## Troubleshooting

### Common Issues

1. **Workers not registering with master**
   ```bash
   # Check network connectivity
   docker network ls
   docker logs spark-master
   ```

2. **Memory issues**
   ```bash
   # Check resource allocation
   make cluster-status
   docker stats
   ```

3. **Streaming queries not processing**
   ```bash
   # Check driver logs
   docker logs streaming-processor -f
   
   # Check Spark Master UI for job status
   # http://localhost:8080
   ```

### Health Check Commands
```bash
# Verify all services are running
make cluster-status

# Check individual service health
curl http://localhost:8080  # Master UI
curl http://localhost:8081  # Worker 1 UI
curl http://localhost:8082  # Worker 2 UI
```

## Migration to Production (EMR)

The cluster configuration is designed to seamlessly migrate to Amazon EMR:

1. **Configuration Alignment**: `spark-defaults.conf` matches EMR settings
2. **Resource Sizing**: Uses EMR m5.xlarge equivalent resource allocation
3. **Bootstrap Scripts**: EMR deployment templates included
4. **Production Patterns**: Follows AWS best practices for Spark on EMR

### EMR Deployment
```bash
# Use the provided EMR template
aws emr create-cluster --cli-input-json file://emr/cluster-config.json
```

## Commands Reference

| Command | Description |
|---------|-------------|
| `make cluster-setup` | Setup cluster configuration files |
| `make cluster-mock-start` | Start cluster with mock data |
| `make cluster-start` | Start cluster with real API data |
| `make cluster-stop` | Stop cluster services |
| `make cluster-status` | Check cluster health |
| `make logs` | View streaming logs |
| `make clean-dev-data` | Clean cluster data for fresh start |

---

**Next Steps**: After successful testing in cluster mode, use the EMR configuration to deploy to AWS for production workloads.
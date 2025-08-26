# Spark Worker Health Check Fix

## Problem

The Spark worker containers (`spark-worker-1` and `spark-worker-2`) were marked as unhealthy in Docker because the health check was using `curl` to check the worker web UI, but `curl` was not installed in the base `bitnami/spark:3.5.1` image.

## Solution

Two approaches were considered:

1. **Process-based health check**: Replace the `curl`-based health check with a process-based check that verifies if the Spark worker process is running.
   ```yaml
   healthcheck:
     test: ["CMD-SHELL", "ps aux | grep 'org.apache.spark.deploy.worker.Worker' | grep -v grep || exit 1"]
     interval: 30s
     timeout: 10s
     retries: 3
   ```

2. **Custom Dockerfile with curl installed**: Create a custom Dockerfile that extends the base `bitnami/spark:3.5.1` image and installs `curl`.
   ```dockerfile
   # Dockerfile.spark-worker
   FROM bitnami/spark:3.5.1
   
   USER root
   
   # Install curl for health checks
   RUN apt-get update && \
       apt-get install -y curl && \
       apt-get clean && \
       rm -rf /var/lib/apt/lists/*
   
   # Switch back to non-root user
   USER 1001
   ```

## Implementation

The second approach was chosen as it provides a more robust solution:

1. Created a custom `Dockerfile.spark-worker` that installs `curl`
2. Updated `docker-compose.cluster.yml` to use this custom Dockerfile for all Spark components (master and workers)
3. Kept the original `curl`-based health checks that verify the web UI is accessible

## Benefits

- More reliable health checks that verify the web UI is accessible, not just that the process is running
- Consistent approach across all Spark components
- Minimal changes to the existing configuration
- Better alignment with Docker health check best practices

## Usage

To apply these changes, rebuild and restart the Spark containers:

```bash
docker-compose -f docker-compose.cluster.yml up -d --build spark-master spark-worker-1 spark-worker-2
```

Verify that the health checks are now passing:

```bash
docker ps | grep spark
```
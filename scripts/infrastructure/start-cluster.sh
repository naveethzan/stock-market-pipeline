#!/bin/bash

# Spark Cluster Startup Script
# Starts the complete streaming pipeline with Spark cluster mode

set -e

echo "🚀 Starting Stock Market Pipeline (OPTIMIZED Spark Cluster)"
echo "========================================================"
echo "⚡ Optimizations: Parallel startup, reduced wait times, better health checks"

# Load environment variables from config/.env if it exists
if [ -f "config/.env" ]; then
    echo "📁 Loading environment variables from config/.env..."
    set -a  # automatically export all variables
    source config/.env
    set +a  # stop automatically exporting
    echo "✅ Environment variables loaded"
else
    echo "⚠️  config/.env not found, using environment variables or defaults"
fi

# Clean up any existing containers to avoid conflicts
echo ""
echo "🧹 Cleaning up existing containers..."
docker-compose -f docker/compose/docker-compose.yaml down --remove-orphans 2>/dev/null || true
docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml down --remove-orphans 2>/dev/null || true
echo "✅ Cleanup complete"

# Function to check if a service is healthy
check_service_health() {
    local service_name=$1
    local health_url=$2
    local max_attempts=20
    local attempt=1
    
    echo "⏳ Waiting for $service_name to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s --connect-timeout 5 --max-time 10 "$health_url" > /dev/null 2>&1; then
            echo "✅ $service_name is healthy"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 3
        ((attempt++))
    done
    
    echo "❌ $service_name failed to become healthy after $max_attempts attempts"
    echo "   Check logs: docker logs $service_name"
    return 1
}

# Create network if it doesn't exist
echo "🔗 Creating Docker network..."
docker network create streaming-network 2>/dev/null || echo "   Network already exists"

# Start infrastructure services first (optimized parallel startup)
echo "🏗️  Starting infrastructure services in parallel..."
docker-compose -f docker/compose/docker-compose.yaml up -d zookeeper &
echo "📊 Starting Kafka and Schema Registry..."
docker-compose -f docker/compose/docker-compose.yaml up -d kafka schema-registry &

echo "⏳ Waiting for infrastructure services..."
wait  # Wait for infrastructure services
sleep 15  # Give infrastructure time to start

# Start initialization services
echo "📋 Initializing topics and schemas..."
docker-compose -f docker/compose/docker-compose.yaml up kafka-topics-init schema-registry-init
echo "✅ Kafka topics and schemas initialized"

# Start Spark Master and wait for it to be ready
echo "⚡ Starting Spark Master..."
docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml up -d spark-master
sleep 10  # Give Spark Master time to start

# Wait for Spark Master to be ready
check_service_health "Spark Master" "http://localhost:8080"

# Start Spark workers
echo "👷 Starting Spark workers..."
docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml up -d spark-worker-1 spark-worker-2

# Wait for workers to register
echo "⏳ Waiting for workers to register with master..."
sleep 15

# Check worker health in parallel
check_service_health "Spark Worker 1" "http://localhost:8181" &
check_service_health "Spark Worker 2" "http://localhost:8182" &
wait
echo "✅ All Spark workers are healthy"

# Start application services with optimized timing
echo "📈 Starting producer and kafka-connect in parallel..."
docker-compose -f docker/compose/docker-compose.yaml up -d streaming-producer &
docker-compose -f docker/compose/docker-compose.yaml up -d kafka-connect &

# Wait for both to start
wait
echo "✅ Application services starting..."

# Check producer health (reduced wait time)
check_service_health "Producer" "http://localhost:8081/health"
echo "⏳ Waiting for producer to prime data (reduced timing - 45s)..."
sleep 45  # Optimized timing - producer needs less time to prime

# Verify data is available in Kafka topics
echo "🔍 Verifying data availability in Kafka topics..."
data_available=false
for attempt in {1..5}; do
    # Check if topics have messages
    if docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic stock-quotes-realtime | grep -q ":1" 2>/dev/null; then
        echo "✅ Data detected in stock-quotes-realtime topic"
        data_available=true
        break
    fi
    echo "   Attempt $attempt/5 - Waiting for data..."
    sleep 10
done

if [ "$data_available" = true ]; then
    echo "✅ Producer has primed the data pipeline"
else
    echo "⚠️  Proceeding without data verification (may be normal in mock mode)"
fi

# Initialize Kafka Connect first
echo "⚙️ Initializing Kafka Connect..."
docker-compose -f docker/compose/docker-compose.yaml up kafka-connect-init
echo "✅ Kafka Connect initialized"

# Start remaining services
echo "🚀 Starting final services..."
docker-compose -f docker/compose/docker-compose.yaml up -d kafka-ui &
docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml up -d streaming-processor &

# Wait for services to start
wait
echo "✅ All services started"

# Wait for services to be healthy
echo "⏳ Performing health checks..."
check_service_health "Kafka Connect" "http://localhost:8083/connectors"
check_service_health "Kafka UI" "http://localhost:8090"
check_service_health "Processor" "http://localhost:8082/health"
echo "✅ All services are healthy and ready!"

# Deploy Kafka Connectors automatically
echo ""
echo "📊 DEPLOYING KAFKA CONNECTORS"
echo "============================="
echo ""

# Check if connector deployment script exists
if [ -f "scripts/deploy-connectors.sh" ]; then
    ./scripts/deploy-connectors.sh
    echo ""
    
    # Load environment variables for connector deployment
    if [ -f "config/.env" ]; then
        set -a
        source config/.env
        set +a
    fi
    
    # Deploy connectors
    ./scripts/deploy-medallion-connectors.sh
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ All connectors deployed successfully!"
    else
        echo ""
        echo "⚠️  Some connectors may have deployment issues (check logs above)"
    fi
else
    echo "⚠️  Connector deployment script not found, skipping connector deployment"
    echo "   Run 'make deploy-connectors' manually if needed"
fi

echo ""
echo "🎉 Spark Cluster Mode Pipeline Started Successfully!"
echo "=================================================="
echo ""
echo "📊 Monitoring URLs:"
echo "   Spark Master UI:    http://localhost:8080"
echo "   Spark Worker 1 UI:  http://localhost:8181"
echo "   Spark Worker 2 UI:  http://localhost:8182"
echo "   Producer Health:    http://localhost:8081/health"
echo "   Processor Health:   http://localhost:8082/health"
echo "   Kafka UI:           http://localhost:8090"
echo "   Kafka Connect:      http://localhost:8083"
echo ""
echo "🔍 Check streaming queries distribution:"
echo "   docker logs streaming-processor -f"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose -f docker/compose/docker-compose.yaml -f docker/compose/docker-compose.cluster.yml down"
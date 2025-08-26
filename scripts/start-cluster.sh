#!/bin/bash

# Spark Cluster Startup Script
# Starts the complete streaming pipeline with Spark cluster mode

set -e

echo "🚀 Starting Stock Market Pipeline with Spark Cluster Mode"
echo "=================================================="

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

# Function to check if a service is healthy
check_service_health() {
    local service_name=$1
    local health_url=$2
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $service_name to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s "$health_url" > /dev/null 2>&1; then
            echo "✅ $service_name is healthy"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 5
        ((attempt++))
    done
    
    echo "❌ $service_name failed to become healthy"
    return 1
}

# Create network if it doesn't exist
echo "🔗 Creating Docker network..."
docker network create streaming-network 2>/dev/null || echo "   Network already exists"

# Start infrastructure services first
echo "🏗️  Starting infrastructure services..."
docker-compose -f docker-compose.yaml up -d zookeeper kafka schema-registry

# Start Spark Master in parallel with Kafka infrastructure (they're independent)
echo "⚡ Starting Spark Master (parallel with Kafka)..."
docker-compose -f docker-compose.yaml -f docker-compose.cluster.yml up -d spark-master

# Wait for Kafka and Schema Registry to be ready
echo "⏳ Waiting for Kafka infrastructure..."
sleep 30

# Start initialization services
echo "📋 Initializing topics and schemas..."
docker-compose -f docker-compose.yaml up kafka-topics-init schema-registry-init
echo "✅ Kafka topics and schemas initialized"

# Wait for Spark Master to be ready
check_service_health "Spark Master" "http://localhost:8080"

# Start Spark workers
echo "👷 Starting Spark workers..."
docker-compose -f docker-compose.yaml -f docker-compose.cluster.yml up -d spark-worker-1 spark-worker-2

# Wait for workers to register
echo "⏳ Waiting for workers to register with master..."
sleep 15

# Check worker health in parallel
check_service_health "Spark Worker 1" "http://localhost:8181" &
check_service_health "Spark Worker 2" "http://localhost:8182" &
wait
echo "✅ All Spark workers are healthy"

# Start application services with staggered timing to avoid cold start
echo "📈 Starting producer first to prime data..."
docker-compose -f docker-compose.yaml up -d streaming-producer

# Start kafka-connect in parallel (independent of producer)
docker-compose -f docker-compose.yaml up -d kafka-connect &

# Wait for producer to complete first data cycle
check_service_health "Producer" "http://localhost:8081/health"
echo "⏳ Waiting for producer to complete first data cycle (75 seconds)..."
sleep 75  # Allow one full production cycle (60s) + buffer

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

# Wait for kafka-connect to finish starting
wait
echo "✅ Application services started with data priming"

# Verify kafka-connect health (producer already verified)
check_service_health "Kafka Connect" "http://localhost:8083/connectors"
echo "✅ All application services are healthy with data primed"

# Initialize Kafka Connect (DLQ topics, etc.)
echo "⚙️ Initializing Kafka Connect configuration..."
docker-compose -f docker-compose.yaml up kafka-connect-init

# Start final services (processor starts after data is available)
echo "🔄 Starting streaming processor (data already primed)..."
docker-compose -f docker-compose.yaml -f docker-compose.cluster.yml up -d streaming-processor

echo "📈 Starting monitoring services..."
docker-compose -f docker-compose.yaml up -d kafka-ui

# Wait for final services to be healthy in parallel
echo "⏳ Performing final health checks..."
check_service_health "Processor" "http://localhost:8082/health" &
check_service_health "Kafka UI" "http://localhost:8090" &
wait
echo "✅ All services are healthy and ready!"

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
echo ""
echo "🔍 Check streaming queries distribution:"
echo "   docker logs streaming-processor -f"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose -f docker-compose.yaml -f docker-compose.cluster.yml down"
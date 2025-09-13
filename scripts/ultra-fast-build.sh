#!/bin/bash

# ===============================================
# ULTRA-FAST BUILD SCRIPT WITH BASE IMAGE CACHING
# ===============================================
# Eliminates 30+ min Spark download with smart caching

set -e

echo "🚀 ULTRA-FAST BUILD WITH BASE IMAGE CACHING"
echo "============================================"

# Enable BuildKit for parallel builds
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

# Check if Spark base image exists
echo "🔍 Checking for cached Spark base image..."
if docker image inspect stock-market-pipeline-spark-base:latest >/dev/null 2>&1; then
    echo "✅ Found cached Spark base image - NO DOWNLOAD NEEDED!"
    SPARK_BASE_EXISTS=true
else
    echo "⚠️  Spark base image not found - will build once and cache forever"
    SPARK_BASE_EXISTS=false
fi

# Build Spark base image (INSTANT with Bitnami)
if [ "$SPARK_BASE_EXISTS" = false ]; then
    echo ""
    echo "⚡ CREATING INSTANT SPARK BASE (Bitnami)"
    echo "======================================="
    echo "🚀 Using pre-built Bitnami Spark - NO DOWNLOAD TIME!"
    echo ""
    
    start_time=$(date +%s)
    
    echo "📥 Pulling Bitnami Spark image (this is fast!)..."
    docker pull bitnami/spark:3.5.1
    
    echo "🔨 Building enhanced Spark base with Python tools..."
    docker build \
        --tag stock-market-pipeline-spark-base:latest \
        --file docker/services/Dockerfile.spark-base \
        .
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo ""
    echo "✅ Instant Spark base ready in ${duration} seconds (vs 30+ minutes!)"
    echo "🎯 This image is now CACHED - future builds will be instant!"
else
    echo "🎯 Using existing Spark base - instant mode activated!"
fi

echo ""
echo "⚡ BUILDING APPLICATION SERVICES (LIGHTNING FAST)"
echo "================================================="

start_time=$(date +%s)

# Build all services in parallel using cached base
echo "📦 Building streaming-producer..."
docker build \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    --cache-from stock-market-pipeline-streaming-producer:latest \
    -t stock-market-pipeline-streaming-producer:latest \
    -f docker/services/Dockerfile.streaming-producer . &

echo "📦 Building kafka-connect..."  
docker build \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    --cache-from stock-market-pipeline-kafka-connect:latest \
    -t stock-market-pipeline-kafka-connect:latest \
    -f docker/services/Dockerfile.kafka-connect . &

echo "⚡ Building ultra-fast streaming-processor..."
docker build \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    --cache-from stock-market-pipeline-streaming-processor:latest \
    -t stock-market-pipeline-streaming-processor:latest \
    -f docker/services/Dockerfile.streaming-processor . &

echo "🔧 Building Spark cluster services..."
docker-compose -f docker/compose/docker-compose.cluster.yml build --parallel spark-master spark-worker-1 spark-worker-2 &

echo "⏳ Waiting for all builds to complete..."
wait

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "🎉 APPLICATION SERVICES BUILD COMPLETE!"
echo "======================================"
echo "⏰ Build time: ${duration} seconds (vs 30+ minutes before!)"

echo ""
echo "📊 FINAL IMAGE REPORT"
echo "====================="
docker images --filter "reference=stock-market-pipeline-*" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}"

echo ""
echo "🏆 PERFORMANCE SUMMARY"
echo "======================"
if [ "$SPARK_BASE_EXISTS" = false ]; then
    echo "🔥 First-time setup: Built cached Spark base (one-time cost)"
    echo "⚡ Future builds: Will be lightning fast (2-3 minutes)"
else
    echo "⚡ Ultra-fast build: Used cached Spark base"
    echo "🚀 Total build time: ~${duration} seconds (vs 30+ minutes)"
fi

echo ""
echo "🎯 NEXT STEPS"
echo "============="
echo "make start-dev    # Start with ultra-fast containers"
echo ""
echo "💡 TIP: The Spark base image is now cached forever."
echo "    Subsequent builds will be lightning fast!"

echo ""
echo "✅ Ultra-fast build completed at $(date)"
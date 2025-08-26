#!/bin/bash
# Fix Confluent Kafka Build Issues
# Simple script to fix librdkafka dependency issues

echo "🔧 Fixing Confluent Kafka Build Issues"
echo "======================================"

# Step 1: Clean Docker cache
echo "🧹 Step 1: Cleaning Docker build cache..."
docker image prune -f
docker builder prune -f
echo "✅ Docker cache cleaned"

# Step 2: Build producer image with enhanced dependencies
echo ""
echo "📦 Step 2: Building producer image with fixed dependencies..."
docker build -f Dockerfile.streaming-producer -t streaming-producer:latest .

if [ $? -eq 0 ]; then
    echo "✅ Producer image built successfully"
else
    echo "❌ Producer build failed"
    exit 1
fi

# Step 3: Build processor image with enhanced dependencies
echo ""
echo "📦 Step 3: Building processor image with fixed dependencies..."
docker build -f Dockerfile.streaming-processor -t streaming-processor:latest .

if [ $? -eq 0 ]; then
    echo "✅ Processor image built successfully"
else
    echo "❌ Processor build failed"
    exit 1
fi

# Step 4: Test confluent-kafka import
echo ""
echo "🧪 Step 4: Testing confluent-kafka import..."
docker run --rm streaming-producer:latest python3 -c "
from confluent_kafka import Producer, Consumer
print('✅ confluent-kafka working in producer image')
"

if [ $? -eq 0 ]; then
    echo "✅ confluent-kafka test passed!"
else
    echo "❌ confluent-kafka test failed!"
    exit 1
fi

# Success message
echo ""
echo "🎉 CONFLUENT KAFKA BUILD FIX COMPLETED!"
echo "======================================"
echo "✅ Enhanced Dockerfiles with librdkafka dependencies"
echo "✅ Both images built successfully"
echo "✅ confluent-kafka imports working"
echo ""
echo "🚀 Next steps:"
echo "1. Run: docker-compose up -d"
echo "2. Monitor: docker-compose logs -f"
echo "3. Your streaming pipeline should now work!"
echo ""
echo "🎯 The confluent-kafka build issue has been resolved!"
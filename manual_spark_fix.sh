#!/bin/bash
# Manual Spark Java Compatibility Fix
# Simple script to fix the Java compatibility issue

echo "🔧 Fixing Spark Java Compatibility Issue"
echo "========================================"

# Step 1: Verify current configuration
echo "📋 Step 1: Verifying current configuration..."
echo "Current Spark version in requirements:"
grep "pyspark" requirements-streaming.txt

echo "Current Spark version in Dockerfile:"
grep "SPARK_VERSION" Dockerfile.streaming-processor

# Step 2: Rebuild Docker image
echo ""
echo "📦 Step 2: Rebuilding Docker image with Spark 3.5.1..."
docker build -f Dockerfile.streaming-processor -t streaming-processor:latest .

if [ $? -eq 0 ]; then
    echo "✅ Docker image rebuilt successfully"
else
    echo "❌ Docker build failed"
    exit 1
fi

# Step 3: Test Spark compatibility
echo ""
echo "🧪 Step 3: Testing Spark compatibility..."
docker run --rm streaming-processor:latest python3 -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('Test').master('local[1]').getOrCreate()
    df = spark.range(5)
    count = df.count()
    print(f'✅ Spark 3.5.1 working! Processed {count} rows')
    spark.stop()
except Exception as e:
    print(f'❌ Spark test failed: {e}')
    exit(1)
"

if [ $? -eq 0 ]; then
    echo "✅ Spark compatibility test passed!"
else
    echo "❌ Spark compatibility test failed!"
    exit 1
fi

# Step 4: Success message
echo ""
echo "🎉 SPARK JAVA COMPATIBILITY FIX COMPLETED!"
echo "=========================================="
echo "✅ Spark 3.5.1 is now working with Java 17+"
echo "✅ Docker image updated successfully"
echo "✅ Compatibility tests passed"
echo ""
echo "🚀 Next steps:"
echo "1. Run: docker-compose up -d"
echo "2. Monitor: docker-compose logs -f streaming-processor"
echo "3. Your streaming pipeline should now work end-to-end!"
echo ""
echo "🎯 The Java compatibility issue has been resolved!"
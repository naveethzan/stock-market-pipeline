#!/bin/bash

# Deploy Silver Layer S3 Connector with Parquet + Symbol+Time Partitioning
# This script deploys the Silver layer S3 connector for processed data storage
# with direct Parquet writing and symbol+time partitioning

set -e

# Load environment variables from .env files
echo "📁 Loading environment variables..."
if [ -f ".env" ]; then
    echo "   Loading from .env..."
    export $(grep -v '^#' .env | xargs)
fi

if [ -f "config/.env" ]; then
    echo "   Loading from config/.env..."
    export $(grep -v '^#' config/.env | xargs)
fi

echo "✅ Environment variables loaded"
echo "   S3_BUCKET_NAME: ${S3_BUCKET_NAME:-'not set'}"
echo "   AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-'not set'}"
echo "   SCHEMA_REGISTRY_URL: ${SCHEMA_REGISTRY_URL:-'not set'}"
echo ""

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CONFIG_FILE="config/kafka-connect/connectors/silver-s3-connector.json"

echo "🥈 Deploying Silver Layer S3 Connector (Parquet + Symbol+Time Partitioning)..."
echo "============================================================================"

# Check if Kafka Connect is ready
echo "⏳ Checking Kafka Connect availability..."
python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL wait --timeout 60

if [ $? -ne 0 ]; then
    echo "❌ Error: Kafka Connect is not available"
    exit 1
fi

# Validate required environment variables
if [ -z "$S3_BUCKET_NAME" ]; then
    echo "❌ Error: S3_BUCKET_NAME environment variable not set"
    echo "   Please set S3_BUCKET_NAME in .env file or environment"
    exit 1
fi

if [ -z "$AWS_DEFAULT_REGION" ]; then
    echo "⚠️  Warning: AWS_DEFAULT_REGION not set, using us-east-1"
    export AWS_DEFAULT_REGION=us-east-1
fi

if [ -z "$SCHEMA_REGISTRY_URL" ]; then
    echo "⚠️  Warning: SCHEMA_REGISTRY_URL not set, using default"
    export SCHEMA_REGISTRY_URL=http://schema-registry:8081
fi

# Delete existing Silver connector if it exists
echo "🔄 Checking for existing Silver connector..."
CONNECTOR_EXISTS=$(curl -s "$CONNECT_URL/connectors/silver-s3-sink-connector" | grep -o '"name"' || echo "")

if [ -n "$CONNECTOR_EXISTS" ]; then
    echo "🗑️  Deleting existing Silver connector..."
    curl -X DELETE "$CONNECT_URL/connectors/silver-s3-sink-connector" || echo "⚠️  Failed to delete existing connector"
    sleep 3
fi

# Create a temporary config file with environment variables substituted
TEMP_CONFIG=$(mktemp)
python3 scripts/env_substitute.py $CONFIG_FILE $TEMP_CONFIG

echo "📊 Creating Silver S3 connector with new configuration..."
echo "   Format: Parquet with Snappy compression"
echo "   Partitioning: symbol=SYMBOL/year=YYYY/month=MM/day=dd/hour=HH/"
echo "   Converter: AvroConverter with Schema Registry"

python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL create $TEMP_CONFIG

if [ $? -eq 0 ]; then
    echo "✅ Silver S3 connector created successfully"
    
    # Wait a moment for connector to initialize
    sleep 5
    
    # Check connector status
    echo "🔍 Checking connector status..."
    python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL status silver-s3-sink-connector
    
    # Restart streaming processor to use Avro serialization
    echo "🔄 Restarting streaming processor with Avro serialization..."
    
    # Check if processor container is running
    if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q "streaming-processor.*Up"; then
        echo "   Stopping existing streaming processor..."
        docker stop streaming-processor >/dev/null 2>&1 || true
        sleep 3
    fi
    
    echo "   Starting streaming processor with Avro configuration..."
    if docker-compose ps | grep -q "cluster"; then
        # Cluster mode
        docker-compose -f docker-compose.yaml -f docker-compose.cluster.yml up -d streaming-processor
    else
        # Regular mode
        docker-compose up -d streaming-processor
    fi
    
    # Wait for processor to be ready
    echo "⏳ Waiting for streaming processor to be ready..."
    attempt=1
    max_attempts=12
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s "http://localhost:8082/health" > /dev/null 2>&1; then
            echo "✅ Streaming processor is ready"
            break
        fi
        
        echo "   Attempt $attempt/$max_attempts - processor not ready yet..."
        sleep 5
        ((attempt++))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        echo "⚠️  Streaming processor took longer than expected to start"
    fi
    
    echo ""
    echo "🎉 Silver Layer S3 Connector deployed successfully!"
    echo "================================================================="
    echo ""
    echo "📊 Configuration Summary:"
    echo "   • Format: Parquet with Snappy compression"
    echo "   • Partitioning: symbol + time based"
    echo "   • Converter: AvroConverter with Schema Registry"
    echo "   • Topics: processed-stock-prices, processed-trading-volume, processed-technical-indicators"
    echo ""
    echo "📂 Expected S3 Structure:"
    echo "   s3://$S3_BUCKET_NAME/silver/stock-data/"
    echo "   ├── symbol=AAPL/year=2025/month=08/day=25/hour=20/"
    echo "   │   ├── processed-stock-prices-0-00000-abc123.parquet"
    echo "   │   └── processed-trading-volume-0-00000-def456.parquet"
    echo "   └── symbol=GOOGL/year=2025/month=08/day=25/hour=20/"
    echo "       └── processed-technical-indicators-0-00000-ghi789.parquet"
    echo ""
    echo "🔍 Monitoring:"
    echo "   • Connector status: curl $CONNECT_URL/connectors/silver-s3-sink-connector/status"
    echo "   • Stream health: curl http://localhost:8082/health"
    echo "   • Kafka topics: kafka-console-consumer --bootstrap-server localhost:9092 --topic processed-stock-prices --max-messages 5"
    echo ""
    echo "📝 To test the connector:"
    echo "   python3 scripts/test-silver-connector.py"
    
else
    echo "❌ Failed to create Silver S3 connector"
    exit 1
fi

# Cleanup
rm -f $TEMP_CONFIG
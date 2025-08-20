#!/bin/bash

# Deploy Silver Layer S3 Connector
# This script deploys the Silver layer S3 connector for processed data storage

set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CONFIG_FILE="config/kafka-connect/connectors/silver-s3-connector.json"

echo "Deploying Silver Layer S3 Connector..."
echo "===================================="

# Check if Kafka Connect is ready
echo "Checking Kafka Connect availability..."
python scripts/kafka-connect-manager.py --connect-url $CONNECT_URL wait --timeout 60

if [ $? -ne 0 ]; then
    echo "Error: Kafka Connect is not available"
    exit 1
fi

# Validate environment variables
if [ -z "$S3_BUCKET_NAME" ]; then
    echo "Error: S3_BUCKET_NAME environment variable is required"
    exit 1
fi

if [ -z "$AWS_DEFAULT_REGION" ]; then
    echo "Warning: AWS_DEFAULT_REGION not set, using us-east-1"
    export AWS_DEFAULT_REGION=us-east-1
fi

# Create a temporary config file with environment variables substituted
TEMP_CONFIG=$(mktemp)
envsubst < $CONFIG_FILE > $TEMP_CONFIG

echo "Creating Silver S3 connector..."
python scripts/kafka-connect-manager.py --connect-url $CONNECT_URL create $TEMP_CONFIG

if [ $? -eq 0 ]; then
    echo "✓ Silver S3 connector created successfully"
    
    # Wait a moment for connector to initialize
    sleep 5
    
    # Check connector status
    echo "Checking connector status..."
    python scripts/kafka-connect-manager.py --connect-url $CONNECT_URL status silver-s3-sink-connector
    
    echo ""
    echo "Silver Layer S3 Connector deployed successfully!"
    echo "Processed data from topics:"
    echo "  - processed-stock-prices"
    echo "  - processed-trading-volume" 
    echo "  - processed-technical-indicators"
    echo "will be stored in S3 bucket: $S3_BUCKET_NAME/silver/stock-data/"
    echo ""
    echo "Data format: Parquet with Snappy compression"
    echo "Partitioning: By symbol and date for optimal analytics performance"
    echo ""
    echo "To test the connector, run:"
    echo "  python scripts/test-silver-connector.py"
    
else
    echo "✗ Failed to create Silver S3 connector"
    exit 1
fi

# Cleanup
rm -f $TEMP_CONFIG
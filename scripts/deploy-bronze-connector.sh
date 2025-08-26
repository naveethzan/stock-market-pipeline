#!/bin/bash

# Deploy Bronze Layer S3 Connector
# This script deploys the Bronze layer S3 connector for raw data storage

set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CONFIG_FILE="config/kafka-connect/connectors/bronze-s3-connector.json"

echo "Deploying Bronze Layer S3 Connector..."
echo "=================================="

# Check if Kafka Connect is ready
echo "Checking Kafka Connect availability..."
python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL wait --timeout 60

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
python3 scripts/env_substitute.py $CONFIG_FILE $TEMP_CONFIG

echo "Creating Bronze S3 connector..."
python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL create $TEMP_CONFIG

if [ $? -eq 0 ]; then
    echo "✓ Bronze S3 connector created successfully"
    
    # Wait a moment for connector to initialize
    sleep 5
    
    # Check connector status
    echo "Checking connector status..."
    python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL status bronze-s3-sink-connector
    
    echo ""
    echo "Bronze Layer S3 Connector deployed successfully!"
    echo "Data from topics 'stock-quotes-realtime' and 'stock-intraday-data'"
    echo "will be stored in S3 bucket: $S3_BUCKET_NAME/bronze/stock-data/"
    echo ""
    echo "To test the connector, run:"
    echo "  python3 scripts/test-bronze-connector.py"
    
else
    echo "✗ Failed to create Bronze S3 connector"
    exit 1
fi

# Cleanup
rm -f $TEMP_CONFIG
#!/bin/bash

# Deploy Gold Layer Snowflake Connector
# This script deploys the Gold layer Snowflake connector for dimensional data loading

set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CONFIG_FILE="config/kafka-connect/connectors/gold-snowflake-connector.json"

echo "Deploying Gold Layer Snowflake Connector..."
echo "=========================================="

# Check if Kafka Connect is ready
echo "Checking Kafka Connect availability..."
python scripts/kafka-connect-manager.py --connect-url $CONNECT_URL wait --timeout 60

if [ $? -ne 0 ]; then
    echo "Error: Kafka Connect is not available"
    exit 1
fi

# Validate required Snowflake environment variables
REQUIRED_VARS=(
    "SNOWFLAKE_ACCOUNT"
    "SNOWFLAKE_USER" 
    "SNOWFLAKE_DATABASE"
    "SNOWFLAKE_SCHEMA"
    "SNOWFLAKE_WAREHOUSE"
    "SNOWFLAKE_ROLE"
)

MISSING_VARS=()
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    echo "Error: Missing required environment variables:"
    printf '  %s\n' "${MISSING_VARS[@]}"
    echo ""
    echo "Please set the following Snowflake configuration variables:"
    echo "  export SNOWFLAKE_ACCOUNT=your-account"
    echo "  export SNOWFLAKE_USER=your-username"
    echo "  export SNOWFLAKE_PASSWORD=your-password  # or use private key"
    echo "  export SNOWFLAKE_DATABASE=your-database"
    echo "  export SNOWFLAKE_SCHEMA=your-schema"
    echo "  export SNOWFLAKE_WAREHOUSE=your-warehouse"
    echo "  export SNOWFLAKE_ROLE=your-role"
    exit 1
fi

# Check authentication method
if [ -z "$SNOWFLAKE_PASSWORD" ] && [ -z "$SNOWFLAKE_PRIVATE_KEY" ]; then
    echo "Error: Either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY must be set"
    exit 1
fi

# Set Snowflake URL if not provided
if [ -z "$SNOWFLAKE_URL" ]; then
    export SNOWFLAKE_URL="https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
fi

echo "Snowflake Configuration:"
echo "  Account: $SNOWFLAKE_ACCOUNT"
echo "  User: $SNOWFLAKE_USER"
echo "  Database: $SNOWFLAKE_DATABASE"
echo "  Schema: $SNOWFLAKE_SCHEMA"
echo "  Warehouse: $SNOWFLAKE_WAREHOUSE"
echo "  Role: $SNOWFLAKE_ROLE"
echo ""

# Create a temporary config file with environment variables substituted
TEMP_CONFIG=$(mktemp)
envsubst < $CONFIG_FILE > $TEMP_CONFIG

echo "Creating Gold Snowflake connector..."
python scripts/kafka-connect-manager.py --connect-url $CONNECT_URL create $TEMP_CONFIG

if [ $? -eq 0 ]; then
    echo "✓ Gold Snowflake connector created successfully"
    
    # Wait a moment for connector to initialize
    sleep 10
    
    # Check connector status
    echo "Checking connector status..."
    python scripts/kafka-connect-manager.py --connect-url $CONNECT_URL status gold-snowflake-sink-connector
    
    echo ""
    echo "Gold Layer Snowflake Connector deployed successfully!"
    echo ""
    echo "Processed data from topics will be loaded into Snowflake staging tables:"
    echo "  - processed-stock-prices → FACT_STOCK_PRICES_STAGING"
    echo "  - processed-trading-volume → FACT_TRADING_VOLUME_STAGING"
    echo "  - processed-technical-indicators → TECHNICAL_INDICATORS_STAGING"
    echo ""
    echo "Data loading method: Direct ingestion from Kafka to Snowflake"
    echo "Buffer settings: 1,000 records or 5MB or 60 seconds"
    echo ""
    echo "To test the connector, run:"
    echo "  python scripts/test-gold-connector.py"
    echo ""
    echo "To monitor data loading:"
    echo "  SELECT COUNT(*) FROM FACT_STOCK_PRICES_STAGING;"
    echo "  SELECT COUNT(*) FROM FACT_TRADING_VOLUME_STAGING;"
    echo "  SELECT COUNT(*) FROM TECHNICAL_INDICATORS_STAGING;"
    
else
    echo "✗ Failed to create Gold Snowflake connector"
    exit 1
fi

# Cleanup
rm -f $TEMP_CONFIG
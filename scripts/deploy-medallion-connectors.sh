#!/bin/bash

# Deploy All Medallion Architecture Connectors
# This script deploys Bronze, Silver, and Gold layer connectors for the complete medallion architecture

set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}

echo "Deploying Medallion Architecture Connectors"
echo "==========================================="
echo ""

# Check if Kafka Connect is ready
echo "1. Checking Kafka Connect availability..."
python3 scripts/kafka-connect-manager.py --connect-url $CONNECT_URL wait --timeout 120

if [ $? -ne 0 ]; then
    echo "Error: Kafka Connect is not available"
    exit 1
fi

echo "✓ Kafka Connect is ready"
echo ""

# Deploy Bronze Layer Connector
echo "2. Deploying Bronze Layer S3 Connector..."
echo "----------------------------------------"
./scripts/deploy-bronze-connector.sh

if [ $? -ne 0 ]; then
    echo "✗ Failed to deploy Bronze layer connector"
    exit 1
fi

echo ""

# Deploy Silver Layer Connector  
echo "3. Deploying Silver Layer S3 Connector..."
echo "----------------------------------------"
./scripts/deploy-silver-connector.sh

if [ $? -ne 0 ]; then
    echo "✗ Failed to deploy Silver layer connector"
    exit 1
fi

echo ""

# Deploy Gold Layer Connector
echo "4. Deploying Gold Layer Snowflake Connector..."
echo "---------------------------------------------"
./scripts/deploy-gold-connector.sh

if [ $? -ne 0 ]; then
    echo "✗ Failed to deploy Gold layer connector"
    exit 1
fi

echo ""

# Verify all connectors
echo "5. Verifying All Connectors..."
echo "-----------------------------"

CONNECTORS=("bronze-s3-sink-connector" "silver-s3-sink-connector" "gold-snowflake-sink-connector")
ALL_RUNNING=true

for connector in "${CONNECTORS[@]}"; do
    echo "Checking $connector..."
    STATUS=$(curl -s http://localhost:8083/connectors/$connector/status | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
    
    if [ "$STATUS" = "RUNNING" ]; then
        echo "✓ $connector is RUNNING"
    else
        echo "✗ $connector is in state: $STATUS"
        ALL_RUNNING=false
    fi
done

echo ""

if [ "$ALL_RUNNING" = true ]; then
    echo "🎉 Medallion Architecture Deployment Complete!"
    echo "=============================================="
    echo ""
    echo "All three layers are now configured:"
    echo ""
    echo "📊 BRONZE LAYER (Raw Data Storage)"
    echo "   • Topics: stock-quotes-realtime, stock-intraday-data"
    echo "   • Format: Avro with schema evolution"
    echo "   • Storage: S3 with time-based partitioning"
    echo "   • Path: s3://$S3_BUCKET_NAME/bronze/stock-data/"
    echo ""
    echo "🔄 SILVER LAYER (Processed Data Storage)"
    echo "   • Topics: processed-stock-prices, processed-trading-volume, processed-technical-indicators"
    echo "   • Format: Parquet with Snappy compression"
    echo "   • Storage: S3 with symbol/date partitioning"
    echo "   • Path: s3://$S3_BUCKET_NAME/silver/stock-data/"
    echo ""
    echo "🏆 GOLD LAYER (Analytics Data)"
    echo "   • Topics: processed-stock-prices, processed-trading-volume, processed-technical-indicators"
    echo "   • Method: Direct ingestion from Kafka to Snowflake"
    echo "   • Storage: Snowflake dimensional model"
    echo "   • Tables: FACT_STOCK_PRICES_STAGING, FACT_TRADING_VOLUME_STAGING, TECHNICAL_INDICATORS_STAGING"
    echo ""
    echo "🔧 MANAGEMENT COMMANDS:"
    echo "   • List connectors: python3 scripts/kafka-connect-manager.py list"
    echo "   • Check status: python3 scripts/kafka-connect-manager.py status <connector-name>"
    echo "   • Restart connector: python3 scripts/kafka-connect-manager.py restart <connector-name>"
    echo ""
    echo "🧪 TESTING:"
    echo "   • Test Bronze: python3 scripts/test-bronze-connector.py"
    echo "   • Test Silver: python3 scripts/test-silver-connector.py"
    echo "   • Test Gold: python3 scripts/test-gold-connector.py"
    echo ""
    echo "📊 MONITORING:"
    echo "   • Kafka UI: http://localhost:8090"
    echo "   • Kafka Connect API: http://localhost:8083"
    echo "   • Prometheus: http://localhost:9090"
    echo "   • Grafana: http://localhost:3000"
    echo ""
    echo "Data will now flow through the complete medallion architecture:"
    echo "Raw Data → Bronze (S3/Avro) → Processing → Silver (S3/Parquet) → Gold (Snowflake)"
    
else
    echo "⚠️  Some connectors are not running properly."
    echo "Please check the connector status and logs for issues."
    echo ""
    echo "Troubleshooting commands:"
    echo "  python3 scripts/kafka-connect-manager.py list"
    echo "  docker logs kafka-connect"
    exit 1
fi
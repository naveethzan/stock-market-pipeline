#!/bin/bash

# ===============================================
# UNIFIED CONNECTOR DEPLOYMENT SCRIPT
# ===============================================
# Deploys all 3 medallion architecture connectors:
# - Bronze S3 Connector (raw data)
# - Silver S3 Connector (processed data)  
# - Redshift Connector (analytics data)
# 
# Features:
# - Enhanced error handling and retries
# - Automatic topic creation
# - Redshift configuration validation
# - Real-time status monitoring

set -e

echo "🚀 MEDALLION CONNECTOR DEPLOYMENT"
echo "=================================="
echo ""

CONNECT_URL="http://localhost:8083"

# Load environment variables
echo "📁 Loading environment variables..."
if [ -f "config/.env" ]; then
    set -a
    source config/.env
    set +a
    echo "✅ Environment loaded from config/.env"
else
    echo "❌ config/.env not found!"
    exit 1
fi

# Wait for Kafka Connect to be ready
echo ""
echo "⏳ Waiting for Kafka Connect to be ready..."
max_attempts=60
attempt=1

while [ $attempt -le $max_attempts ]; do
    if curl -f -s "$CONNECT_URL/connectors" >/dev/null 2>&1; then
        echo "✅ Kafka Connect is ready!"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        echo "   Still waiting... ($attempt/$max_attempts)"
    fi
    
    sleep 2
    ((attempt++))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ Kafka Connect did not become ready within timeout"
    exit 1
fi

# Function to deploy a connector with retries
deploy_connector() {
    local connector_name="$1"
    local config_file="$2"
    local display_name="$3"
    
    echo ""
    echo "📦 Deploying $display_name..."
    echo "   Config: $config_file"
    
    # Check if config file exists
    if [ ! -f "$config_file" ]; then
        echo "❌ Config file not found: $config_file"
        return 1
    fi
    
    # Delete existing connector if it exists
    if curl -f -s "$CONNECT_URL/connectors/$connector_name" >/dev/null 2>&1; then
        echo "   Removing existing connector..."
        curl -X DELETE "$CONNECT_URL/connectors/$connector_name"
        sleep 3
    fi
    
    # Substitute environment variables
    temp_config=$(mktemp)
    envsubst < "$config_file" > "$temp_config"
    
    # Deploy connector
    echo "   Creating connector..."
    response=$(curl -s -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @"$temp_config")
    
    # Check if deployment was successful
    if echo "$response" | grep -q '"name"'; then
        echo "✅ $display_name deployed successfully"
        
        # Wait for connector to start
        echo "   Waiting for connector to start..."
        sleep 10
        
        # Check status
        status=$(curl -s "$CONNECT_URL/connectors/$connector_name/status" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data['connector']['state'])
except:
    print('UNKNOWN')
" 2>/dev/null)
        
        if [ "$status" = "RUNNING" ]; then
            echo "✅ $display_name is RUNNING"
            return 0
        else
            echo "⚠️  $display_name status: $status"
            echo "   Response: $response"
            return 1
        fi
    else
        echo "❌ Failed to deploy $display_name"
        echo "   Response: $response"
        rm -f "$temp_config"
        return 1
    fi
    
    rm -f "$temp_config"
}

# Verify required topics exist for Redshift
echo ""
echo "🔍 Checking required topics for Redshift..."
required_topics=("processed-stock-prices" "processed-trading-volume" "processed-technical-indicators")
missing_topics=()

for topic in "${required_topics[@]}"; do
    if ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^${topic}$"; then
        missing_topics+=("$topic")
    fi
done

if [ ${#missing_topics[@]} -gt 0 ]; then
    echo "⚠️  Missing required topics for Redshift connector:"
    printf '   - %s\n' "${missing_topics[@]}"
    echo ""
    echo "🔧 Creating missing topics..."
    
    for topic in "${missing_topics[@]}"; do
        echo "   Creating topic: $topic"
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
            --create --if-not-exists --topic "$topic" \
            --partitions 3 --replication-factor 1 || true
    done
    
    echo "✅ Topics created"
fi

# Deploy connectors in order
echo ""
echo "🚀 DEPLOYING CONNECTORS IN SEQUENCE"
echo "===================================="

# 1. Deploy Bronze S3 Connector
deploy_connector "bronze-s3-sink-connector" \
    "config/kafka-connect/connectors/bronze-s3-connector.json" \
    "Bronze S3 Connector"
bronze_result=$?

# 2. Deploy Silver S3 Connector  
deploy_connector "silver-s3-sink-connector" \
    "config/kafka-connect/connectors/silver-s3-connector.json" \
    "Silver S3 Connector"
silver_result=$?

# 3. Deploy Redshift Connector (with enhanced error handling)
echo ""
echo "📊 DEPLOYING REDSHIFT CONNECTOR (Enhanced)"
echo "=========================================="

# Validate Redshift configuration
echo "🔍 Validating Redshift configuration..."
required_vars=("REDSHIFT_ENDPOINT" "REDSHIFT_DATABASE" "REDSHIFT_USER" "REDSHIFT_PASSWORD" "REDSHIFT_PORT")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ Missing required Redshift variables:"
    printf '   %s\n' "${missing_vars[@]}"
    redshift_result=1
else
    echo "✅ Redshift configuration validated"
    echo "   Endpoint: $REDSHIFT_ENDPOINT"
    echo "   Database: $REDSHIFT_DATABASE"
    echo "   User: $REDSHIFT_USER"
    
    deploy_connector "redshift-streaming-sink" \
        "config/kafka-connect/connectors/redshift-streaming-connector.json" \
        "Redshift Streaming Connector"
    redshift_result=$?
fi

# Final summary
echo ""
echo "📊 DEPLOYMENT SUMMARY"
echo "===================="

connectors_deployed=0
if [ $bronze_result -eq 0 ]; then
    echo "✅ Bronze S3 Connector: SUCCESS"
    ((connectors_deployed++))
else
    echo "❌ Bronze S3 Connector: FAILED"
fi

if [ $silver_result -eq 0 ]; then
    echo "✅ Silver S3 Connector: SUCCESS"
    ((connectors_deployed++))
else
    echo "❌ Silver S3 Connector: FAILED"
fi

if [ $redshift_result -eq 0 ]; then
    echo "✅ Redshift Connector: SUCCESS"
    ((connectors_deployed++))
else
    echo "❌ Redshift Connector: FAILED"
fi

echo ""
echo "📈 Results: $connectors_deployed/3 connectors deployed successfully"

# List all active connectors
echo ""
echo "🔍 ACTIVE CONNECTORS:"
active_connectors=$(curl -s "$CONNECT_URL/connectors" 2>/dev/null | python3 -c "
import sys, json
try:
    connectors = json.load(sys.stdin)
    for c in connectors:
        print(f'  ✓ {c}')
except:
    print('  Unable to fetch connector list')
")

echo "$active_connectors"

if [ $connectors_deployed -eq 3 ]; then
    echo ""
    echo "🎉 ALL CONNECTORS DEPLOYED SUCCESSFULLY!"
    echo "======================================="
    echo ""
    echo "📊 Medallion Architecture Active:"
    echo "  Bronze → Raw data to S3 (Avro)"
    echo "  Silver → Processed data to S3 (Parquet)" 
    echo "  Gold   → Analytics data to Redshift"
    exit 0
else
    echo ""
    echo "⚠️  PARTIAL DEPLOYMENT"
    echo "===================="
    echo "Some connectors failed to deploy. Check logs and retry."
    exit 1
fi
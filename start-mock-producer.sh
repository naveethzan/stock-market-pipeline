#!/bin/bash
# Start the streaming producer with mock Alpha Vantage data
# This enables continuous data streaming without hitting API rate limits

echo "Starting Stock Market Data Streaming Pipeline with Mock Data"
echo "============================================================"
echo ""
echo "This will start the producer using mock Alpha Vantage responses"
echo "for continuous streaming without API rate limits."
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
    echo "Error: Docker and docker-compose are required"
    exit 1
fi

# Use docker compose (newer) or docker-compose (legacy)
COMPOSE_CMD="docker-compose"
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
fi

echo "Using mock environment configuration..."

# Export mock mode environment variables
export ALPHA_VANTAGE_MOCK_MODE=true
export ALPHA_VANTAGE_API_KEY=mock_api_key_for_development
export PRODUCTION_INTERVAL_SECONDS=60
export STOCK_SYMBOLS="AAPL,GOOGL,MSFT,AMZN,TSLA,META,NVDA,NFLX,AMD,INTC"
export LOG_LEVEL=INFO

echo "Mock mode configuration:"
echo "  - ALPHA_VANTAGE_MOCK_MODE: $ALPHA_VANTAGE_MOCK_MODE"
echo "  - PRODUCTION_INTERVAL_SECONDS: $PRODUCTION_INTERVAL_SECONDS"
echo "  - STOCK_SYMBOLS: $STOCK_SYMBOLS"
echo ""

# Build the producer image first
echo "Building streaming producer image..."
$COMPOSE_CMD build streaming-producer

if [ $? -ne 0 ]; then
    echo "Error: Failed to build streaming producer image"
    exit 1
fi

echo ""
echo "Starting infrastructure services (Kafka, Schema Registry, etc.)..."
$COMPOSE_CMD up -d zookeeper kafka schema-registry kafka-topics-init schema-registry-init

echo ""
echo "Waiting for services to be ready..."
sleep 10

echo ""
echo "Starting streaming producer with mock data..."
$COMPOSE_CMD up streaming-producer

echo ""
echo "To monitor the stream:"
echo "  - Health endpoint: http://localhost:8081/health"
echo "  - Metrics endpoint: http://localhost:8081/metrics"
echo "  - Kafka UI: http://localhost:8090 (if running full stack)"
echo ""
echo "To stop the producer: Ctrl+C"
echo "To run in background: $COMPOSE_CMD up -d streaming-producer"
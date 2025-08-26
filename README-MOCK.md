# Mock Alpha Vantage API for Continuous Data Streaming

## Overview

Due to Alpha Vantage API rate limits (25 requests/day for free tier), we've implemented a Mock Alpha Vantage client that generates realistic stock market data for continuous streaming without hitting rate limits.

## Features

✅ **Realistic Stock Market Data**: Generates realistic price movements, volumes, and market trends  
✅ **No Rate Limits**: Unlimited API calls for continuous streaming  
✅ **Same Interface**: Drop-in replacement for real Alpha Vantage client  
✅ **40+ Stock Symbols**: Pre-configured with popular stock symbols and their realistic base prices  
✅ **Dynamic Price Movements**: Simulates realistic price trends (upward, downward, sideways)  
✅ **Intraday Data Support**: Generates complete intraday time series data  
✅ **Continuous Price Evolution**: Maintains price continuity between requests  

## Quick Start

### Option 1: Use the Helper Script (Recommended)

```bash
# Start the producer with mock data
./start-mock-producer.sh
```

This script will:
- Enable mock mode automatically
- Start all required infrastructure (Kafka, Schema Registry)
- Launch the streaming producer with realistic mock data
- Set up monitoring endpoints

### Option 2: Manual Environment Configuration

1. **Set Environment Variables**:
```bash
export ALPHA_VANTAGE_MOCK_MODE=true
export ALPHA_VANTAGE_API_KEY=mock_api_key
export PRODUCTION_INTERVAL_SECONDS=300  # 5 minutes for realistic intraday data
```

2. **Start the Services**:
```bash
# Start infrastructure
docker compose up -d zookeeper kafka schema-registry

# Start producer with mock data
docker compose up streaming-producer
```

### Option 3: Using Environment File

1. **Copy the mock environment file**:
```bash
cp .env.mock .env.local
```

2. **Load environment and start**:
```bash
docker compose --env-file .env.local up streaming-producer
```

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ALPHA_VANTAGE_MOCK_MODE` | `false` | Enable mock responses instead of real API calls |
| `PRODUCTION_INTERVAL_SECONDS` | `300` | Interval between streaming cycles (300s/5min for realistic data) |
| `STOCK_SYMBOLS` | `AAPL,GOOGL,MSFT,AMZN,TSLA` | Comma-separated list of symbols to stream |
| `LOG_LEVEL` | `INFO` | Logging level for detailed output |

### Supported Stock Symbols

The mock client supports 40+ popular stock symbols with realistic base prices:

**Tech Stocks**: AAPL, GOOGL, MSFT, AMZN, TSLA, META, NVDA, NFLX, AMD, INTC, ORCL, CRM  
**Financial**: JPM, BAC, WFC, C, GS, MS, V, MA  
**Consumer**: WMT, PG, JNJ, KO, PEP, MCD, HD, LOW  
**Energy**: XOM, CVX  
**Others**: UBER, LYFT, SNAP, BABA, SHOP, SQ, PYPL  

*New symbols are automatically added with random realistic prices when requested.*

## Mock Data Features

### Real-time Quote Generation

```json
{
  "Global Quote": {
    "01. symbol": "AAPL",
    "02. open": "175.1234",
    "03. high": "176.5678",
    "04. low": "174.9876",
    "05. price": "175.8901",
    "06. volume": "25678901",
    "07. latest trading day": "2025-08-23",
    "08. previous close": "175.2345",
    "09. change": "0.6556",
    "10. change percent": "0.3738%"
  },
  "_metadata": {
    "symbol": "AAPL",
    "mock_mode": true,
    "data_source": "alpha_vantage_mock"
  }
}
```

### Features:
- **Realistic Price Movements**: ±5% daily variation with trending behavior
- **Dynamic Volume**: 1M-50M share volumes based on symbol popularity
- **Price Continuity**: Consecutive requests show realistic price evolution
- **Market Trends**: Simulates upward, downward, and sideways price trends

### Intraday Data Generation

- **Multiple Intervals**: 1min, 5min, 15min, 30min, 60min
- **Complete Time Series**: Full OHLCV data for each time point
- **Configurable Size**: Compact (80-100 points) or Full (800-1000 points)
- **Realistic Patterns**: Price movements follow realistic intraday patterns

## Monitoring and Verification

### Health Check Endpoints

```bash
# Producer health
curl http://localhost:8081/health

# Detailed metrics
curl http://localhost:8081/metrics
```

### Sample Health Response (Mock Mode)
```json
{
  "status": "healthy",
  "is_running": true,
  "metrics": {
    "messages": {
      "sent": 150,
      "success_rate": 1.0
    },
    "api": {
      "requests": 50,
      "errors": 0,
      "error_rate": 0.0
    }
  },
  "mock_mode": true,
  "client_type": "MockAlphaVantageClient"
}
```

### Kafka Topics Verification

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Monitor messages
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-quotes-realtime --from-beginning
```

## Benefits of Mock Mode

### ✅ Development Benefits
- **No API Costs**: No Alpha Vantage subscription required
- **Unlimited Testing**: Test streaming pipeline indefinitely
- **Consistent Data**: Reproducible test scenarios
- **Fast Development**: No waiting for API rate limits

### ✅ Realistic Simulation
- **Market-like Behavior**: Realistic price movements and trends
- **Volume Patterns**: Authentic trading volume simulation
- **Temporal Consistency**: Proper time-based data flow
- **Multiple Assets**: Test with diverse stock portfolios

### ✅ Pipeline Validation
- **End-to-End Testing**: Complete pipeline validation without external dependencies
- **Performance Testing**: Load test your Kafka and Spark components
- **Error Handling**: Test system resilience with consistent data flow
- **Monitoring Setup**: Validate all monitoring and alerting systems

## Transitioning to Production

When ready to use real Alpha Vantage data:

1. **Obtain Alpha Vantage API Key**:
```bash
export ALPHA_VANTAGE_API_KEY=your_real_api_key
export ALPHA_VANTAGE_MOCK_MODE=false
```

2. **Adjust Streaming Interval**:
```bash
# Respect API rate limits
export PRODUCTION_INTERVAL_SECONDS=300  # 5 minutes
```

3. **Restart Services**:
```bash
docker compose restart streaming-producer
```

## Troubleshooting

### Common Issues

**Issue**: Container fails to start  
**Solution**: Check environment variables are set correctly:
```bash
docker compose logs streaming-producer
```

**Issue**: No data flowing to Kafka  
**Solution**: Verify topics exist and producer is running:
```bash
# Check producer status
curl http://localhost:8081/health

# Verify Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Issue**: Mock data seems unrealistic  
**Solution**: The mock client generates random but realistic movements. For consistent testing, set a random seed in your test environment.

### Logs Analysis

```bash
# Follow producer logs
docker compose logs -f streaming-producer

# Check for mock mode activation
docker compose logs streaming-producer | grep -i mock
```

## Architecture

```
Alpha Vantage Mock Client
       ↓
Stock Quote Generation (40+ symbols)
       ↓
Avro Serialization
       ↓
Kafka Topics (stock-quotes-realtime, stock-intraday-data)
       ↓
Downstream Processing (Spark, Kafka Connect, etc.)
```

The mock client seamlessly integrates with your existing streaming pipeline, providing continuous realistic data for development, testing, and demonstration purposes.

---

**Ready to start streaming realistic stock market data without API limits? Use the mock mode and build your pipeline with confidence!**
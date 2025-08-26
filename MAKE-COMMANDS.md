# Make Commands Quick Reference

## Development vs Production Modes

### 🎭 Mock Mode (Development)
**Perfect for development, testing, and demos**
```bash
make mock-start    # Start with unlimited mock data
make mock-stop     # Stop mock mode
make validate-mock # Check mock mode status
```
**Features:**
- ✅ No Alpha Vantage API key required
- ✅ Unlimited realistic stock data
- ✅ Fast streaming (10-second intervals)
- ✅ 10+ popular stock symbols
- ✅ Perfect for continuous development

### 🚀 Normal Mode (Production)
**Uses real Alpha Vantage API**
```bash
make normal-start    # Start with real API data
make validate-normal # Check API key and status
```
**Requirements:**
- 🔑 Alpha Vantage API key required
- ⏱️ Slower intervals (5 minutes - respects API limits)
- 📊 Real market data

## Quick Start Commands

```bash
# First time setup
make setup

# Development workflow
make mock-start      # Start development environment
make status          # Check what's running
make logs           # Monitor in real-time
make mock-stop      # Stop when done

# Production workflow  
make normal-start   # Start production environment
make status         # Check status
make stop          # Stop all services
```

## Mode Switching

```bash
make switch-to-mock     # Switch from normal to mock mode
make switch-to-normal   # Switch from mock to normal mode
```

## Monitoring Commands

```bash
make status          # Show pipeline status and current mode
make logs           # Show real-time logs
make validate-mock  # Validate mock setup
make validate-normal # Validate normal setup
```

## Complete Command List

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make setup` | One-time environment setup |
| `make mock-start` | Start development mode (mock data) |
| `make normal-start` | Start production mode (real API) |
| `make start` | Alias for normal-start |
| `make stop` | Stop all services |
| `make mock-stop` | Stop with mock mode info |
| `make restart` | Restart current mode |
| `make switch-to-mock` | Switch to mock mode |
| `make switch-to-normal` | Switch to normal mode |
| `make status` | Check status and mode |
| `make validate-mock` | Validate mock setup |
| `make validate-normal` | Validate normal setup |
| `make logs` | Show real-time logs |
| `make clean` | Stop and clean everything |

## Environment Variables

### Mock Mode (Automatic)
```bash
ALPHA_VANTAGE_MOCK_MODE=true
ALPHA_VANTAGE_API_KEY=mock_api_key_for_development
PRODUCTION_INTERVAL_SECONDS=10
STOCK_SYMBOLS="AAPL,GOOGL,MSFT,AMZN,TSLA,META,NVDA,NFLX,AMD,INTC"
```

### Normal Mode (Manual)
```bash
ALPHA_VANTAGE_API_KEY=your_real_api_key
ALPHA_VANTAGE_MOCK_MODE=false
PRODUCTION_INTERVAL_SECONDS=300
```

## Workflow Examples

### 🎯 Development Workflow
```bash
make setup           # One-time setup
make mock-start      # Start with mock data
# ... develop and test ...
make logs           # Monitor if needed
make mock-stop      # Stop when done
```

### 🎯 Production Workflow
```bash
make setup          # One-time setup
# Edit config/.env with your API key
make normal-start   # Start with real data
make status         # Check health
make stop          # Stop when done
```

### 🎯 Demo Workflow
```bash
make mock-start     # Instant start with realistic data
# Show http://localhost:8090 (Kafka UI)
# Show http://localhost:8081/health
make mock-stop      # Clean stop
```
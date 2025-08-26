# Stock Market Streaming Pipeline - Pre-Flight Checklist & Running Guide

## 🔍 Pre-Flight Checklist

### 📋 System Requirements Verification

#### ✅ **Software Dependencies**
- [ ] **Docker** (v20.0+) installed and running
  ```bash
  docker --version
  docker-compose --version
  ```
- [ ] **Python 3.9+** installed
  ```bash
  python --version
  ```
- [ ] **Make** utility available
  ```bash
  make --version
  ```
- [ ] **JDK 11** installed (for Spark)
  ```bash
  java -version
  ```
- [ ] **Git** installed
  ```bash
  git --version
  ```

#### ✅ **Hardware Requirements**
- [ ] **RAM**: Minimum 8GB (Recommended 16GB+)
- [ ] **CPU**: 4+ cores recommended
- [ ] **Disk Space**: 20GB+ free space
- [ ] **Network**: Stable internet connection for API calls

### 🔑 **Environment Variables Setup**

#### ✅ **Required API Keys & Credentials**
- [ ] **Alpha Vantage API Key**
  ```bash
  export ALPHA_VANTAGE_API_KEY="your_api_key_here"
  ```
  Get your free key: https://www.alphavantage.co/support/#api-key

- [ ] **Snowflake Credentials** (if using Snowflake)
  ```bash
  export SNOWFLAKE_ACCOUNT="your_account"
  export SNOWFLAKE_USER="your_username"
  export SNOWFLAKE_PASSWORD="your_password"
  export SNOWFLAKE_WAREHOUSE="STOCK_WH"
  export SNOWFLAKE_DATABASE="STOCK_MARKET"
  export SNOWFLAKE_SCHEMA="STREAMING"
  export SNOWFLAKE_ROLE="SYSADMIN"
  ```

- [ ] **AWS Credentials** (if using S3)
  ```bash
  export AWS_ACCESS_KEY_ID="your_access_key"
  export AWS_SECRET_ACCESS_KEY="your_secret_key"
  export AWS_DEFAULT_REGION="us-east-1"
  export S3_BUCKET_NAME="your-bucket-name"
  ```

#### ✅ **Create `.env` File**
Create `config/.env` file with your credentials:
```bash
# Alpha Vantage Configuration
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Snowflake Configuration (Optional)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=STOCK_WH
SNOWFLAKE_DATABASE=STOCK_MARKET
SNOWFLAKE_SCHEMA=STREAMING
SNOWFLAKE_ROLE=SYSADMIN

# AWS Configuration (Optional)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Pipeline Configuration
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,AMZN,TSLA
PRODUCTION_INTERVAL_SECONDS=60
OUTPUT_BASE_PATH=/app/data/output
```

### 🐳 **Docker Environment Verification**

#### ✅ **Docker Resources**
- [ ] Docker Desktop running with sufficient resources:
  - Memory: 6GB+ allocated to Docker
  - CPU: 4+ cores allocated
  - Disk: 20GB+ available

#### ✅ **Port Availability Check**
Verify these ports are available:
```bash
# Check if ports are free (should return empty)
netstat -an | grep LISTEN | grep -E ":(2181|9092|8081|8082|8083|8085|8090|9090|3000|18080)"
```

**Required Ports:**
- [ ] **2181** - Zookeeper
- [ ] **9092** - Kafka
- [ ] **8081** - Producer Health Check
- [ ] **8082** - Processor Health Check  
- [ ] **8083** - Kafka Connect
- [ ] **8085** - Schema Registry
- [ ] **8090** - Kafka UI
- [ ] **9090** - Prometheus
- [ ] **3000** - Grafana
- [ ] **18080** - Spark Master UI

### 📁 **Directory Structure Verification**

#### ✅ **Required Directories**
Ensure these directories exist (will be created automatically):
- [ ] `./data/` - Data storage
- [ ] `./logs/` - Application logs
- [ ] `./checkpoints/` - Spark checkpoints

```bash
mkdir -p data logs checkpoints
mkdir -p data/{kafka,zookeeper,spark,schema-registry}
```

## 🚀 Step-by-Step Pipeline Execution

### **Phase 1: Environment Preparation**

#### **Step 1: Clone and Navigate to Project**
```bash
cd /Users/naveeth/Documents/Stock-market-pipeline
pwd  # Verify you're in the correct directory
```

#### **Step 2: Verify File Structure**
```bash
ls -la
# Should see: docker-compose.yaml, src/, config/, scripts/, etc.
```

#### **Step 3: Set Environment Variables**
```bash
# Either export variables or ensure config/.env exists
source config/.env  # if using .env file
```

#### **Step 4: Validate Alpha Vantage API Key**
```bash
# Test API connectivity
curl "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=${ALPHA_VANTAGE_API_KEY}"
```

### **Phase 2: Infrastructure Startup**

#### **Step 5: Build Docker Images**
```bash
# Build all custom images
make docker-build

# Or manually:
docker-compose build
```

**Expected Output:**
- Successfully built streaming-producer image
- Successfully built streaming-processor image
- Successfully built kafka-connect image

#### **Step 6: Start Core Infrastructure**
```bash
# Start Zookeeper and Kafka first
docker-compose up -d zookeeper kafka

# Wait for services to be ready (30-60 seconds)
sleep 60

# Verify Kafka is running
docker-compose logs kafka | tail -20
```

#### **Step 7: Start Schema Registry**
```bash
# Start Schema Registry
docker-compose up -d schema-registry

# Wait for startup
sleep 30

# Verify Schema Registry health
curl -f http://localhost:8085/subjects || echo "Schema Registry not ready yet"
```

#### **Step 8: Initialize Kafka Topics**
```bash
# Start topic initialization
docker-compose up kafka-topics-init

# Verify topics created
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Expected Topics:**
- stock-quotes-realtime
- stock-intraday-data  
- processed-stock-prices
- processed-trading-volume
- processed-technical-indicators
- data-quality-alerts

#### **Step 9: Initialize Schema Registry**
```bash
# Start schema initialization
docker-compose up schema-registry-init

# Verify schemas registered
curl http://localhost:8085/subjects
```

### **Phase 3: Processing Layer Startup**

#### **Step 10: Start Spark Cluster**
```bash
# Start Spark Master and Worker
docker-compose up -d spark-master spark-worker

# Wait for Spark cluster startup
sleep 45

# Verify Spark Master UI
curl -f http://localhost:18080 || echo "Spark Master not ready yet"
```

#### **Step 11: Start Kafka Connect**
```bash
# Start Kafka Connect
docker-compose up -d kafka-connect

# Wait for Kafka Connect startup
sleep 60

# Verify Kafka Connect health
curl -f http://localhost:8083/connectors || echo "Kafka Connect not ready yet"
```

### **Phase 4: Pipeline Services Startup**

#### **Step 12: Start Data Producer**
```bash
# Start Alpha Vantage Producer
docker-compose up -d streaming-producer

# Wait for producer startup
sleep 30

# Check producer health
curl http://localhost:8081/health
```

**Expected Response:**
```json
{
  "status": "healthy",
  "is_running": true,
  "total_runs": 0,
  "error_count": 0
}
```

#### **Step 13: Start Stream Processor**
```bash
# Start Spark Streaming Processor
docker-compose up -d streaming-processor

# Wait for processor startup
sleep 60

# Check processor health
curl http://localhost:8082/health
```

**Expected Response:**
```json
{
  "status": "healthy",
  "is_running": true,
  "active_queries": 1,
  "healthy_queries": 1
}
```

### **Phase 5: Monitoring & Connect Setup**

#### **Step 14: Start Monitoring Stack**
```bash
# Start Prometheus and Grafana
docker-compose up -d prometheus grafana

# Start Kafka UI
docker-compose up -d kafka-ui

# Wait for startup
sleep 30
```

#### **Step 15: Deploy Kafka Connectors (Optional)**
```bash
# Deploy Bronze S3 Connector (if using S3)
./scripts/deploy-bronze-connector.sh

# Deploy Silver S3 Connector (if using S3)  
./scripts/deploy-silver-connector.sh

# Deploy Gold Snowflake Connector (if using Snowflake)
./scripts/deploy-gold-connector.sh
```

### **Phase 6: Verification & Monitoring**

#### **Step 16: Verify All Services Running**
```bash
# Check all container status
docker-compose ps

# All services should show "Up" status
```

#### **Step 17: Monitor Data Flow**

##### **Check Producer Activity**
```bash
# View producer logs
docker-compose logs -f streaming-producer

# Check producer metrics
curl http://localhost:8081/metrics
```

##### **Check Kafka Topics**
```bash
# View Kafka topics and messages
# Open Kafka UI: http://localhost:8090
# Navigate to Topics → stock-quotes-realtime
```

##### **Check Stream Processing**
```bash
# View processor logs
docker-compose logs -f streaming-processor

# Check Spark UI: http://localhost:18080
# Check processor metrics
curl http://localhost:8082/metrics
```

##### **Check Processed Data**
```bash
# Check processed topics in Kafka UI
# Topics: processed-stock-prices, processed-trading-volume, processed-technical-indicators
```

#### **Step 18: Access Monitoring Dashboards**

- **Kafka UI**: http://localhost:8090
  - Monitor topics, consumers, schema registry
  
- **Spark Master UI**: http://localhost:18080  
  - Monitor Spark applications and resource usage
  
- **Prometheus**: http://localhost:9090
  - Query metrics and check targets
  
- **Grafana**: http://localhost:3000 (admin/admin)
  - View pre-configured dashboards

## 🔍 Health Check Commands

### **Quick Health Verification**
```bash
# Check all service health
echo "=== Producer Health ==="
curl -s http://localhost:8081/health | jq .

echo "=== Processor Health ==="
curl -s http://localhost:8082/health | jq .

echo "=== Kafka Connect Health ==="
curl -s http://localhost:8083/ | jq .

echo "=== Schema Registry Health ==="
curl -s http://localhost:8085/subjects

echo "=== Container Status ==="
docker-compose ps
```

### **Data Flow Verification**
```bash
# Check if data is flowing through topics
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-quotes-realtime \
  --from-beginning \
  --max-messages 5
```

### **Log Monitoring**
```bash
# Monitor all logs in real-time
docker-compose logs -f

# Monitor specific service
docker-compose logs -f streaming-producer
docker-compose logs -f streaming-processor
```

## 🛑 Troubleshooting Common Issues

### **Issue 1: Port Already in Use**
```bash
# Find process using port
lsof -i :9092

# Kill process if needed
kill -9 <PID>
```

### **Issue 2: Alpha Vantage API Rate Limit**
- Check API key validity
- Verify rate limiting settings
- Monitor producer logs for API errors

### **Issue 3: Out of Memory**
```bash
# Increase Docker memory allocation
# Docker Desktop → Settings → Resources → Memory (8GB+)

# Or reduce Spark memory in docker-compose.yaml
```

### **Issue 4: Kafka Connect Fails**
```bash
# Check connector status
curl http://localhost:8083/connectors/bronze-s3-connector/status

# Restart connector
curl -X POST http://localhost:8083/connectors/bronze-s3-connector/restart
```

## 🧹 Cleanup Commands

### **Stop Pipeline**
```bash
# Stop all services
make docker-down

# Or manually
docker-compose down
```

### **Clean Restart**
```bash
# Stop and remove all containers, networks, volumes
docker-compose down -v --remove-orphans

# Remove images (optional)
docker-compose down --rmi all

# Clean restart
make docker-build
make docker-up
```

## 📊 Success Indicators

Your pipeline is running successfully when:

✅ **All services show "Up" status in `docker-compose ps`**
✅ **Producer health check returns "healthy" status**  
✅ **Processor health check shows active queries**
✅ **Kafka topics contain messages (visible in Kafka UI)**
✅ **Spark UI shows running applications**
✅ **Prometheus shows all targets as "UP"**
✅ **No critical errors in logs**
✅ **Data flows from producer → Kafka → processor → processed topics**

Following this checklist ensures your streaming pipeline will start successfully and process real-time stock market data!
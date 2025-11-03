# 🚀 Real-Time Stock Market Streaming Pipeline
### Medallion Architecture | Apache Spark | AWS | Production-Ready

> A production-grade data streaming pipeline that ingests real-time stock market data, processes it through Apache Spark Structured Streaming with advanced technical analysis, and delivers analytics-ready data to AWS Redshift using the Medallion Architecture pattern.

![Architecture Diagram](docs/images/architecture-diagram.png)

---

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.0-black.svg)](https://kafka.apache.org/)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Redshift-orange.svg)](https://aws.amazon.com/)
[![DBT](https://img.shields.io/badge/DBT-1.7.0-orange.svg)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

---

## ✨ What Makes This Special

### 🏗️ **Medallion Architecture (Bronze → Silver → Gold)**
- **Bronze Layer:** Raw data ingestion with Kafka + Avro serialization to AWS S3
- **Silver Layer:** Real-time Spark transformations with comprehensive data quality checks
- **Gold Layer:** Analytics-ready dimensional model in AWS Redshift

### ⚡ **Real-Time Processing at Scale**
- Apache Spark Structured Streaming with micro-batch processing (10-second intervals)
- Advanced technical indicators: RSI (14-period), Bollinger Bands, Moving Averages
- Statistical anomaly detection using Z-score analysis
- Real-time data quality monitoring and validation

### 🔧 **Production-Ready Features**
- **Fault Tolerance:** Automatic recovery with checkpoint-based state management
- **Exactly-Once Processing:** Guaranteed message delivery semantics
- **Comprehensive Monitoring:** Health checks, Spark UI, Kafka UI dashboards
- **Error Handling:** Dead letter queues for failed messages with retry logic
- **Schema Evolution:** Backward-compatible Avro schema management

### 📊 **End-to-End Data Engineering**
- API-to-Warehouse pipeline with full observability
- DBT for warehouse transformations and data modeling
- Dimensional modeling with fact and dimension tables
- Scalable infrastructure using Docker Compose orchestration

---

## 🏗️ Architecture

![Medallion Architecture Flow](docs/images/architecture-diagram.png)

### Data Flow

#### **🥉 Bronze Layer (Raw Ingestion)**
- **Data Source:** Alpha Vantage API for real-time stock market data
- **Message Queue:** Apache Kafka with Avro serialization via Schema Registry
- **Storage:** AWS S3 with time-based partitioning (year/month/day/hour)
- **Topics:** `stock-quotes-realtime`, `stock-intraday-data`
- **Format:** Avro (schema-enforced, backward-compatible)

#### **🥈 Silver Layer (Stream Processing)**
- **Processing Engine:** Apache Spark Structured Streaming (1 Master + 2 Workers)
- **Transformations:**
  - Price metrics calculation (volatility, momentum, trends)
  - Moving averages with sliding windows (5min, 20min, 1hour)
  - Technical indicators (RSI, Bollinger Bands)
  - Anomaly detection using statistical methods
  - Data validation and quality scoring
- **Output:** Processed Kafka topics + Parquet files on S3
- **Topics:** `processed-stock-prices`, `processed-trading-volume`, `processed-technical-indicators`

#### **🥇 Gold Layer (Analytics Warehouse)**
- **Warehouse:** AWS Redshift with JDBC Sink Connector for streaming ingestion
- **Transformations:** DBT models for dimensional modeling
- **Data Model:**
  - **Fact Tables:** `fact_stock_prices`, `fact_trading_volume`
  - **Dimension Tables:** `dim_company`, `dim_date`, `dim_time`
- **Use Cases:** BI dashboards, analytics, ML model training

---

### Tech Stack

| Layer | Technologies | Purpose |
|-------|-------------|---------|
| **Data Ingestion** | Python, Alpha Vantage API, Kafka Producers | API integration and message publishing |
| **Message Queue** | Apache Kafka, Avro, Schema Registry | Event streaming and schema management |
| **Stream Processing** | Apache Spark 3.5.1, PySpark, Structured Streaming | Real-time distributed data processing |
| **Cloud Storage** | AWS S3 (Bronze/Silver layers) | Scalable data lake storage |
| **Data Warehouse** | AWS Redshift (Gold layer) | Analytics-ready data storage |
| **Orchestration** | Kafka Connect, JDBC/S3 Sink Connectors | Automated data movement |
| **Transformation** | DBT 1.7.0, SQL | Warehouse transformations and testing |
| **Infrastructure** | Docker Compose, Multi-container orchestration | Reproducible development environment |
| **Monitoring** | Spark UI, Kafka UI, Health Check APIs | Observability and debugging |

---

## 🔬 Technical Implementation

### Stream Processing Features

#### **Real-Time Analytics**
- **Moving Averages:** 5-minute, 20-minute, and 1-hour sliding window calculations
- **Price Metrics:** Volatility percentage, momentum indicators, trend detection
- **RSI (Relative Strength Index):** 14-period momentum oscillator for overbought/oversold signals
- **Bollinger Bands:** Price envelope detection with standard deviation bands
- **Anomaly Detection:** Statistical outlier identification using Z-score analysis (3σ threshold)

#### **Data Quality & Reliability**
- **Schema Validation:** Strict Avro schema enforcement on every batch
- **Data Quality Scoring:** Automated quality metrics per batch with configurable thresholds
- **Null Detection:** Comprehensive null value identification and handling
- **Alert System:** Real-time alerts published to `data-quality-alerts` topic for critical errors
- **Dead Letter Queues:** Failed records isolated for investigation and replay
- **Validation Rules:** Price range checks, volume validation, timestamp verification

#### **Performance Optimizations**
- **Adaptive Query Execution (AQE):** Dynamic query optimization at runtime
- **Kryo Serialization:** Reduced network overhead with efficient binary serialization
- **Partition Strategy:** 3 partitions per topic for balanced load distribution
- **Checkpointing:** Incremental state saves for fast recovery and exactly-once semantics
- **Watermarking:** 1-minute late data tolerance with event-time processing
- **Memory Management:** Optimized executor memory allocation (60% execution, 30% storage)

---

### Medallion Architecture Benefits

✅ **Separation of Concerns** - Raw, cleaned, and business-ready data isolated by layer
✅ **Data Quality** - Progressive data refinement and validation through pipeline stages
✅ **Flexibility** - Multiple consumers can access data at any transformation stage
✅ **Debugging** - Easy issue tracing through clear pipeline stages
✅ **Compliance** - Raw data preserved in Bronze layer for audit and replay requirements
✅ **Performance** - Optimized storage formats per layer (Avro → Parquet → Redshift)
✅ **Scalability** - Each layer can scale independently based on workload

---

## 📁 Project Structure

```
stock-market-pipeline/
├── src/
│   └── stock_market_pipeline/
│       ├── ingestion/              # Data ingestion layer
│       │   ├── clients/           # Alpha Vantage API clients (real + mock)
│       │   └── producers/         # Kafka producers with Avro serialization
│       ├── processing/            # Stream processing layer
│       │   ├── core/              # Spark streaming engine
│       │   ├── transformations/   # Business logic transformations
│       │   └── producers/         # Output producers for processed data
│       ├── storage/               # Storage layer
│       │   ├── schemas/           # Avro schema definitions
│       │   └── connectors/        # Kafka Connect configurations
│       ├── config/                # Configuration management
│       └── utils/                 # Logging and utilities
├── dbt/                           # DBT project for Redshift
│   ├── models/
│   │   ├── staging/              # Staging views for raw Redshift data
│   │   └── marts/                # Production data marts
│   │       ├── dimensions/       # Dimension tables (SCD Type 1/2)
│   │       └── facts/            # Fact tables (incremental models)
│   └── snapshots/                # SCD Type 2 change tracking
├── docker/
│   ├── compose/                  # Docker Compose configurations
│   └── services/                 # Custom Dockerfiles for each service
├── scripts/
│   ├── connectors/               # Kafka Connect deployment automation
│   ├── infrastructure/           # Cluster management scripts
│   ├── database/                 # Redshift schema initialization
│   └── schemas/                  # Schema Registry initialization
├── config/                       # Environment configurations
├── docs/                         # Technical documentation
└── Makefile                      # Automated operational commands
```

---

## 📊 Monitoring & Observability

### Health Monitoring

#### **Service-Level Health Checks**
- **Producer Health Endpoint:** `http://localhost:8081/health` - API ingestion status
- **Processor Health Endpoint:** `http://localhost:8082/health` - Spark streaming status
- **Kafka Connect Health:** REST API monitoring for connector status

#### **Dashboard Access**
- **Spark Master UI:** `http://localhost:8080` - Job monitoring, resource utilization, DAG visualization
- **Spark Worker UIs:** `http://localhost:8181`, `http://localhost:8182` - Worker-level metrics
- **Kafka UI:** `http://localhost:8090` - Topic inspection, message browsing, consumer lag
- **Schema Registry:** `http://localhost:8085` - Schema versions and compatibility

#### **Data Quality Metrics**
- Real-time quality score tracking per micro-batch
- Validation rule pass/fail rates
- Anomaly detection alerts via dedicated Kafka topic
- Null value percentage monitoring

---

### Fault Tolerance & Recovery

#### **Checkpointing Strategy**
- Incremental state checkpoints every micro-batch
- Exactly-once processing guarantees via offset management
- Automatic recovery from last successful checkpoint
- Configurable checkpoint retention policy

#### **Error Handling**
- **Dead Letter Queues:** Separate topics for failed messages
  - `bronze-s3-connector-dlq` - S3 sink failures
  - `redshift-streaming-dlq` - Redshift ingestion failures
  - `data-quality-alerts` - Validation failures
- **Retry Logic:** Exponential backoff for transient failures (3 retries, 3-second backoff)
- **Circuit Breakers:** Automatic query restart after error threshold detection

#### **Operational Commands**
```bash
make status         # Complete system health check across all services
make logs           # Stream real-time processing logs
make verify         # Quick pipeline verification (topics, messages, connectors)
make clean          # Full cleanup and reset
```

---

## 📚 Technical Documentation

For detailed technical deep-dives and implementation details:

- **[Spark Architecture Explained](docs/spark_architecture_explanation.md)** - How Structured Streaming works under the hood, micro-batching model, window functions
- **[Visual Diagrams](docs/spark_visual_diagrams.md)** - DataFrame transformations, technical indicator calculations, error handling flows
- **[Development Guide](WARP.md)** - Development workflow, testing patterns, code structure, environment setup

---

## 🎯 Key Learning Outcomes

This project demonstrates expertise in:

### **Data Engineering & Architecture**
✅ Real-time data pipeline design and implementation
✅ Medallion architecture pattern (Bronze/Silver/Gold layers)
✅ Dimensional modeling for analytics (fact/dimension tables)
✅ Data quality engineering and validation frameworks
✅ ETL/ELT pipeline development

### **Distributed Systems & Streaming**
✅ Apache Spark for distributed stream processing
✅ Structured Streaming with micro-batch processing
✅ Kafka-based event streaming architecture
✅ Exactly-once processing semantics
✅ Watermarking and late data handling

### **Cloud & Infrastructure**
✅ AWS cloud data engineering (S3, Redshift)
✅ Infrastructure as Code with Docker Compose
✅ Multi-container orchestration and networking
✅ Scalable storage layer design (data lake + warehouse)

### **Production Engineering**
✅ Fault tolerance and automatic recovery mechanisms
✅ Comprehensive monitoring and observability
✅ Error handling with dead letter queues
✅ Performance optimization (AQE, serialization, partitioning)
✅ Operational automation with Makefile

### **Data Transformation & Modeling**
✅ DBT for warehouse transformations and testing
✅ Advanced technical analysis (RSI, Bollinger Bands, moving averages)
✅ Statistical anomaly detection
✅ Schema evolution and management

---

## 📋 System Requirements

- **Docker:** 20.10+ with Docker Compose
- **Memory:** 8GB RAM minimum (12GB recommended for smooth operation)
- **Storage:** 10GB free disk space
- **Network:** Stable internet connection for API ingestion
- **AWS Account:** Active account with S3 and Redshift access
- **API Key:** Alpha Vantage API key (free tier available)

---

## 🔧 Configuration

All system configurations are centralized in `config/.env`:

- **AWS Credentials:** S3 bucket, Redshift endpoint, access keys
- **Alpha Vantage API:** API key and rate limiting settings
- **Kafka Configuration:** Bootstrap servers, topic settings
- **Spark Configuration:** Memory allocation, executor settings
- **Pipeline Tuning:** Batch intervals, watermark delays, checkpoint locations

---

## 📝 License

This project is available for educational and portfolio purposes.

---

## 🤝 Contact

For questions or collaboration opportunities, please reach out via [GitHub Issues](../../issues).

---

**Built with:** Apache Spark | Kafka | AWS | Python | DBT | Docker

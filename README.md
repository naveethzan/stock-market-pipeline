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

## 🏗️ Architecture

![Medallion Architecture Flow](docs/images/architecture-diagram.png)

### Data Flow

#### **🥉 Bronze Layer (Raw Ingestion)**
- **Data Source:** Alpha Vantage API for real-time stock market data
- **Message Queue:** Apache Kafka with Avro serialization via Schema Registry
- **Storage:** AWS S3 with time-based partitioning (year/month/day/hour)
- **Topics:** `stock-quotes-realtime`, `stock-intraday-data`
- **Format:** Avro (schema-enforced, backwards-compatible)

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

### Fault Tolerance & Recovery

#### **Checkpointing Strategy**
- Incremental state checkpoints every micro-batch
- Exactly-once processing guarantees via offset management
- Automatic recovery from the last successful checkpoint
- Configurable checkpoint retention policy

#### **Error Handling**
- **Dead Letter Queues:** Separate topics for failed messages
  - `bronze-s3-connector-dlq` - S3 sink failures
  - `redshift-streaming-dlq` - Redshift ingestion failures
  - `data-quality-alerts` - Validation failures
- **Retry Logic:** Exponential backoff for transient failures (3 retries, 3-second backoff)
- **Circuit Breakers:** Automatic query restart after error threshold detection

**Built with:** Apache Spark | Kafka | AWS | Python | DBT | Docker

# System Flow Diagrams - Stock Market Streaming Pipeline

## 🔄 Complete Data Flow Architecture

```mermaid
graph TB
    %% Data Sources
    AV[Alpha Vantage API<br/>Stock Market Data]
    
    %% Ingestion Layer
    PROD[Alpha Vantage Producer<br/>Python + FastAPI<br/>Port: 8081]
    
    %% Message Broker
    KAF[Apache Kafka Cluster<br/>Topics: stock-quotes-realtime<br/>stock-intraday-data]
    SR[Schema Registry<br/>Avro Schema Management<br/>Port: 8085]
    
    %% Stream Processing
    SPARK[Spark Structured Streaming<br/>Real-time Processing<br/>Port: 8082]
    
    %% Processed Topics
    PROC_TOPICS[Processed Kafka Topics<br/>processed-stock-prices<br/>processed-trading-volume<br/>processed-technical-indicators]
    
    %% Storage Layer - Bronze
    S3_BRONZE[S3 Bronze Layer<br/>Raw Data Storage<br/>Avro Format]
    
    %% Storage Layer - Silver  
    S3_SILVER[S3 Silver Layer<br/>Processed Data<br/>Parquet Format]
    
    %% Storage Layer - Gold
    SF_STAGING[Snowflake Staging<br/>FACT_STOCK_PRICES_STAGING<br/>VARIANT JSON]
    
    %% Dimensional Model
    SF_DIM[Snowflake Dimensions<br/>DIM_COMPANY<br/>DIM_DATE<br/>DIM_TIME]
    SF_FACT[Snowflake Facts<br/>FACT_STOCK_PRICES<br/>FACT_TRADING_VOLUME]
    
    %% ETL Process
    ETL[Snowflake ETL<br/>Dimensional Modeling<br/>SCD Type 2]
    
    %% Kafka Connect
    KC_BRONZE[Kafka Connect<br/>Bronze S3 Connector]
    KC_SILVER[Kafka Connect<br/>Silver S3 Connector] 
    KC_GOLD[Kafka Connect<br/>Gold Snowflake Connector]
    
    %% Monitoring
    PROM[Prometheus<br/>Metrics Collection<br/>Port: 9090]
    GRAF[Grafana<br/>Dashboards<br/>Port: 3000]
    KUI[Kafka UI<br/>Topic Monitoring<br/>Port: 8090]
    
    %% Data Flow
    AV --> PROD
    PROD --> KAF
    PROD --> SR
    KAF --> SPARK
    SPARK --> PROC_TOPICS
    
    %% Bronze Layer
    KAF --> KC_BRONZE
    KC_BRONZE --> S3_BRONZE
    
    %% Silver Layer
    PROC_TOPICS --> KC_SILVER
    KC_SILVER --> S3_SILVER
    
    %% Gold Layer
    PROC_TOPICS --> KC_GOLD
    KC_GOLD --> SF_STAGING
    SF_STAGING --> ETL
    ETL --> SF_DIM
    ETL --> SF_FACT
    
    %% Monitoring
    PROD --> PROM
    SPARK --> PROM
    KAF --> PROM
    PROM --> GRAF
    KAF --> KUI
    
    %% Styling
    classDef ingestion fill:#e1f5fe
    classDef processing fill:#f3e5f5
    classDef storage fill:#e8f5e8
    classDef monitoring fill:#fff3e0
    
    class PROD,AV ingestion
    class SPARK,KAF,SR,PROC_TOPICS processing
    class S3_BRONZE,S3_SILVER,SF_STAGING,SF_DIM,SF_FACT,ETL storage
    class PROM,GRAF,KUI monitoring
```

## 📊 Medallion Architecture Flow

```mermaid
graph LR
    %% Bronze Layer
    subgraph BRONZE["🥉 Bronze Layer - Raw Data"]
        B1[Stock Quotes<br/>Raw JSON]
        B2[Intraday Data<br/>Time Series]
        B3[Market Events<br/>Unstructured]
    end
    
    %% Silver Layer
    subgraph SILVER["🥈 Silver Layer - Processed Data"]
        S1[Validated Stock Prices<br/>+ Technical Indicators]
        S2[Trading Volume Metrics<br/>+ Volume Analysis]
        S3[Technical Indicators<br/>+ Trend Signals]
    end
    
    %% Gold Layer
    subgraph GOLD["🥇 Gold Layer - Analytics Ready"]
        G1[FACT_STOCK_PRICES<br/>Star Schema]
        G2[FACT_TRADING_VOLUME<br/>Star Schema]
        G3[DIM_COMPANY<br/>SCD Type 2]
        G4[DIM_DATE<br/>Calendar]
        G5[DIM_TIME<br/>Market Sessions]
    end
    
    %% Flow
    B1 --> S1
    B2 --> S2
    B1 --> S3
    
    S1 --> G1
    S2 --> G2
    S1 --> G3
    S1 --> G4
    S1 --> G5
    
    %% Data Quality
    DQ[Data Quality<br/>Validation Rules<br/>Alerts]
    S1 --> DQ
    S2 --> DQ
    S3 --> DQ
```

## 🔧 Component Interaction Diagram

```mermaid
graph TD
    %% External Services
    subgraph EXT["External Services"]
        API[Alpha Vantage API<br/>Rate Limited: 5/min]
        AWS[AWS S3<br/>Object Storage]
        SF[Snowflake<br/>Data Warehouse]
    end
    
    %% Docker Services
    subgraph DOCKER["Docker Compose Services"]
        direction TB
        
        subgraph KAFKA_CLUSTER["Kafka Cluster"]
            ZK[Zookeeper<br/>:2181]
            K[Kafka Broker<br/>:9092,:29092]
            SR[Schema Registry<br/>:8085]
            KC[Kafka Connect<br/>:8083]
            KUI[Kafka UI<br/>:8090]
        end
        
        subgraph SPARK_CLUSTER["Spark Cluster"]
            SM[Spark Master<br/>:18080,:7077]
            SW[Spark Worker<br/>Memory: 2GB<br/>Cores: 2]
        end
        
        subgraph PIPELINE_SERVICES["Pipeline Services"]
            PROD[Streaming Producer<br/>:8081]
            PROC[Streaming Processor<br/>:8082]
        end
        
        subgraph MONITORING["Monitoring Stack"]
            PROM[Prometheus<br/>:9090]
            GRAF[Grafana<br/>:3000]
        end
    end
    
    %% Connections
    API --> PROD
    PROD --> K
    K --> PROC
    PROC --> K
    KC --> AWS
    KC --> SF
    
    PROC --> SM
    SM --> SW
    
    %% Health Checks
    PROD --> PROM
    PROC --> PROM
    K --> PROM
    PROM --> GRAF
    K --> KUI
    
    %% Dependencies
    K --> ZK
    KC --> K
    KC --> SR
    PROC --> SR
```

## 📈 Real-time Processing Flow

```mermaid
sequenceDiagram
    participant AV as Alpha Vantage API
    participant PROD as Producer Service
    participant KAFKA as Kafka Topics
    participant SPARK as Spark Processor
    participant S3 as S3 Storage
    participant SF as Snowflake
    
    Note over AV,SF: Every 60 seconds (configurable)
    
    PROD->>AV: GET /query?function=GLOBAL_QUOTE
    AV-->>PROD: Stock Quote JSON
    
    PROD->>PROD: Avro Serialization
    PROD->>KAFKA: Publish to stock-quotes-realtime
    
    KAFKA->>SPARK: Stream consumption
    SPARK->>SPARK: Data transformations:<br/>- Parse JSON<br/>- Calculate indicators<br/>- Validate quality
    
    par Bronze Layer
        KAFKA->>S3: Raw data via S3 Connector<br/>(Avro format)
    and Silver Layer  
        SPARK->>KAFKA: Publish to processed topics
        KAFKA->>S3: Processed data via S3 Connector<br/>(Parquet format)
    and Gold Layer
        KAFKA->>SF: Stream to staging tables<br/>via Snowflake Connector
        SF->>SF: ETL to dimensional model<br/>(Fact & Dimension tables)
    end
    
    Note over SPARK: Continuous 10-second micro-batches
    Note over SF: ETL runs on data arrival
```

## 🏗️ Snowflake Dimensional Model

```mermaid
erDiagram
    %% Dimension Tables
    DIM_COMPANY {
        NUMBER COMPANY_KEY PK
        VARCHAR SYMBOL
        VARCHAR COMPANY_NAME
        VARCHAR SECTOR
        VARCHAR INDUSTRY
        VARCHAR EXCHANGE
        DATE EFFECTIVE_DATE
        DATE EXPIRY_DATE
        BOOLEAN IS_CURRENT
    }
    
    DIM_DATE {
        NUMBER DATE_KEY PK
        DATE DATE_VALUE
        NUMBER YEAR
        NUMBER QUARTER
        NUMBER MONTH
        VARCHAR MONTH_NAME
        NUMBER DAY_OF_MONTH
        BOOLEAN IS_TRADING_DAY
    }
    
    DIM_TIME {
        NUMBER TIME_KEY PK
        TIME TIME_VALUE
        NUMBER HOUR
        NUMBER MINUTE
        VARCHAR MARKET_SESSION
        NUMBER TRADING_DAY_MINUTE
    }
    
    %% Fact Tables
    FACT_STOCK_PRICES {
        NUMBER PRICE_KEY PK
        NUMBER COMPANY_KEY FK
        NUMBER DATE_KEY FK
        NUMBER TIME_KEY FK
        DECIMAL OPEN_PRICE
        DECIMAL HIGH_PRICE
        DECIMAL LOW_PRICE
        DECIMAL CLOSE_PRICE
        NUMBER VOLUME
        DECIMAL SMA_20
        DECIMAL SMA_50
        DECIMAL RSI_14
        VARCHAR DATA_SOURCE
        TIMESTAMP PROCESSING_TIMESTAMP
    }
    
    FACT_TRADING_VOLUME {
        NUMBER VOLUME_KEY PK
        NUMBER COMPANY_KEY FK
        NUMBER DATE_KEY FK
        NUMBER TIME_KEY FK
        NUMBER VOLUME
        DECIMAL VOLUME_WEIGHTED_PRICE
        NUMBER TRADE_COUNT
        DECIMAL VOLUME_RATIO
        VARCHAR DATA_SOURCE
        TIMESTAMP PROCESSING_TIMESTAMP
    }
    
    %% Staging Tables (Kafka Connect)
    FACT_STOCK_PRICES_STAGING {
        VARIANT RECORD_METADATA
        VARIANT RECORD_CONTENT
    }
    
    %% Relationships
    DIM_COMPANY ||--o{ FACT_STOCK_PRICES : "one to many"
    DIM_DATE ||--o{ FACT_STOCK_PRICES : "one to many"
    DIM_TIME ||--o{ FACT_STOCK_PRICES : "one to many"
    
    DIM_COMPANY ||--o{ FACT_TRADING_VOLUME : "one to many"
    DIM_DATE ||--o{ FACT_TRADING_VOLUME : "one to many"
    DIM_TIME ||--o{ FACT_TRADING_VOLUME : "one to many"
```

## 🎯 Data Quality & Monitoring Flow

```mermaid
graph TD
    %% Data Sources
    INPUT[Incoming Data]
    
    %% Validation Layers
    subgraph BRONZE_VAL["Bronze Layer Validation"]
        BV1[Schema Validation]
        BV2[Required Fields Check]
        BV3[Data Type Validation]
    end
    
    subgraph SILVER_VAL["Silver Layer Validation"]
        SV1[Business Rules]
        SV2[Data Range Checks]
        SV3[Cross-field Validation]
        SV4[Technical Indicators]
    end
    
    subgraph GOLD_VAL["Gold Layer Validation"]
        GV1[Referential Integrity]
        GV2[Dimension Key Lookup]
        GV3[SCD Type 2 Logic]
        GV4[Fact Table Constraints]
    end
    
    %% Quality Results
    PASS[✅ Valid Data<br/>Continue Processing]
    FAIL[❌ Invalid Data<br/>Dead Letter Queue]
    ALERT[🚨 Quality Alert<br/>Monitoring Topic]
    
    %% Monitoring
    METRICS[Prometheus Metrics<br/>- Success Rate<br/>- Error Count<br/>- Latency]
    DASHBOARD[Grafana Dashboard<br/>- Real-time Charts<br/>- Alerts<br/>- Health Status]
    
    %% Flow
    INPUT --> BV1
    BV1 --> BV2
    BV2 --> BV3
    BV3 --> SV1
    SV1 --> SV2
    SV2 --> SV3
    SV3 --> SV4
    SV4 --> GV1
    GV1 --> GV2
    GV2 --> GV3
    GV3 --> GV4
    
    %% Results
    GV4 --> PASS
    BV1 --> FAIL
    BV2 --> FAIL
    BV3 --> FAIL
    SV1 --> FAIL
    SV2 --> FAIL
    SV3 --> FAIL
    SV4 --> FAIL
    GV1 --> FAIL
    GV2 --> FAIL
    GV3 --> FAIL
    GV4 --> FAIL
    
    FAIL --> ALERT
    PASS --> METRICS
    ALERT --> METRICS
    METRICS --> DASHBOARD
    
    %% Styling
    classDef validation fill:#e3f2fd
    classDef success fill:#e8f5e8
    classDef error fill:#ffebee
    classDef monitoring fill:#fff3e0
    
    class BV1,BV2,BV3,SV1,SV2,SV3,SV4,GV1,GV2,GV3,GV4 validation
    class PASS success
    class FAIL,ALERT error
    class METRICS,DASHBOARD monitoring
```

## 🚀 Deployment & Scaling Architecture

```mermaid
graph TB
    %% Development Environment
    subgraph DEV["Development Environment"]
        DEV_DOCKER[Docker Compose<br/>Single Machine<br/>All Services Local]
    end
    
    %% Production Environment Options
    subgraph PROD["Production Environment"]
        direction TB
        
        subgraph K8S["Kubernetes Deployment"]
            K8S_KAFKA[Kafka Cluster<br/>3+ Brokers<br/>High Availability]
            K8S_SPARK[Spark on K8s<br/>Auto-scaling<br/>Dynamic Allocation]
            K8S_CONNECT[Kafka Connect<br/>Distributed Mode<br/>Multiple Workers]
        end
        
        subgraph CLOUD["Cloud Services"]
            MSK[Amazon MSK<br/>Managed Kafka]
            EMR[Amazon EMR<br/>Managed Spark]
            SF_PROD[Snowflake<br/>Enterprise Edition]
            S3_PROD[S3<br/>Multi-region<br/>Versioning]
        end
    end
    
    %% CI/CD Pipeline
    subgraph CICD["CI/CD Pipeline"]
        GIT[Git Repository]
        BUILD[Docker Build]
        TEST[Automated Tests<br/>- Unit Tests<br/>- Integration Tests<br/>- E2E Tests]
        DEPLOY[Deployment<br/>- Blue/Green<br/>- Rolling Updates]
    end
    
    %% Monitoring Stack
    subgraph MON["Production Monitoring"]
        PROM_PROD[Prometheus<br/>High Availability<br/>Long-term Storage]
        GRAF_PROD[Grafana<br/>Multi-tenant<br/>Alert Manager]
        LOG[Centralized Logging<br/>ELK Stack<br/>Log Aggregation]
        TRACE[Distributed Tracing<br/>Jaeger/Zipkin<br/>Performance Monitoring]
    end
    
    %% Connections
    DEV --> CICD
    CICD --> PROD
    PROD --> MON
    
    GIT --> BUILD
    BUILD --> TEST
    TEST --> DEPLOY
    DEPLOY --> K8S
    DEPLOY --> CLOUD
```

This comprehensive set of diagrams provides a complete visual understanding of your stock market streaming pipeline architecture, from high-level data flow to detailed component interactions and production deployment considerations.
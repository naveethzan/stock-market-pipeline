# Apache Spark & Structured Streaming Architecture Guide

## 🚀 What is Apache Spark?

Apache Spark is a **unified analytics engine** for large-scale data processing. Think of it as a super-powered calculator that can process massive amounts of data across multiple computers simultaneously.

### Key Concepts:

1. **Distributed Computing**: Instead of one computer doing all the work, Spark spreads the work across many computers (cluster)
2. **In-Memory Processing**: Keeps data in RAM for faster processing (vs traditional disk-based systems)
3. **Fault Tolerance**: If one computer fails, Spark automatically recovers and continues processing
4. **Unified Engine**: Can handle batch processing, streaming, machine learning, and graph processing

## 🌊 What is Structured Streaming?

Structured Streaming is Spark's **real-time data processing** engine. It treats streaming data as a table that's continuously growing - like a spreadsheet that keeps getting new rows added.

### Core Concepts:

```
📊 STREAMING AS A TABLE CONCEPT:

Time 0:    [Row 1] [Row 2] [Row 3]
Time 1:    [Row 1] [Row 2] [Row 3] [Row 4] [Row 5]
Time 2:    [Row 1] [Row 2] [Row 3] [Row 4] [Row 5] [Row 6] [Row 7]
           ↑                                              ↑
        Old Data                                      New Data
```

## 🏗️ Your Streaming Pipeline Architecture

Here's how Spark Structured Streaming works in your financial data pipeline:

```
🔄 COMPLETE DATA FLOW ARCHITECTURE:

┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL DATA SOURCES                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                                        │
│  │ Alpha Vantage   │ ──── Real-time Stock Quotes ────┐                     │
│  │ API             │ ──── Intraday Data ─────────────┤                     │
│  └─────────────────┘                                 │                     │
└─────────────────────────────────────────────────────┼─────────────────────┘
                                                        │
┌─────────────────────────────────────────────────────┼─────────────────────┐
│                        KAFKA LAYER                   │                     │
├─────────────────────────────────────────────────────┼─────────────────────┤
│  ┌─────────────────┐                                 ▼                     │
│  │ Avro Producer   │ ──── Serializes & Publishes ────┐                    │
│  │ Container       │                                  │                    │
│  └─────────────────┘                                  │                    │
│                                                       ▼                    │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                    KAFKA TOPICS                                     │  │
│  │  ┌─────────────────────┐  ┌─────────────────────────────────────┐  │  │
│  │  │ stock-quotes-       │  │ stock-intraday-data                 │  │  │
│  │  │ realtime            │  │ (3 partitions)                      │  │  │
│  │  │ (3 partitions)      │  └─────────────────────────────────────┘  │  │
│  │  └─────────────────────┘                                           │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                                       │
┌─────────────────────────────────────────────────────┼─────────────────────┐
│                    SPARK STREAMING LAYER             │                     │
├─────────────────────────────────────────────────────┼─────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                 SPARK STRUCTURED STREAMING                          │  │
│  │                                                                     │  │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐  │  │
│  │  │ Kafka Source    │    │ Transformations │    │ Multiple Sinks  │  │  │
│  │  │                 │    │                 │    │                 │  │  │
│  │  │ • Consumes      │───▶│ • Parse JSON    │───▶│ • Parquet Files │  │  │
│  │  │ • Deserializes  │    │ • Calculate     │    │ • Kafka Topics  │  │  │
│  │  │ • Validates     │    │ • Enrich        │    │ • Quality Checks│  │  │
│  │  └─────────────────┘    └─────────────────┘    └─────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                                       │
┌─────────────────────────────────────────────────────┼─────────────────────┐
│                      OUTPUT LAYER                    │                     │
├─────────────────────────────────────────────────────┼─────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                    MEDALLION ARCHITECTURE                           │  │
│  │                                                                     │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │  │
│  │  │ Bronze      │  │ Silver      │  │ Gold        │  │ Parquet     │ │  │
│  │  │ (Raw Data)  │  │ (Cleaned)   │  │ (Aggregated)│  │ (Backup)    │ │  │
│  │  │             │  │             │  │             │  │             │ │  │
│  │  │ Kafka Topic │  │ Kafka Topic │  │ Kafka Topic │  │ File System │ │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🧠 Spark Internal Architecture

### 1. Spark Components in Your Pipeline:

```
🏗️ SPARK ARCHITECTURE COMPONENTS:

┌─────────────────────────────────────────────────────────────────────────────┐
│                           SPARK APPLICATION                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        DRIVER PROGRAM                               │    │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │    │
│  │  │ SparkSession    │  │ StreamProcessor │  │ Query Manager   │     │    │
│  │  │ (Entry Point)   │  │ (Your Code)     │  │ (Spark Internal)│     │    │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                        │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐  │
│  │                    CLUSTER MANAGER                                   │  │
│  │                    (Coordinates Resources)                           │  │
│  └─────────────────────────────────┼─────────────────────────────────────┘  │
│                                    │                                        │
│  ┌─────────────────────────────────┼─────────────────────────────────────┐  │
│  │                           EXECUTORS                                  │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │ Executor 1  │  │ Executor 2  │  │ Executor 3  │  │ Executor N  │  │  │
│  │  │             │  │             │  │             │  │             │  │  │
│  │  │ • CPU Cores │  │ • CPU Cores │  │ • CPU Cores │  │ • CPU Cores │  │  │
│  │  │ • Memory    │  │ • Memory    │  │ • Memory    │  │ • Memory    │  │  │
│  │  │ • Cache     │  │ • Cache     │  │ • Cache     │  │ • Cache     │  │  │
│  │  │ • Tasks     │  │ • Tasks     │  │ • Tasks     │  │ • Tasks     │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2. How Your Code Maps to Spark:

```python
# Your StreamProcessor class creates a SparkSession
spark = SparkSession.builder \
    .appName("streaming-pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# This creates the distributed computing environment
```

## 🔄 Structured Streaming Processing Model

### 1. Micro-Batch Processing:

Your pipeline uses **micro-batch processing** - it processes data in small batches every 10 seconds:

```
⏰ MICRO-BATCH TIMELINE:

Time 0s:     [Batch 1] ──── Process ──── Output
Time 10s:    [Batch 2] ──── Process ──── Output  
Time 20s:    [Batch 3] ──── Process ──── Output
Time 30s:    [Batch 4] ──── Process ──── Output

Each batch contains all data that arrived in the last 10 seconds
```

### 2. Watermarking for Late Data:

```
🌊 WATERMARKING CONCEPT:

Current Time: 12:00:30
Watermark: 1 minute delay

Accept data with timestamps: 12:00:30 - 1 minute = 11:59:30 or later
Drop data with timestamps: Earlier than 11:59:30

This handles network delays and out-of-order data
```

## 📊 Data Transformations in Your Pipeline

### 1. Raw Data Processing:

```python
# Your pipeline receives this raw JSON from Kafka:
{
    "01. symbol": "AAPL",
    "05. price": "150.25",
    "06. volume": "1000000",
    "09. change": "2.50"
}
```

### 2. Spark Transformations Applied:

```
🔄 TRANSFORMATION PIPELINE:

Raw Kafka Data
      │
      ▼
┌─────────────────┐
│ Parse JSON      │ ──── Extract fields, convert types
│ Clean Data      │
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Calculate       │ ──── Price volatility, momentum
│ Price Metrics   │      Volume weighted price
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Moving          │ ──── 5min, 20min, 1hour averages
│ Averages        │      Using window functions
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Technical       │ ──── RSI, Bollinger Bands
│ Indicators      │      Support/Resistance levels
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Anomaly         │ ──── Statistical outlier detection
│ Detection       │      Z-score calculations
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Data Quality    │ ──── Validation rules
│ Checks          │      Missing data flags
└─────────────────┘
      │
      ▼
Enriched Output Data
```

## 🪟 Window Functions Explained

Window functions are crucial for financial analysis. Here's how they work:

### 1. Time-Based Windows:

```
📈 MOVING AVERAGE CALCULATION:

Time:     10:00  10:01  10:02  10:03  10:04  10:05
Price:    100    102    101    103    105    104
          │      │      │      │      │      │
          └──────┴──────┴──────┴──────┴──────┘
                    5-minute window
                    
5-min SMA at 10:05 = (100 + 102 + 101 + 103 + 105 + 104) / 6 = 102.5
```

### 2. Partition-Based Windows:

```python
# Your code creates windows partitioned by stock symbol
window_5min = (Window.partitionBy("symbol")
              .orderBy("processing_timestamp")
              .rangeBetween(-300, 0))  # 5 minutes in seconds

# This means each stock gets its own separate calculation
```

### 3. Window Types in Your Pipeline:

```
🪟 WINDOW TYPES USED:

1. Range Windows (Time-based):
   - 5 minutes: Recent price trends
   - 20 minutes: Medium-term trends  
   - 1 hour: Long-term trends

2. Row Windows (Count-based):
   - Last 14 rows: RSI calculation
   - Last 20 rows: Bollinger Bands
   - Last 20 rows: Anomaly detection
```

## 🎯 Specific Transformations in Your Code

### 1. Price Volatility Calculation:

```python
# From your transformations.py
.withColumn("price_volatility", 
           F.when(F.col("current_price") > 0,
                (F.col("high_price") - F.col("low_price")) / F.col("current_price") * 100)
            .otherwise(0.0))
```

**What this does:**
- Measures how much the price fluctuated during the trading period
- Higher volatility = more risky/volatile stock
- Formula: (High - Low) / Current Price × 100

### 2. RSI (Relative Strength Index):

```python
# Technical indicator for momentum
df_with_rsi = (df_with_changes
              .withColumn("avg_gain", F.avg("gain").over(window_rsi))
              .withColumn("avg_loss", F.avg("loss").over(window_rsi))
              .withColumn("rsi_14", 100 - (100 / (1 + F.col("rs")))))
```

**What this does:**
- Measures if a stock is overbought (RSI > 70) or oversold (RSI < 30)
- Uses last 14 price changes to calculate momentum
- Helps traders decide when to buy or sell

### 3. Anomaly Detection:

```python
# Statistical outlier detection
.withColumn("price_z_score",
           F.when(F.col("price_std") > 0,
                (F.col("price_col") - F.col("price_mean")) / F.col("price_std"))
            .otherwise(0))
.withColumn("is_price_anomaly",
           F.abs(F.col("price_z_score")) > z_threshold)
```

**What this does:**
- Detects unusual price movements (potential news events, errors)
- Uses Z-score: measures how many standard deviations away from normal
- Flags prices that are more than 3 standard deviations from average

## 🏛️ Medallion Architecture Implementation

Your pipeline implements a **medallion architecture** with three layers:

```
🥉 BRONZE LAYER (Raw Data):
┌─────────────────────────────────────────┐
│ • Raw data from Alpha Vantage API       │
│ • Minimal processing                    │
│ • Avro serialized                      │
│ • Stored in: stock-quotes-realtime     │
└─────────────────────────────────────────┘

🥈 SILVER LAYER (Cleaned Data):
┌─────────────────────────────────────────┐
│ • Parsed and validated data            │
│ • Data quality checks applied          │
│ • Basic transformations                │
│ • Stored in: processed-stock-prices    │
└─────────────────────────────────────────┘

🥇 GOLD LAYER (Business Ready):
┌─────────────────────────────────────────┐
│ • Aggregated data                      │
│ • Technical indicators calculated      │
│ • Ready for analytics/ML               │
│ • Stored in: processed-technical-...   │
└─────────────────────────────────────────┘
```

## ⚡ Performance Optimizations

### 1. Adaptive Query Execution (AQE):

```python
# Your configuration enables AQE
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**What this does:**
- Spark automatically optimizes queries during execution
- Combines small partitions to reduce overhead
- Adjusts join strategies based on actual data sizes

### 2. Kryo Serialization:

```python
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

**What this does:**
- Faster serialization than Java's default
- Reduces network traffic between Spark nodes
- Improves overall performance by 10-30%

### 3. Checkpointing:

```python
.option("checkpointLocation", checkpoint_path)
```

**What this does:**
- Saves processing progress to disk
- Enables fault tolerance - if Spark crashes, it resumes from last checkpoint
- Prevents data loss and duplicate processing

## 🔍 Monitoring and Health Checks

Your pipeline includes comprehensive monitoring:

### 1. Query Status Monitoring:

```python
def get_query_status(self, query_name: str) -> dict:
    # Returns detailed status of each streaming query
    # Including: batch_id, input_rate, processing_rate, errors
```

### 2. Data Quality Monitoring:

```python
def validate_silver_layer_data(self, batch_df, data_type):
    # Validates each batch of data
    # Checks for: null values, data ranges, schema compliance
```

### 3. Health Check Endpoints:

```
GET /health     - Overall system health
GET /metrics    - Detailed performance metrics  
GET /queries    - Status of all streaming queries
POST /queries/{name}/restart - Restart specific query
```

## 🚨 Error Handling and Fault Tolerance

### 1. Automatic Recovery:

```python
# Your pipeline handles failures gracefully
if self.error_count > 10:
    logger.error("High error rate detected, attempting to restart queries")
    self._restart_queries()
```

### 2. Dead Letter Queue:

```python
# Failed records are sent to separate topic for investigation
if critical_errors:
    logger.error("Critical validation errors, publishing alerts")
    self.publish_data_quality_alerts(validation_results)
```

### 3. Watermarking for Late Data:

```python
# Handles out-of-order data gracefully
watermarked_df = df.withWatermark("processing_timestamp", "1 minute")
```

## 🎯 Why These Transformations Matter

### 1. **Moving Averages**:
- **Purpose**: Smooth out price fluctuations to identify trends
- **Business Value**: Helps traders identify buy/sell signals
- **Example**: If current price > 5-min average, trend is "up"

### 2. **Volatility Calculation**:
- **Purpose**: Measure risk level of each stock
- **Business Value**: Risk management, portfolio optimization
- **Example**: High volatility stocks need different trading strategies

### 3. **Technical Indicators (RSI, Bollinger Bands)**:
- **Purpose**: Identify overbought/oversold conditions
- **Business Value**: Timing market entry/exit points
- **Example**: RSI > 70 might signal "sell", RSI < 30 might signal "buy"

### 4. **Anomaly Detection**:
- **Purpose**: Identify unusual market events
- **Business Value**: Risk management, fraud detection
- **Example**: Sudden 50% price jump might indicate news event or data error

### 5. **Data Quality Checks**:
- **Purpose**: Ensure data reliability
- **Business Value**: Prevent bad trading decisions based on bad data
- **Example**: Negative prices or null values indicate data problems

## 🔄 Complete Data Flow Example

Let's trace a single stock quote through your entire pipeline:

```
📈 COMPLETE DATA FLOW EXAMPLE:

1. Alpha Vantage API Response:
   {
     "01. symbol": "AAPL",
     "05. price": "150.25",
     "06. volume": "1000000",
     "09. change": "2.50"
   }

2. Kafka Message (Bronze Layer):
   - Serialized with Avro schema
   - Published to: stock-quotes-realtime
   - Partitioned by symbol

3. Spark Consumption:
   - Reads from Kafka every 10 seconds
   - Deserializes Avro data
   - Creates DataFrame

4. Transformations Applied:
   - Parse: symbol="AAPL", current_price=150.25, volume=1000000
   - Calculate: price_volatility=2.1%, price_momentum=1.69%
   - Moving Averages: sma_5min=149.80, sma_20min=148.90
   - Technical: rsi_14=65.2, bb_position=0.7
   - Quality: data_quality_score=1.0 (perfect)
   - Anomaly: is_price_anomaly=false, anomaly_score=1.2

5. Output (Silver Layer):
   {
     "symbol": "AAPL",
     "current_price": 150.25,
     "volume": 1000000,
     "price_volatility": 2.1,
     "sma_5min": 149.80,
     "sma_20min": 148.90,
     "price_trend_5min": "up",
     "rsi_14": 65.2,
     "data_quality_score": 1.0,
     "processing_timestamp": "2024-01-15T10:30:00Z"
   }

6. Multiple Outputs:
   - Kafka Topic: processed-stock-prices (Silver)
   - Kafka Topic: processed-technical-indicators (Gold)
   - Parquet File: /tmp/streaming-output/stock_quotes/
   - Monitoring: Health metrics updated
```

This enriched data is now ready for:
- Real-time dashboards
- Trading algorithms  
- Risk management systems
- Machine learning models
- Historical analysis

The beauty of Spark Structured Streaming is that it handles all the complexity of distributed processing, fault tolerance, and exactly-once processing guarantees, while you focus on the business logic of transforming financial data into actionable insights! 🚀
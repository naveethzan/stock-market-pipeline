# Practical Transformation Example: AAPL Stock Processing

## 📈 Real Example: Processing Apple (AAPL) Stock Data

Let's trace a real Apple stock quote through your entire Spark Structured Streaming pipeline to see exactly how each transformation works.

## 🔄 Step-by-Step Data Transformation

### Step 1: Raw Data from Alpha Vantage API

```json
{
    "Global Quote": {
        "01. symbol": "AAPL",
        "02. open": "150.00",
        "03. high": "152.50",
        "04. low": "149.75",
        "05. price": "151.25",
        "06. volume": "45678900",
        "07. latest trading day": "2024-01-15",
        "08. previous close": "149.50",
        "09. change": "1.75",
        "10. change percent": "1.17%"
    }
}
```

### Step 2: Kafka Message (Bronze Layer)

```json
{
    "01. symbol": "AAPL",
    "02. open": "150.00",
    "03. high": "152.50",
    "04. low": "149.75",
    "05. price": "151.25",
    "06. volume": "45678900",
    "07. latest trading day": "2024-01-15",
    "08. previous close": "149.50",
    "09. change": "1.75",
    "10. change percent": "1.17%",
    "_producer_metadata": {
        "producer_timestamp": "2024-01-15T14:30:15Z",
        "producer_version": "1.0",
        "serialization_format": "avro"
    }
}
```

### Step 3: Spark DataFrame After Parsing

```python
# After parse_kafka_messages() function
DataFrame Schema:
root
 |-- symbol: string
 |-- open_price: double
 |-- high_price: double
 |-- low_price: double
 |-- current_price: double
 |-- volume: long
 |-- previous_close: double
 |-- change: double
 |-- change_percent: double
 |-- producer_timestamp: timestamp
 |-- processing_timestamp: timestamp
 |-- kafka_timestamp: timestamp

# Data Row:
{
    "symbol": "AAPL",
    "open_price": 150.00,
    "high_price": 152.50,
    "low_price": 149.75,
    "current_price": 151.25,
    "volume": 45678900,
    "previous_close": 149.50,
    "change": 1.75,
    "change_percent": 1.17,
    "producer_timestamp": "2024-01-15T14:30:15Z",
    "processing_timestamp": "2024-01-15T14:30:20Z",
    "kafka_timestamp": "2024-01-15T14:30:16Z"
}
```

### Step 4: Price Metrics Calculation

```python
# From calculate_price_metrics() in transformations.py

# 1. Price Change Absolute
price_change_abs = abs(1.75) = 1.75

# 2. Price Volatility
price_volatility = ((152.50 - 149.75) / 151.25) * 100
                 = (2.75 / 151.25) * 100
                 = 1.82%

# 3. Volume Weighted Price (simplified for single data point)
volume_weighted_price = (151.25 * 45678900) / 45678900 = 151.25

# 4. Price Momentum
price_momentum = (151.25 - 149.50) / 149.50
               = 1.75 / 149.50
               = 0.0117 (1.17%)

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "volume": 45678900,
    "change": 1.75,
    "price_change_abs": 1.75,
    "price_volatility": 1.82,
    "volume_weighted_price": 151.25,
    "price_momentum": 0.0117,
    # ... other fields
}
```

### Step 5: Market Classification

```python
# From classify_market_data() function

# 1. Market Cap Indicator
market_value = 151.25 * 45678900 = 6,909,183,125 (> 1M)
market_cap_indicator = "large"

# 2. Trading Session (assuming processing at 2:30 PM EST)
hour = 14  # 2 PM
trading_session = "regular"  # between 9 AM and 4 PM

# 3. Volume Category
volume = 45678900 (> 1M)
volume_category = "high"

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "volume": 45678900,
    "market_cap_indicator": "large",
    "trading_session": "regular",
    "volume_category": "high",
    # ... other fields
}
```

### Step 6: Moving Averages (Window Functions)

Let's assume we have historical data for AAPL over the last 20 minutes:

```python
# Historical prices for AAPL (last 20 minutes)
price_history = [
    {"time": "14:10", "price": 150.80},
    {"time": "14:11", "price": 150.95},
    {"time": "14:12", "price": 151.10},
    {"time": "14:13", "price": 150.85},
    {"time": "14:14", "price": 151.00},
    {"time": "14:15", "price": 151.15},  # 5-min window starts here
    {"time": "14:16", "price": 151.30},
    {"time": "14:17", "price": 151.05},
    {"time": "14:18", "price": 151.20},
    {"time": "14:19", "price": 151.35},
    {"time": "14:20", "price": 151.25},  # Current price
]

# 5-Minute Simple Moving Average (last 6 data points)
sma_5min = (151.15 + 151.30 + 151.05 + 151.20 + 151.35 + 151.25) / 6
         = 907.30 / 6 = 151.22

# 20-Minute Simple Moving Average (all 11 data points)
sma_20min = sum(all_prices) / 11 = 1662.00 / 11 = 151.09

# Volume SMA (assuming volume data available)
volume_sma_5min = 44000000  # average volume last 5 minutes

# Price Trend
price_trend_5min = "up" if 151.25 > 151.22 else "down"
                 = "up" (151.25 > 151.22)

# Volume Ratio
volume_ratio = 45678900 / 44000000 = 1.04

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "sma_5min": 151.22,
    "sma_20min": 151.09,
    "sma_1hour": 150.95,  # calculated similarly
    "volume_sma_5min": 44000000,
    "price_trend_5min": "up",
    "volume_ratio": 1.04,
    # ... other fields
}
```

### Step 7: Technical Indicators

```python
# RSI Calculation (14-period)
# Assuming we have 14 periods of price changes
gains = [0.15, 0.15, 0, 0.15, 0.15, 0.15, 0, 0.15, 0.15, 0, 0, 0.15, 0.10, 0.03]
losses = [0, 0, 0.25, 0, 0, 0, 0.25, 0, 0, 0.15, 0.10, 0, 0, 0]

avg_gain = sum(gains) / 14 = 1.37 / 14 = 0.098
avg_loss = sum(losses) / 14 = 0.75 / 14 = 0.054

rs = avg_gain / avg_loss = 0.098 / 0.054 = 1.81
rsi_14 = 100 - (100 / (1 + 1.81)) = 100 - 35.6 = 64.4

# Bollinger Bands (20-period)
bb_middle = sma_20min = 151.09
bb_std = 0.85  # standard deviation of 20 prices
bb_upper = 151.09 + (2 * 0.85) = 152.79
bb_lower = 151.09 - (2 * 0.85) = 149.39

bb_position = (151.25 - 149.39) / (152.79 - 149.39)
            = 1.86 / 3.40 = 0.55

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "rsi_14": 64.4,
    "bb_middle": 151.09,
    "bb_upper": 152.79,
    "bb_lower": 149.39,
    "bb_position": 0.55,
    # ... other fields
}
```

### Step 8: Anomaly Detection

```python
# Statistical anomaly detection using Z-score
# Using last 20 data points for statistical baseline

price_mean = 151.09  # mean of last 20 prices
price_std = 0.85     # standard deviation

# Z-score calculation
price_z_score = (151.25 - 151.09) / 0.85 = 0.16 / 0.85 = 0.19

# Volume anomaly
volume_mean = 42000000  # average volume
volume_std = 5000000    # volume standard deviation

volume_z_score = (45678900 - 42000000) / 5000000 = 0.74

# Anomaly flags (threshold = 3.0)
is_price_anomaly = abs(0.19) > 3.0 = False
is_volume_anomaly = abs(0.74) > 3.0 = False
anomaly_score = max(0.19, 0.74) = 0.74

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "price_z_score": 0.19,
    "volume_z_score": 0.74,
    "is_price_anomaly": False,
    "is_volume_anomaly": False,
    "anomaly_score": 0.74,
    # ... other fields
}
```

### Step 9: Data Quality Checks

```python
# Data quality validation
has_null_price = False      # price is not null
has_zero_price = False      # price > 0
has_negative_price = False  # price > 0
has_null_volume = False     # volume is not null
has_negative_volume = False # volume > 0

# Price range validation
price_range_valid = (152.50 >= 149.75) and  # high >= low
                   (151.25 >= 149.75) and   # current >= low
                   (151.25 <= 152.50)       # current <= high
                 = True

# Overall data quality score
data_quality_score = 1.0  # Perfect quality

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "has_null_price": False,
    "has_zero_price": False,
    "has_negative_price": False,
    "price_range_valid": True,
    "data_quality_score": 1.0,
    # ... other fields
}
```

### Step 10: Market Context Enrichment

```python
# Market context information
trading_day = "2024-01-15"
trading_hour = 14
trading_minute = 30
is_market_hours = True  # 14 is between 9 and 16

market_session_detailed = "regular"  # 14 is between 9 and 16
day_of_week = 2  # Monday = 2 in Spark (Sunday = 1)
is_weekend = False  # Monday is not weekend

# Updated DataFrame:
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "trading_day": "2024-01-15",
    "trading_hour": 14,
    "is_market_hours": True,
    "market_session_detailed": "regular",
    "day_of_week": 2,
    "is_weekend": False,
    # ... other fields
}
```

## 📊 Final Enriched DataFrame

After all transformations, here's the complete enriched data for AAPL:

```python
{
    # Original data
    "symbol": "AAPL",
    "open_price": 150.00,
    "high_price": 152.50,
    "low_price": 149.75,
    "current_price": 151.25,
    "volume": 45678900,
    "previous_close": 149.50,
    "change": 1.75,
    "change_percent": 1.17,
    
    # Timestamps
    "producer_timestamp": "2024-01-15T14:30:15Z",
    "processing_timestamp": "2024-01-15T14:30:20Z",
    "kafka_timestamp": "2024-01-15T14:30:16Z",
    
    # Price metrics
    "price_change_abs": 1.75,
    "price_volatility": 1.82,
    "volume_weighted_price": 151.25,
    "price_momentum": 0.0117,
    
    # Market classification
    "market_cap_indicator": "large",
    "trading_session": "regular",
    "volume_category": "high",
    
    # Moving averages
    "sma_5min": 151.22,
    "sma_20min": 151.09,
    "sma_1hour": 150.95,
    "volume_sma_5min": 44000000,
    "price_trend_5min": "up",
    "volume_ratio": 1.04,
    
    # Technical indicators
    "rsi_14": 64.4,
    "bb_middle": 151.09,
    "bb_upper": 152.79,
    "bb_lower": 149.39,
    "bb_position": 0.55,
    
    # Anomaly detection
    "price_z_score": 0.19,
    "volume_z_score": 0.74,
    "is_price_anomaly": False,
    "is_volume_anomaly": False,
    "anomaly_score": 0.74,
    
    # Data quality
    "has_null_price": False,
    "has_zero_price": False,
    "has_negative_price": False,
    "has_null_volume": False,
    "has_negative_volume": False,
    "price_range_valid": True,
    "data_quality_score": 1.0,
    
    # Market context
    "trading_day": "2024-01-15",
    "trading_hour": 14,
    "trading_minute": 30,
    "is_market_hours": True,
    "market_session_detailed": "regular",
    "day_of_week": 2,
    "is_weekend": False
}
```

## 🎯 Business Value of Each Transformation

### 1. **Price Volatility (1.82%)**
- **Meaning**: AAPL's price fluctuated 1.82% during this period
- **Use Case**: Risk assessment - low volatility indicates stable stock
- **Trading Decision**: Safe for conservative portfolios

### 2. **RSI (64.4)**
- **Meaning**: Momentum indicator showing buying/selling pressure
- **Use Case**: RSI between 30-70 is neutral (not overbought/oversold)
- **Trading Decision**: No immediate buy/sell signal

### 3. **Price Trend (up)**
- **Meaning**: Current price (151.25) > 5-min average (151.22)
- **Use Case**: Short-term trend identification
- **Trading Decision**: Positive momentum, potential buy signal

### 4. **Bollinger Band Position (0.55)**
- **Meaning**: Price is 55% between lower and upper bands
- **Use Case**: Price is in middle-upper range, not at extremes
- **Trading Decision**: No strong buy/sell signal from BB

### 5. **Volume Ratio (1.04)**
- **Meaning**: Current volume is 4% higher than recent average
- **Use Case**: Confirms price movement with volume support
- **Trading Decision**: Slight increase in interest

### 6. **Data Quality Score (1.0)**
- **Meaning**: Perfect data quality, no issues detected
- **Use Case**: Confidence in using this data for decisions
- **Trading Decision**: Safe to act on this data

## 🔄 Output to Medallion Architecture

This enriched data is then published to different Kafka topics:

### Silver Layer (processed-stock-prices):
```json
{
    "symbol": "AAPL",
    "current_price": 151.25,
    "volume": 45678900,
    "price_volatility": 1.82,
    "price_trend_5min": "up",
    "data_quality_score": 1.0,
    "processing_timestamp": "2024-01-15T14:30:20Z",
    "data_layer": "silver",
    "record_type": "stock_price"
}
```

### Gold Layer (processed-technical-indicators):
```json
{
    "symbol": "AAPL",
    "rsi_14": 64.4,
    "bb_position": 0.55,
    "sma_5min": 151.22,
    "sma_20min": 151.09,
    "anomaly_score": 0.74,
    "trading_signal": "neutral",
    "processing_timestamp": "2024-01-15T14:30:20Z",
    "data_layer": "gold",
    "record_type": "technical_indicators"
}
```

This shows how your Spark Structured Streaming pipeline transforms raw financial data into actionable business intelligence! 🚀📈
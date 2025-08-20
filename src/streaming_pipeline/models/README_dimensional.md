# Dimensional Data Modeling

This module provides comprehensive dimensional data modeling capabilities for the streaming pipeline, including fact and dimension table creation, SCD Type 2 logic, and data quality validation.

## Overview

The dimensional modeling implementation follows the star schema design pattern with:

- **Dimension Tables**: `dim_company`, `dim_date`, `dim_time`
- **Fact Tables**: `fact_stock_prices`, `fact_trading_volume`
- **SCD Type 2**: Slowly Changing Dimension support for historical tracking
- **Data Quality**: Comprehensive validation and quality checks

## Components

### 1. DimensionalModelBuilder (`dimensional.py`)

Core class for building dimensional model tables from streaming stock data.

**Key Features:**
- Schema definitions for all dimension and fact tables
- Dimension table builders with proper surrogate keys
- SCD Type 2 logic for tracking historical changes
- Fact table builders with proper foreign key relationships

**Usage:**
```python
from streaming_pipeline.models import DimensionalModelBuilder

builder = DimensionalModelBuilder(spark)

# Build dimensions
dim_company = builder.build_dim_company(stock_data)
dim_date = builder.build_dim_date(start_date, end_date)
dim_time = builder.build_dim_time()

# Build facts
fact_prices = builder.build_fact_stock_prices(stock_data, dim_company, dim_date, dim_time)
```

### 2. DataQualityValidator (`data_quality.py`)

Comprehensive data quality validation framework for dimensional tables.

**Validation Types:**
- **Not Null**: Required field validation
- **Range**: Numeric range validation
- **Format**: Pattern and allowed values validation
- **Uniqueness**: Duplicate detection
- **Business Rules**: Custom business logic validation

**Usage:**
```python
from streaming_pipeline.models import DataQualityValidator

validator = DataQualityValidator(spark)

# Validate dimension tables
company_results = validator.validate_dim_company(dim_company)
date_results = validator.validate_dim_date(dim_date)

# Generate quality report
report = validator.generate_data_quality_report(all_results)
```

### 3. DimensionalPipeline (`dimensional_pipeline.py`)

High-level orchestration class that integrates all dimensional modeling components.

**Features:**
- End-to-end processing of streaming batches
- Automatic SCD Type 2 handling
- Integrated data quality validation
- Save/load functionality for dimensional models

**Usage:**
```python
from streaming_pipeline.models import DimensionalPipeline

pipeline = DimensionalPipeline(spark)

# Process streaming batch
dimensional_tables = pipeline.process_streaming_batch(stock_data)

# Generate quality report
quality_report = pipeline.generate_quality_report(dimensional_tables)

# Save dimensional model
pipeline.save_dimensional_model(dimensional_tables, "/path/to/output")
```

## Dimensional Model Schema

### Dimension Tables

#### dim_company
- **Purpose**: Company master data with SCD Type 2 support
- **Key Columns**: `company_key` (surrogate), `symbol` (natural key)
- **SCD Columns**: `company_name`, `sector`, `industry`, `exchange`
- **Temporal Columns**: `effective_date`, `expiry_date`, `is_current`

#### dim_date
- **Purpose**: Date dimension with calendar attributes
- **Key Columns**: `date_key` (YYYYMMDD format)
- **Attributes**: Year, quarter, month, day, fiscal periods, weekend/holiday flags

#### dim_time
- **Purpose**: Time dimension with market session information
- **Key Columns**: `time_key` (HHMM format)
- **Attributes**: Hour, minute, market session (PRE_MARKET, REGULAR, AFTER_HOURS)

### Fact Tables

#### fact_stock_prices
- **Purpose**: Stock price facts with technical indicators
- **Measures**: OHLC prices, volume, adjusted close, technical indicators
- **Dimensions**: Links to company, date, and time dimensions

#### fact_trading_volume
- **Purpose**: Trading volume facts and volume-based metrics
- **Measures**: Volume, volume-weighted price, volume indicators
- **Dimensions**: Links to company, date, and time dimensions

## Data Quality Rules

### Dimension Validation
- **Company**: Symbol format, uniqueness, current record consistency
- **Date**: Date range validation, calendar consistency
- **Time**: Time range validation, market session logic

### Fact Validation
- **Prices**: Positive values, price consistency (High >= Low, etc.)
- **Volume**: Non-negative values, reasonable ranges
- **Referential Integrity**: Valid foreign key relationships

## SCD Type 2 Implementation

The implementation supports Slowly Changing Dimensions Type 2 for tracking historical changes:

1. **Change Detection**: Compares new data with existing records
2. **Record Expiration**: Sets `expiry_date` and `is_current=False` for changed records
3. **New Record Creation**: Creates new records with `effective_date` and `is_current=True`
4. **History Preservation**: Maintains complete historical timeline

## Example Usage

See `example_dimensional_usage.py` for a complete example demonstrating:
- Sample data creation
- Dimensional model building
- Data quality validation
- SCD Type 2 processing
- Quality report generation

## Testing

Run validation checks:
```bash
python src/streaming_pipeline/models/validate_dimensional.py
```

For full testing with PySpark:
```bash
python -m pytest src/streaming_pipeline/models/test_dimensional.py -v
```

## Requirements

The dimensional modeling components require:
- PySpark 3.4+
- Python 3.8+
- Pandas (for data manipulation)
- PyArrow (for Parquet support)

## Integration with Streaming Pipeline

The dimensional modeling components integrate with the broader streaming pipeline:

1. **Stream Processor**: Calls dimensional pipeline for each micro-batch
2. **Data Producer**: Provides enriched stock data with company metadata
3. **Storage Layer**: Saves dimensional model to S3/Snowflake
4. **Monitoring**: Tracks data quality metrics and validation results

## Performance Considerations

- **Partitioning**: Fact tables partitioned by date for optimal query performance
- **Clustering**: Tables clustered on frequently queried columns
- **Caching**: Dimension tables cached for repeated fact table joins
- **Incremental Processing**: SCD Type 2 logic optimized for streaming updates

## Future Enhancements

- **SCD Type 1**: Support for overwrite-style dimension updates
- **SCD Type 3**: Support for previous value tracking
- **Data Lineage**: Track data transformation lineage
- **Advanced Metrics**: More sophisticated data quality metrics
- **Schema Evolution**: Automatic schema evolution handling
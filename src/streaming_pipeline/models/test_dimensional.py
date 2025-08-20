"""
Unit tests for dimensional data modeling components.
"""

import pytest
from datetime import datetime, date, time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit

from dimensional import DimensionalModelBuilder, DimensionConfig
from data_quality import DataQualityValidator, ValidationRule


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("DimensionalModelTest") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


@pytest.fixture
def dimensional_builder(spark):
    """Create DimensionalModelBuilder instance."""
    return DimensionalModelBuilder(spark)


@pytest.fixture
def data_quality_validator(spark):
    """Create DataQualityValidator instance."""
    return DataQualityValidator(spark)


@pytest.fixture
def sample_stock_data(spark):
    """Create sample stock data for testing."""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("open", DecimalType(18, 4), True),
        StructField("high", DecimalType(18, 4), True),
        StructField("low", DecimalType(18, 4), True),
        StructField("close", DecimalType(18, 4), True),
        StructField("volume", IntegerType(), True),
        StructField("adjusted_close", DecimalType(18, 4), True),
        StructField("company_name", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True)
    ])
    
    data = [
        ("AAPL", datetime(2024, 1, 15, 10, 30), 150.00, 152.50, 149.50, 151.75, 1000000, 151.75,
         "Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ", "USD", "USA"),
        ("GOOGL", datetime(2024, 1, 15, 10, 30), 2800.00, 2825.00, 2790.00, 2810.50, 500000, 2810.50,
         "Alphabet Inc.", "Technology", "Internet Services", "NASDAQ", "USD", "USA"),
        ("MSFT", datetime(2024, 1, 15, 10, 30), 380.00, 385.00, 378.00, 382.25, 750000, 382.25,
         "Microsoft Corporation", "Technology", "Software", "NASDAQ", "USD", "USA")
    ]
    
    return spark.createDataFrame(data, schema)


class TestDimensionalModelBuilder:
    """Test cases for DimensionalModelBuilder."""
    
    def test_create_schemas(self, dimensional_builder):
        """Test schema creation methods."""
        # Test dim_company schema
        company_schema = dimensional_builder.create_dim_company_schema()
        assert len(company_schema.fields) == 14
        assert "company_key" in [f.name for f in company_schema.fields]
        assert "symbol" in [f.name for f in company_schema.fields]
        
        # Test dim_date schema
        date_schema = dimensional_builder.create_dim_date_schema()
        assert len(date_schema.fields) == 14
        assert "date_key" in [f.name for f in date_schema.fields]
        assert "date_value" in [f.name for f in date_schema.fields]
        
        # Test dim_time schema
        time_schema = dimensional_builder.create_dim_time_schema()
        assert len(time_schema.fields) == 9
        assert "time_key" in [f.name for f in time_schema.fields]
        assert "market_session" in [f.name for f in time_schema.fields]
        
        # Test fact schemas
        fact_prices_schema = dimensional_builder.create_fact_stock_prices_schema()
        assert len(fact_prices_schema.fields) == 22
        assert "price_key" in [f.name for f in fact_prices_schema.fields]
        
        fact_volume_schema = dimensional_builder.create_fact_trading_volume_schema()
        assert len(fact_volume_schema.fields) == 15
        assert "volume_key" in [f.name for f in fact_volume_schema.fields]
    
    def test_build_dim_date(self, dimensional_builder):
        """Test dim_date building."""
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 3)
        
        dim_date = dimensional_builder.build_dim_date(start_date, end_date)
        
        # Should have 3 records
        assert dim_date.count() == 3
        
        # Check columns exist
        columns = dim_date.columns
        assert "date_key" in columns
        assert "date_value" in columns
        assert "year" in columns
        assert "month" in columns
        assert "is_weekend" in columns
        
        # Check date_key format (YYYYMMDD)
        first_row = dim_date.orderBy("date_key").first()
        assert first_row["date_key"] == 20240101
        assert first_row["year"] == 2024
        assert first_row["month"] == 1
    
    def test_build_dim_time(self, dimensional_builder):
        """Test dim_time building."""
        dim_time = dimensional_builder.build_dim_time()
        
        # Should have 1440 records (24 hours * 60 minutes)
        assert dim_time.count() == 1440
        
        # Check columns exist
        columns = dim_time.columns
        assert "time_key" in columns
        assert "hour" in columns
        assert "minute" in columns
        assert "market_session" in columns
        
        # Check market session logic
        regular_hours = dim_time.filter(col("market_session") == "REGULAR")
        assert regular_hours.count() > 0
        
        # Check time_key format (HHMM)
        first_row = dim_time.orderBy("time_key").first()
        assert first_row["time_key"] == 0
        assert first_row["hour"] == 0
        assert first_row["minute"] == 0
    
    def test_build_dim_company(self, dimensional_builder, sample_stock_data):
        """Test dim_company building."""
        dim_company = dimensional_builder.build_dim_company(sample_stock_data)
        
        # Should have 3 unique companies
        assert dim_company.count() == 3
        
        # Check columns exist
        columns = dim_company.columns
        assert "company_key" in columns
        assert "symbol" in columns
        assert "company_name" in columns
        assert "effective_date" in columns
        assert "is_current" in columns
        
        # Check all records are current
        current_count = dim_company.filter(col("is_current") == True).count()
        assert current_count == 3
        
        # Check symbols are present
        symbols = [row["symbol"] for row in dim_company.collect()]
        assert "AAPL" in symbols
        assert "GOOGL" in symbols
        assert "MSFT" in symbols
    
    def test_scd_type2_logic(self, dimensional_builder, spark):
        """Test SCD Type 2 logic."""
        # Create existing dimension data
        existing_data = spark.createDataFrame([
            (1, "AAPL", "Apple Inc.", "Technology", date(2024, 1, 1), None, True),
            (2, "GOOGL", "Alphabet Inc.", "Technology", date(2024, 1, 1), None, True)
        ], ["company_key", "symbol", "company_name", "sector", "effective_date", "expiry_date", "is_current"])
        
        # Create new data with changes
        new_data = spark.createDataFrame([
            ("AAPL", "Apple Inc.", "Consumer Electronics"),  # Sector changed
            ("GOOGL", "Alphabet Inc.", "Technology"),        # No change
            ("MSFT", "Microsoft Corp.", "Technology")        # New record
        ], ["symbol", "company_name", "sector"])
        
        config = DimensionConfig(
            table_name="dim_company",
            natural_key_columns=["symbol"],
            scd_columns=["sector", "company_name"]
        )
        
        result = dimensional_builder.apply_scd_type2(new_data, existing_data, config)
        
        # Should have more records due to SCD Type 2
        assert result.count() > existing_data.count()
        
        # Check that AAPL has both old and new records
        aapl_records = result.filter(col("symbol") == "AAPL")
        assert aapl_records.count() == 2  # Old expired + new current
        
        # Check current flags
        current_records = result.filter(col("is_current") == True)
        assert current_records.count() == 3  # AAPL new, GOOGL unchanged, MSFT new


class TestDataQualityValidator:
    """Test cases for DataQualityValidator."""
    
    def test_validate_dim_company(self, data_quality_validator, spark):
        """Test dim_company validation."""
        # Create test data with some quality issues
        test_data = spark.createDataFrame([
            (1, "AAPL", "Apple Inc.", date(2024, 1, 1), None, True),
            (2, None, "Invalid Co.", date(2024, 1, 1), None, True),  # Null symbol
            (3, "INVALID123", "Bad Symbol Co.", date(2024, 1, 1), None, True),  # Invalid symbol format
            (4, "GOOGL", "Alphabet Inc.", date(2024, 1, 1), None, True),
            (5, "GOOGL", "Alphabet Inc.", date(2024, 1, 1), None, True)  # Duplicate current record
        ], ["company_key", "symbol", "company_name", "effective_date", "expiry_date", "is_current"])
        
        results = data_quality_validator.validate_dim_company(test_data)
        
        # Should have validation results
        assert len(results) > 0
        
        # Check for specific validation failures
        rule_names = [r.rule_name for r in results]
        assert "company_symbol_not_null" in rule_names
        assert "company_symbol_format" in rule_names
        assert "current_record_consistency" in rule_names
        
        # Check that some validations failed
        failed_results = [r for r in results if not r.passed]
        assert len(failed_results) > 0
    
    def test_validate_fact_stock_prices(self, data_quality_validator, spark):
        """Test fact_stock_prices validation."""
        # Create test data with quality issues
        test_data = spark.createDataFrame([
            (1, 1, 20240115, 1030, 150.00, 152.50, 149.50, 151.75, 1000000),
            (2, 2, 20240115, 1030, -10.00, 152.50, 149.50, 151.75, 500000),  # Negative price
            (3, 3, 20240115, 1030, 380.00, 375.00, 378.00, 382.25, -1000),   # High < Low, negative volume
            (4, None, 20240115, 1030, 100.00, 105.00, 98.00, 102.00, 750000)  # Null company_key
        ], ["price_key", "company_key", "date_key", "time_key", "open_price", "high_price", "low_price", "close_price", "volume"])
        
        results = data_quality_validator.validate_fact_stock_prices(test_data)
        
        # Should have validation results
        assert len(results) > 0
        
        # Check for specific validation failures
        rule_names = [r.rule_name for r in results]
        assert "company_key_not_null" in rule_names
        assert "positive_prices" in rule_names
        assert "volume_non_negative" in rule_names
        assert "price_consistency" in rule_names
        
        # Check that validations failed
        failed_results = [r for r in results if not r.passed]
        assert len(failed_results) > 0
    
    def test_generate_data_quality_report(self, data_quality_validator):
        """Test data quality report generation."""
        # Create mock validation results
        from data_quality import ValidationResult
        
        results = [
            ValidationResult("test_rule_1", True, 0, 100, 0.0, "ERROR", "All good"),
            ValidationResult("test_rule_2", False, 5, 100, 0.05, "WARNING", "Some issues"),
            ValidationResult("test_rule_3", False, 10, 100, 0.10, "ERROR", "Major issues")
        ]
        
        report = data_quality_validator.generate_data_quality_report(results)
        
        # Check report structure
        assert "summary" in report
        assert "failed_rules" in report
        assert "recommendations" in report
        
        # Check summary
        summary = report["summary"]
        assert summary["total_rules"] == 3
        assert summary["passed_rules"] == 1
        assert summary["failed_rules"] == 2
        assert summary["error_count"] == 1
        assert summary["warning_count"] == 1
        
        # Check failed rules
        failed_rules = report["failed_rules"]
        assert len(failed_rules["errors"]) == 1
        assert len(failed_rules["warnings"]) == 1


if __name__ == "__main__":
    pytest.main([__file__])
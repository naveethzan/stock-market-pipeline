"""
Dimensional data modeling components for the streaming pipeline.

This module provides classes and functions for creating and managing
dimensional model tables including fact and dimension tables with
SCD Type 2 support.
"""

from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Dict, List, Optional, Tuple, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, 
    to_date, date_format, hour, minute, second,
    year, quarter, month, dayofmonth, dayofweek,
    weekofyear, row_number, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, BooleanType, DateType, TimeType, TimestampType
)
import logging

logger = logging.getLogger(__name__)


@dataclass
class DimensionConfig:
    """Configuration for dimension table processing."""
    table_name: str
    natural_key_columns: List[str]
    scd_columns: List[str]  # Columns that trigger SCD Type 2
    effective_date_column: str = "effective_date"
    expiry_date_column: str = "expiry_date"
    is_current_column: str = "is_current"


class DimensionalModelBuilder:
    """Builder class for creating and managing dimensional model tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def create_dim_company_schema(self) -> StructType:
        """Create schema for dim_company table."""
        return StructType([
            StructField("company_key", IntegerType(), False),
            StructField("symbol", StringType(), False),
            StructField("company_name", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("market_cap_category", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("country", StringType(), True),
            StructField("effective_date", DateType(), False),
            StructField("expiry_date", DateType(), True),
            StructField("is_current", BooleanType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False)
        ])
    
    def create_dim_date_schema(self) -> StructType:
        """Create schema for dim_date table."""
        return StructType([
            StructField("date_key", IntegerType(), False),
            StructField("date_value", DateType(), False),
            StructField("year", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("month_name", StringType(), True),
            StructField("day_of_month", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("day_name", StringType(), True),
            StructField("week_of_year", IntegerType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("is_holiday", BooleanType(), True),
            StructField("fiscal_year", IntegerType(), True),
            StructField("fiscal_quarter", IntegerType(), True)
        ])
    
    def create_dim_time_schema(self) -> StructType:
        """Create schema for dim_time table."""
        return StructType([
            StructField("time_key", IntegerType(), False),
            StructField("time_value", TimeType(), False),
            StructField("hour", IntegerType(), True),
            StructField("minute", IntegerType(), True),
            StructField("second", IntegerType(), True),
            StructField("hour_minute", StringType(), True),
            StructField("am_pm", StringType(), True),
            StructField("market_session", StringType(), True),
            StructField("trading_day_minute", IntegerType(), True)
        ])
    
    def create_fact_stock_prices_schema(self) -> StructType:
        """Create schema for fact_stock_prices table."""
        return StructType([
            StructField("price_key", IntegerType(), False),
            StructField("company_key", IntegerType(), False),
            StructField("date_key", IntegerType(), False),
            StructField("time_key", IntegerType(), False),
            StructField("open_price", DecimalType(18, 4), True),
            StructField("high_price", DecimalType(18, 4), True),
            StructField("low_price", DecimalType(18, 4), True),
            StructField("close_price", DecimalType(18, 4), True),
            StructField("volume", IntegerType(), True),
            StructField("adjusted_close", DecimalType(18, 4), True),
            StructField("dividend_amount", DecimalType(18, 4), True),
            StructField("split_coefficient", DecimalType(10, 6), True),
            # Technical Indicators
            StructField("sma_20", DecimalType(18, 4), True),
            StructField("sma_50", DecimalType(18, 4), True),
            StructField("ema_12", DecimalType(18, 4), True),
            StructField("ema_26", DecimalType(18, 4), True),
            StructField("rsi_14", DecimalType(8, 4), True),
            StructField("macd", DecimalType(18, 4), True),
            StructField("macd_signal", DecimalType(18, 4), True),
            # Metadata
            StructField("data_source", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("processing_timestamp", TimestampType(), False)
        ])
    
    def create_fact_trading_volume_schema(self) -> StructType:
        """Create schema for fact_trading_volume table."""
        return StructType([
            StructField("volume_key", IntegerType(), False),
            StructField("company_key", IntegerType(), False),
            StructField("date_key", IntegerType(), False),
            StructField("time_key", IntegerType(), False),
            StructField("volume", IntegerType(), False),
            StructField("volume_weighted_price", DecimalType(18, 4), True),
            StructField("trade_count", IntegerType(), True),
            StructField("buy_volume", IntegerType(), True),
            StructField("sell_volume", IntegerType(), True),
            # Volume Indicators
            StructField("volume_sma_20", IntegerType(), True),
            StructField("volume_ratio", DecimalType(8, 4), True),
            # Metadata
            StructField("data_source", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("processing_timestamp", TimestampType(), False)
        ])
    
    def build_dim_date(self, start_date: date, end_date: date) -> DataFrame:
        """
        Build dim_date dimension table with date attributes.
        
        Args:
            start_date: Start date for the dimension
            end_date: End date for the dimension
            
        Returns:
            DataFrame with dim_date data
        """
        # Generate date range
        date_range = self.spark.sql(f"""
            SELECT explode(sequence(
                to_date('{start_date}'), 
                to_date('{end_date}'), 
                interval 1 day
            )) as date_value
        """)
        
        # Add date attributes
        dim_date = date_range.select(
            # Primary key as YYYYMMDD integer
            date_format(col("date_value"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
            col("date_value"),
            year(col("date_value")).alias("year"),
            quarter(col("date_value")).alias("quarter"),
            month(col("date_value")).alias("month"),
            date_format(col("date_value"), "MMMM").alias("month_name"),
            dayofmonth(col("date_value")).alias("day_of_month"),
            dayofweek(col("date_value")).alias("day_of_week"),
            date_format(col("date_value"), "EEEE").alias("day_name"),
            weekofyear(col("date_value")).alias("week_of_year"),
            # Weekend check (Sunday=1, Saturday=7)
            when(dayofweek(col("date_value")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
            # Placeholder for holiday logic
            lit(False).alias("is_holiday"),
            # Fiscal year (assuming April-March fiscal year)
            when(month(col("date_value")) >= 4, year(col("date_value")))
            .otherwise(year(col("date_value")) - 1).alias("fiscal_year"),
            # Fiscal quarter
            when(month(col("date_value")).between(4, 6), 1)
            .when(month(col("date_value")).between(7, 9), 2)
            .when(month(col("date_value")).between(10, 12), 3)
            .otherwise(4).alias("fiscal_quarter")
        )
        
        return dim_date
    
    def build_dim_time(self) -> DataFrame:
        """
        Build dim_time dimension table with time attributes.
        
        Returns:
            DataFrame with dim_time data
        """
        # Generate time range (every minute of the day)
        time_range = self.spark.sql("""
            SELECT explode(sequence(0, 1439)) as minute_of_day
        """)
        
        # Convert minutes to time attributes
        dim_time = time_range.select(
            # Primary key as HHMM integer
            ((col("minute_of_day") / 60).cast(IntegerType()) * 100 + 
             (col("minute_of_day") % 60)).alias("time_key"),
            # Time value
            (col("minute_of_day") * 60).cast("timestamp").cast("time").alias("time_value"),
            (col("minute_of_day") / 60).cast(IntegerType()).alias("hour"),
            (col("minute_of_day") % 60).alias("minute"),
            lit(0).alias("second"),
            # Hour:Minute format
            date_format((col("minute_of_day") * 60).cast("timestamp"), "HH:mm").alias("hour_minute"),
            # AM/PM
            when((col("minute_of_day") / 60) < 12, "AM").otherwise("PM").alias("am_pm"),
            # Market session (assuming 9:30 AM - 4:00 PM EST regular hours)
            when(col("minute_of_day").between(570, 959), "REGULAR")  # 9:30 AM - 4:00 PM
            .when(col("minute_of_day").between(240, 569), "PRE_MARKET")  # 4:00 AM - 9:29 AM
            .when(col("minute_of_day").between(960, 1200), "AFTER_HOURS")  # 4:01 PM - 8:00 PM
            .otherwise("CLOSED").alias("market_session"),
            # Minutes since market open (9:30 AM = 570 minutes)
            when(col("minute_of_day") >= 570, col("minute_of_day") - 570)
            .otherwise(lit(None)).alias("trading_day_minute")
        )
        
        return dim_time    
   
    def apply_scd_type2(self, 
                       new_data: DataFrame, 
                       existing_data: DataFrame,
                       config: DimensionConfig) -> DataFrame:
        """
        Apply SCD Type 2 logic to dimension data.
        
        Args:
            new_data: New dimension data
            existing_data: Existing dimension data
            config: Dimension configuration
            
        Returns:
            DataFrame with SCD Type 2 applied
        """
        current_date = datetime.now().date()
        
        # Get current records
        current_records = existing_data.filter(col(config.is_current_column) == True)
        
        # Join new data with current records on natural key
        natural_key_join = new_data.alias("new").join(
            current_records.alias("current"),
            [col(f"new.{key}") == col(f"current.{key}") for key in config.natural_key_columns],
            "left_outer"
        )
        
        # Identify records that need SCD Type 2 treatment
        scd_condition = None
        for scd_col in config.scd_columns:
            col_condition = (col(f"new.{scd_col}") != col(f"current.{scd_col}")) | \
                          (col(f"new.{scd_col}").isNull() != col(f"current.{scd_col}").isNull())
            if scd_condition is None:
                scd_condition = col_condition
            else:
                scd_condition = scd_condition | col_condition
        
        # Records that changed (need to expire old and create new)
        changed_records = natural_key_join.filter(
            col("current.company_key").isNotNull() & scd_condition
        )
        
        # Records that are new (no existing record)
        new_records = natural_key_join.filter(col("current.company_key").isNull())
        
        # Records that didn't change (keep as is)
        unchanged_records = natural_key_join.filter(
            col("current.company_key").isNotNull() & ~scd_condition
        )
        
        # Expire old records for changed data
        expired_records = changed_records.select(
            *[col(f"current.{field.name}") for field in existing_data.schema.fields 
              if field.name not in [config.expiry_date_column, config.is_current_column, "updated_at"]]
        ).withColumn(config.expiry_date_column, lit(current_date)) \
         .withColumn(config.is_current_column, lit(False)) \
         .withColumn("updated_at", current_timestamp())
        
        # Create new records for changed data
        window_spec = Window.orderBy(lit(1))
        max_key = existing_data.agg(spark_max("company_key")).collect()[0][0] or 0
        
        new_changed_records = changed_records.select(
            *[col(f"new.{field.name}") for field in new_data.schema.fields]
        ).withColumn("company_key", row_number().over(window_spec) + max_key) \
         .withColumn(config.effective_date_column, lit(current_date)) \
         .withColumn(config.expiry_date_column, lit(None)) \
         .withColumn(config.is_current_column, lit(True)) \
         .withColumn("created_at", current_timestamp()) \
         .withColumn("updated_at", current_timestamp())
        
        # Create completely new records
        new_insert_records = new_records.select(
            *[col(f"new.{field.name}") for field in new_data.schema.fields]
        ).withColumn("company_key", row_number().over(window_spec) + max_key + changed_records.count()) \
         .withColumn(config.effective_date_column, lit(current_date)) \
         .withColumn(config.expiry_date_column, lit(None)) \
         .withColumn(config.is_current_column, lit(True)) \
         .withColumn("created_at", current_timestamp()) \
         .withColumn("updated_at", current_timestamp())
        
        # Combine all records
        result = existing_data.filter(col(config.is_current_column) == False) \
                             .union(expired_records) \
                             .union(new_changed_records) \
                             .union(new_insert_records)
        
        return result
    
    def build_dim_company(self, stock_data: DataFrame) -> DataFrame:
        """
        Build dim_company dimension from stock data.
        
        Args:
            stock_data: Raw stock data DataFrame
            
        Returns:
            DataFrame with dim_company data
        """
        # Extract unique company information
        company_data = stock_data.select(
            col("symbol"),
            coalesce(col("company_name"), col("symbol")).alias("company_name"),
            col("sector"),
            col("industry"),
            col("market_cap_category"),
            col("exchange"),
            col("currency"),
            col("country")
        ).distinct()
        
        # Add dimension attributes
        window_spec = Window.orderBy("symbol")
        current_date = datetime.now().date()
        
        dim_company = company_data.withColumn("company_key", row_number().over(window_spec)) \
                                 .withColumn("effective_date", lit(current_date)) \
                                 .withColumn("expiry_date", lit(None)) \
                                 .withColumn("is_current", lit(True)) \
                                 .withColumn("created_at", current_timestamp()) \
                                 .withColumn("updated_at", current_timestamp())
        
        return dim_company
    
    def build_fact_stock_prices(self, 
                               stock_data: DataFrame,
                               dim_company: DataFrame,
                               dim_date: DataFrame,
                               dim_time: DataFrame) -> DataFrame:
        """
        Build fact_stock_prices table from stock data and dimensions.
        
        Args:
            stock_data: Raw stock data
            dim_company: Company dimension
            dim_date: Date dimension  
            dim_time: Time dimension
            
        Returns:
            DataFrame with fact_stock_prices data
        """
        # Join with dimensions to get keys
        fact_data = stock_data.alias("stock") \
            .join(dim_company.alias("company"), 
                  col("stock.symbol") == col("company.symbol"), "inner") \
            .join(dim_date.alias("date"), 
                  to_date(col("stock.timestamp")) == col("date.date_value"), "inner") \
            .join(dim_time.alias("time"),
                  # Match time by hour and minute
                  (hour(col("stock.timestamp")) * 100 + minute(col("stock.timestamp"))) == col("time.time_key"), 
                  "inner")
        
        # Generate fact key
        window_spec = Window.orderBy(col("stock.timestamp"))
        
        fact_stock_prices = fact_data.select(
            row_number().over(window_spec).alias("price_key"),
            col("company.company_key"),
            col("date.date_key"),
            col("time.time_key"),
            col("stock.open").alias("open_price"),
            col("stock.high").alias("high_price"),
            col("stock.low").alias("low_price"),
            col("stock.close").alias("close_price"),
            col("stock.volume"),
            col("stock.adjusted_close"),
            coalesce(col("stock.dividend_amount"), lit(0)).alias("dividend_amount"),
            coalesce(col("stock.split_coefficient"), lit(1)).alias("split_coefficient"),
            # Technical indicators (to be calculated)
            col("stock.sma_20"),
            col("stock.sma_50"),
            col("stock.ema_12"),
            col("stock.ema_26"),
            col("stock.rsi_14"),
            col("stock.macd"),
            col("stock.macd_signal"),
            # Metadata
            lit("alpha_vantage").alias("data_source"),
            col("stock.timestamp").alias("ingestion_timestamp"),
            current_timestamp().alias("processing_timestamp")
        )
        
        return fact_stock_prices
    
    def build_fact_trading_volume(self,
                                 stock_data: DataFrame,
                                 dim_company: DataFrame,
                                 dim_date: DataFrame,
                                 dim_time: DataFrame) -> DataFrame:
        """
        Build fact_trading_volume table from stock data and dimensions.
        
        Args:
            stock_data: Raw stock data
            dim_company: Company dimension
            dim_date: Date dimension
            dim_time: Time dimension
            
        Returns:
            DataFrame with fact_trading_volume data
        """
        # Join with dimensions to get keys
        fact_data = stock_data.alias("stock") \
            .join(dim_company.alias("company"), 
                  col("stock.symbol") == col("company.symbol"), "inner") \
            .join(dim_date.alias("date"), 
                  to_date(col("stock.timestamp")) == col("date.date_value"), "inner") \
            .join(dim_time.alias("time"),
                  (hour(col("stock.timestamp")) * 100 + minute(col("stock.timestamp"))) == col("time.time_key"), 
                  "inner")
        
        # Generate fact key
        window_spec = Window.orderBy(col("stock.timestamp"))
        
        fact_trading_volume = fact_data.select(
            row_number().over(window_spec).alias("volume_key"),
            col("company.company_key"),
            col("date.date_key"),
            col("time.time_key"),
            col("stock.volume"),
            # Volume weighted price calculation
            (col("stock.close") * col("stock.volume") / col("stock.volume")).alias("volume_weighted_price"),
            lit(None).cast(IntegerType()).alias("trade_count"),  # Not available from Alpha Vantage
            lit(None).cast(IntegerType()).alias("buy_volume"),   # Not available from Alpha Vantage
            lit(None).cast(IntegerType()).alias("sell_volume"),  # Not available from Alpha Vantage
            # Volume indicators (to be calculated)
            col("stock.volume_sma_20"),
            col("stock.volume_ratio"),
            # Metadata
            lit("alpha_vantage").alias("data_source"),
            col("stock.timestamp").alias("ingestion_timestamp"),
            current_timestamp().alias("processing_timestamp")
        )
        
        return fact_trading_volume
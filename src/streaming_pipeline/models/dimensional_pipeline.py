"""
Dimensional pipeline integration module.

This module provides a high-level interface for building and validating
the complete dimensional model from streaming stock data.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging

from .dimensional import DimensionalModelBuilder, DimensionConfig
from .data_quality import DataQualityValidator, ValidationResult

logger = logging.getLogger(__name__)


class DimensionalPipeline:
    """
    High-level pipeline for building and validating dimensional model.
    
    This class orchestrates the creation of dimension and fact tables
    from streaming stock data, applies data quality validation, and
    manages the overall dimensional modeling process.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.builder = DimensionalModelBuilder(spark)
        self.validator = DataQualityValidator(spark)
        self.logger = logging.getLogger(__name__)
    
    def process_streaming_batch(self, 
                               stock_data: DataFrame,
                               existing_dimensions: Optional[Dict[str, DataFrame]] = None) -> Dict[str, DataFrame]:
        """
        Process a streaming batch of stock data into dimensional model.
        
        Args:
            stock_data: Raw stock data from streaming source
            existing_dimensions: Existing dimension tables for SCD processing
            
        Returns:
            Dictionary containing all dimensional tables
        """
        self.logger.info("Starting dimensional model processing for streaming batch")
        
        try:
            # Build dimension tables
            dimensions = self._build_dimensions(stock_data, existing_dimensions)
            
            # Build fact tables
            facts = self._build_facts(stock_data, dimensions)
            
            # Combine all tables
            result = {**dimensions, **facts}
            
            # Validate data quality
            validation_results = self._validate_all_tables(result)
            
            # Check for critical validation failures
            critical_failures = [r for r in validation_results if r.severity == 'ERROR' and not r.passed]
            if critical_failures:
                self.logger.error(f"Critical validation failures detected: {len(critical_failures)} errors")
                for failure in critical_failures:
                    self.logger.error(f"  - {failure.rule_name}: {failure.message}")
                raise ValueError("Critical data quality validation failures detected")
            
            # Log warnings
            warnings = [r for r in validation_results if r.severity == 'WARNING' and not r.passed]
            if warnings:
                self.logger.warning(f"Data quality warnings detected: {len(warnings)} warnings")
                for warning in warnings:
                    self.logger.warning(f"  - {warning.rule_name}: {warning.message}")
            
            self.logger.info("Dimensional model processing completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing streaming batch: {str(e)}")
            raise
    
    def _build_dimensions(self, 
                         stock_data: DataFrame,
                         existing_dimensions: Optional[Dict[str, DataFrame]] = None) -> Dict[str, DataFrame]:
        """Build all dimension tables."""
        
        dimensions = {}
        
        # Build dim_date (for the date range in the data)
        min_date = stock_data.select(col("timestamp").cast("date")).agg({"timestamp": "min"}).collect()[0][0]
        max_date = stock_data.select(col("timestamp").cast("date")).agg({"timestamp": "max"}).collect()[0][0]
        
        if min_date and max_date:
            dimensions["dim_date"] = self.builder.build_dim_date(min_date, max_date)
        else:
            # Fallback to current date
            current_date = datetime.now().date()
            dimensions["dim_date"] = self.builder.build_dim_date(current_date, current_date)
        
        # Build dim_time (full day)
        dimensions["dim_time"] = self.builder.build_dim_time()
        
        # Build dim_company with SCD Type 2 logic
        new_company_data = stock_data.select(
            col("symbol"),
            col("company_name"),
            col("sector"),
            col("industry"),
            col("exchange"),
            col("currency"),
            col("country")
        ).distinct()
        
        if existing_dimensions and "dim_company" in existing_dimensions:
            # Apply SCD Type 2 logic
            config = DimensionConfig(
                table_name="dim_company",
                natural_key_columns=["symbol"],
                scd_columns=["company_name", "sector", "industry", "exchange"]
            )
            dimensions["dim_company"] = self.builder.apply_scd_type2(
                new_company_data, 
                existing_dimensions["dim_company"], 
                config
            )
        else:
            # Build new dimension
            dimensions["dim_company"] = self.builder.build_dim_company(stock_data)
        
        return dimensions
    
    def _build_facts(self, stock_data: DataFrame, dimensions: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Build all fact tables."""
        
        facts = {}
        
        # Build fact_stock_prices
        facts["fact_stock_prices"] = self.builder.build_fact_stock_prices(
            stock_data,
            dimensions["dim_company"],
            dimensions["dim_date"],
            dimensions["dim_time"]
        )
        
        # Build fact_trading_volume
        facts["fact_trading_volume"] = self.builder.build_fact_trading_volume(
            stock_data,
            dimensions["dim_company"],
            dimensions["dim_date"],
            dimensions["dim_time"]
        )
        
        return facts
    
    def _validate_all_tables(self, tables: Dict[str, DataFrame]) -> List[ValidationResult]:
        """Validate all dimensional tables."""
        
        all_results = []
        
        # Validate dimensions
        if "dim_company" in tables:
            results = self.validator.validate_dim_company(tables["dim_company"])
            all_results.extend(results)
        
        if "dim_date" in tables:
            results = self.validator.validate_dim_date(tables["dim_date"])
            all_results.extend(results)
        
        if "dim_time" in tables:
            results = self.validator.validate_dim_time(tables["dim_time"])
            all_results.extend(results)
        
        # Validate facts
        if "fact_stock_prices" in tables:
            results = self.validator.validate_fact_stock_prices(tables["fact_stock_prices"])
            all_results.extend(results)
        
        if "fact_trading_volume" in tables:
            results = self.validator.validate_fact_trading_volume(tables["fact_trading_volume"])
            all_results.extend(results)
        
        return all_results
    
    def save_dimensional_model(self, 
                              tables: Dict[str, DataFrame],
                              output_path: str,
                              format: str = "parquet",
                              mode: str = "overwrite") -> None:
        """
        Save dimensional model tables to storage.
        
        Args:
            tables: Dictionary of dimensional tables
            output_path: Base output path
            format: Output format (parquet, delta, etc.)
            mode: Write mode (overwrite, append, etc.)
        """
        
        self.logger.info(f"Saving dimensional model to {output_path}")
        
        for table_name, df in tables.items():
            table_path = f"{output_path}/{table_name}"
            
            try:
                df.write \
                  .mode(mode) \
                  .format(format) \
                  .option("path", table_path) \
                  .save()
                
                self.logger.info(f"Saved {table_name} with {df.count()} records to {table_path}")
                
            except Exception as e:
                self.logger.error(f"Error saving {table_name}: {str(e)}")
                raise
    
    def load_dimensional_model(self, 
                              input_path: str,
                              format: str = "parquet") -> Dict[str, DataFrame]:
        """
        Load dimensional model tables from storage.
        
        Args:
            input_path: Base input path
            format: Input format (parquet, delta, etc.)
            
        Returns:
            Dictionary of dimensional tables
        """
        
        self.logger.info(f"Loading dimensional model from {input_path}")
        
        tables = {}
        table_names = ["dim_company", "dim_date", "dim_time", "fact_stock_prices", "fact_trading_volume"]
        
        for table_name in table_names:
            table_path = f"{input_path}/{table_name}"
            
            try:
                df = self.spark.read \
                    .format(format) \
                    .load(table_path)
                
                tables[table_name] = df
                self.logger.info(f"Loaded {table_name} with {df.count()} records from {table_path}")
                
            except Exception as e:
                self.logger.warning(f"Could not load {table_name}: {str(e)}")
                # Continue loading other tables
        
        return tables
    
    def generate_quality_report(self, tables: Dict[str, DataFrame]) -> Dict:
        """
        Generate comprehensive data quality report for all tables.
        
        Args:
            tables: Dictionary of dimensional tables
            
        Returns:
            Comprehensive quality report
        """
        
        validation_results = self._validate_all_tables(tables)
        report = self.validator.generate_data_quality_report(validation_results)
        
        # Add table-specific metrics
        table_metrics = {}
        for table_name, df in tables.items():
            table_metrics[table_name] = {
                "record_count": df.count(),
                "column_count": len(df.columns),
                "null_counts": {col_name: df.filter(col(col_name).isNull()).count() 
                               for col_name in df.columns}
            }
        
        report["table_metrics"] = table_metrics
        
        return report
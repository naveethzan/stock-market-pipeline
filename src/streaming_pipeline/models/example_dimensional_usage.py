"""
Example usage of dimensional modeling components.

This script demonstrates how to use the dimensional modeling pipeline
to process streaming stock data into a dimensional model.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from datetime import datetime
import logging

from dimensional_pipeline import DimensionalPipeline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_stock_data(spark: SparkSession):
    """Create sample stock data for demonstration."""
    
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
        StructField("market_cap_category", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True)
    ])
    
    # Sample data representing streaming stock data
    data = [
        # Apple Inc.
        ("AAPL", datetime(2024, 1, 15, 9, 30), 150.00, 152.50, 149.50, 151.75, 1000000, 151.75,
         "Apple Inc.", "Technology", "Consumer Electronics", "Large Cap", "NASDAQ", "USD", "USA"),
        ("AAPL", datetime(2024, 1, 15, 10, 30), 151.75, 153.00, 150.25, 152.25, 800000, 152.25,
         "Apple Inc.", "Technology", "Consumer Electronics", "Large Cap", "NASDAQ", "USD", "USA"),
        
        # Google/Alphabet
        ("GOOGL", datetime(2024, 1, 15, 9, 30), 2800.00, 2825.00, 2790.00, 2810.50, 500000, 2810.50,
         "Alphabet Inc.", "Technology", "Internet Services", "Large Cap", "NASDAQ", "USD", "USA"),
        ("GOOGL", datetime(2024, 1, 15, 10, 30), 2810.50, 2830.00, 2805.00, 2820.75, 450000, 2820.75,
         "Alphabet Inc.", "Technology", "Internet Services", "Large Cap", "NASDAQ", "USD", "USA"),
        
        # Microsoft
        ("MSFT", datetime(2024, 1, 15, 9, 30), 380.00, 385.00, 378.00, 382.25, 750000, 382.25,
         "Microsoft Corporation", "Technology", "Software", "Large Cap", "NASDAQ", "USD", "USA"),
        ("MSFT", datetime(2024, 1, 15, 10, 30), 382.25, 386.50, 380.75, 384.00, 650000, 384.00,
         "Microsoft Corporation", "Technology", "Software", "Large Cap", "NASDAQ", "USD", "USA"),
        
        # Tesla
        ("TSLA", datetime(2024, 1, 15, 9, 30), 240.00, 245.00, 238.50, 242.75, 2000000, 242.75,
         "Tesla Inc.", "Consumer Discretionary", "Automotive", "Large Cap", "NASDAQ", "USD", "USA"),
        ("TSLA", datetime(2024, 1, 15, 10, 30), 242.75, 246.00, 240.25, 244.50, 1800000, 244.50,
         "Tesla Inc.", "Consumer Discretionary", "Automotive", "Large Cap", "NASDAQ", "USD", "USA")
    ]
    
    return spark.createDataFrame(data, schema)


def main():
    """Main example function."""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DimensionalModelingExample") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        logger.info("Starting dimensional modeling example")
        
        # Create sample streaming data
        stock_data = create_sample_stock_data(spark)
        logger.info(f"Created sample stock data with {stock_data.count()} records")
        
        # Show sample data
        print("\n=== Sample Stock Data ===")
        stock_data.show(5, truncate=False)
        
        # Initialize dimensional pipeline
        pipeline = DimensionalPipeline(spark)
        
        # Process the streaming batch into dimensional model
        logger.info("Processing streaming batch into dimensional model")
        dimensional_tables = pipeline.process_streaming_batch(stock_data)
        
        # Display results
        print("\n=== Dimensional Model Results ===")
        
        for table_name, df in dimensional_tables.items():
            print(f"\n--- {table_name.upper()} ---")
            print(f"Record count: {df.count()}")
            print("Schema:")
            df.printSchema()
            print("Sample data:")
            df.show(5, truncate=False)
        
        # Generate data quality report
        logger.info("Generating data quality report")
        quality_report = pipeline.generate_quality_report(dimensional_tables)
        
        print("\n=== Data Quality Report ===")
        print(f"Total validation rules: {quality_report['summary']['total_rules']}")
        print(f"Passed rules: {quality_report['summary']['passed_rules']}")
        print(f"Failed rules: {quality_report['summary']['failed_rules']}")
        print(f"Pass rate: {quality_report['summary']['pass_rate']:.2%}")
        
        if quality_report['failed_rules']['errors']:
            print("\nERRORS:")
            for error in quality_report['failed_rules']['errors']:
                print(f"  - {error['rule_name']}: {error['message']}")
        
        if quality_report['failed_rules']['warnings']:
            print("\nWARNINGS:")
            for warning in quality_report['failed_rules']['warnings']:
                print(f"  - {warning['rule_name']}: {warning['message']}")
        
        if quality_report['recommendations']:
            print("\nRECOMMENDATIONS:")
            for rec in quality_report['recommendations']:
                print(f"  - {rec}")
        
        # Demonstrate SCD Type 2 processing
        print("\n=== SCD Type 2 Example ===")
        
        # Simulate updated company data (sector change for Apple)
        updated_stock_data = spark.createDataFrame([
            ("AAPL", datetime(2024, 1, 16, 9, 30), 152.00, 154.00, 151.00, 153.50, 900000, 153.50,
             "Apple Inc.", "Consumer Electronics", "Consumer Electronics", "Large Cap", "NASDAQ", "USD", "USA"),  # Sector changed
        ], stock_data.schema)
        
        logger.info("Processing updated data with SCD Type 2")
        updated_tables = pipeline.process_streaming_batch(
            updated_stock_data, 
            existing_dimensions={
                "dim_company": dimensional_tables["dim_company"],
                "dim_date": dimensional_tables["dim_date"],
                "dim_time": dimensional_tables["dim_time"]
            }
        )
        
        print("Updated dim_company with SCD Type 2:")
        apple_records = updated_tables["dim_company"].filter(
            updated_tables["dim_company"]["symbol"] == "AAPL"
        ).orderBy("effective_date")
        apple_records.show(truncate=False)
        
        # Save dimensional model (optional - uncomment to save)
        # output_path = "/tmp/dimensional_model"
        # pipeline.save_dimensional_model(dimensional_tables, output_path)
        # logger.info(f"Dimensional model saved to {output_path}")
        
        logger.info("Dimensional modeling example completed successfully")
        
    except Exception as e:
        logger.error(f"Error in dimensional modeling example: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
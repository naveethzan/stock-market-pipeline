import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Import our modules
from batch.models.schemas import raw_batch_schema, transformed_batch_schema
from batch.models.transformations import BatchTransformerOptimized
from batch.utils.data_quality import DataQualityChecker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DailyETLOptimized:
    """Optimized ETL job for daily batch processing"""
    
    def __init__(self, spark):
        self.spark = spark
        self.transformer = BatchTransformerOptimized(spark)
        self.quality_checker = DataQualityChecker(spark)
        
        # Configuration from Spark conf
        self.s3_bucket = spark.conf.get("spark.s3.bucket", "stock-market-pipeline")
        self.raw_zone = spark.conf.get("spark.s3.raw_zone", "raw-data/batch")
        self.transformed_zone = spark.conf.get("spark.s3.transformed_zone", "transformed/batch")
        
        # Performance settings
        self.enable_caching = spark.conf.get("spark.etl.enable_caching", "true").lower() == "true"
        self.output_partitions = int(spark.conf.get("spark.etl.output_partitions", "4"))
    
    def run(self, process_date: str):
        """Run the optimized ETL pipeline"""
        try:
            logger.info(f"Starting optimized ETL for date: {process_date}")
            
            # Extract with early filtering and projection
            raw_df = self._extract_optimized(process_date)
            if raw_df.rdd.isEmpty():
                logger.warning(f"No data for date: {process_date}")
                return
            
            # Transform with caching strategy
            transformed_df = self._transform_optimized(raw_df, process_date)
            
            # Quality checks (streamlined)
            self._check_quality_optimized(transformed_df, process_date)
            
            # Load with optimized partitioning
            self._load_optimized(transformed_df, process_date)
            
            logger.info("ETL completed successfully")
            
        except Exception as e:
            logger.error(f"ETL failed: {str(e)}")
            raise
        finally:
            # Cleanup cached DataFrames
            self.spark.catalog.clearCache()
    
    def _extract_optimized(self, process_date: str):
        """Optimized data extraction with early filtering"""
        logger.info(f"Extracting data for: {process_date}")
        
        # Parse date once
        dt = datetime.strptime(process_date, "%Y-%m-%d")
        s3_path = (f"s3a://{self.s3_bucket}/{self.raw_zone}/"
                  f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/*.parquet")
        
        try:
            # Read with schema enforcement
            raw_df = self.spark.read.schema(raw_batch_schema).parquet(s3_path)
            
            # Early projection - select only needed columns
            required_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "adj_close"]
            available_cols = [col for col in required_cols if col in raw_df.columns]
            
            if not available_cols:
                return self.spark.createDataFrame([], raw_batch_schema)
            
            # Select and standardize in one pass
            df = raw_df.select(*available_cols)
            
            # Standardize column types efficiently
            df = (df
                .withColumn("symbol", F.col("symbol").cast("string"))
                .withColumn("date", F.to_date(F.col("date")))
                .withColumn("open", F.col("open").cast("double"))
                .withColumn("high", F.col("high").cast("double"))
                .withColumn("low", F.col("low").cast("double"))
                .withColumn("close", F.col("close").cast("double"))
                .withColumn("volume", F.col("volume").cast("long"))
                .withColumn("adj_close", F.col("adj_close").cast("double"))
                .withColumn("ingestion_timestamp", F.current_timestamp())
            )
            
            # Filter out invalid records early
            df = df.filter(
                F.col("symbol").isNotNull() & 
                F.col("date").isNotNull() &
                F.col("close").isNotNull()
            )
            
            count = df.count()
            logger.info(f"Extracted {count} valid records")
            
            # Cache if enabled and data size is reasonable
            if self.enable_caching and count > 1000:
                df.cache()
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from {s3_path}: {e}")
            return self.spark.createDataFrame([], raw_batch_schema)
    
    def _transform_optimized(self, df, process_date: str):
        """Optimized transformation with efficient operations"""
        logger.info("Applying transformations")
        
        # Apply transformations with optimized operations
        transformed_df = self.transformer.transform_raw_data_optimized(df)
        
        # Add metadata efficiently
        transformed_df = transformed_df.withColumn("process_date", F.lit(process_date).cast("date"))
        
        # Cache transformed data if it will be used multiple times
        if self.enable_caching:
            transformed_df.cache()
        
        return transformed_df
    
    def _check_quality_optimized(self, df, batch_id: str):
        """Streamlined quality checks"""
        logger.info("Running quality checks")
        
        # Basic quality metrics in single pass
        quality_metrics = df.agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("stock_id").isNull(), 1).otherwise(0)).alias("null_stock_ids"),
            F.sum(F.when(F.col("close_price") <= 0, 1).otherwise(0)).alias("invalid_prices"),
            F.countDistinct("stock_id").alias("unique_stocks")
        ).collect()[0]
        
        # Log quality summary
        logger.info(f"Quality check - Total: {quality_metrics['total_records']}, "
                   f"Null IDs: {quality_metrics['null_stock_ids']}, "
                   f"Invalid prices: {quality_metrics['invalid_prices']}, "
                   f"Unique stocks: {quality_metrics['unique_stocks']}")
        
        # Only run detailed checks if issues found
        if quality_metrics['null_stock_ids'] > 0 or quality_metrics['invalid_prices'] > 0:
            logger.warning("Data quality issues detected - running detailed analysis")
            # Run detailed quality checks only when needed
            self.quality_checker.run_checks(df, batch_id)
    
    def _load_optimized(self, df, process_date: str):
        """Optimized data loading with proper partitioning"""
        logger.info("Loading data")
        
        # Filter invalid records in single operation
        valid_df = df.filter(
            F.col("stock_id").isNotNull() &
            F.col("trading_date").isNotNull() &
            (F.col("open_price").isNotNull() | F.col("close_price").isNotNull())
        )
        
        record_count = valid_df.count()
        if record_count == 0:
            logger.warning("No valid records to write")
            return
        
        # Optimize partitioning for output
        output_df = valid_df.repartition(self.output_partitions, "stock_id")
        
        # Write to S3 with optimized settings
        s3_output_path = f"s3a://{self.s3_bucket}/{self.transformed_zone}/date={process_date}"
        
        (output_df
         .write
         .mode("overwrite")
         .option("compression", "snappy")  # Better compression
         .parquet(s3_output_path))
        
        logger.info(f"Successfully wrote {record_count} records to {s3_output_path}")


def create_spark_session_optimized():
    """Create optimized Spark session with performance tuning"""
    return (SparkSession.builder
            .appName("StockMarketBatchETL-Optimized")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                   "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB blocks
            .getOrCreate())


def main():
    """Optimized main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run optimized daily Spark ETL")
    parser.add_argument('--date', required=True, help='Processing date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session_optimized()
        etl = DailyETLOptimized(spark)
        etl.run(args.date)
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
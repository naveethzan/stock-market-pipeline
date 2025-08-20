import sys
import logging
from datetime import datetime, timedelta
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import our modules
from batch.models.schemas import raw_batch_schema, transformed_batch_schema
from batch.models.transformations import BatchTransformer
from batch.utils.data_quality import DataQualityChecker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DailyETL:
    """Main ETL job for daily batch processing"""
    
    def __init__(self, spark):
        """Initialize the ETL job"""
        self.spark = spark
        self.transformer = BatchTransformer(spark)
        self.quality_checker = DataQualityChecker(spark)
        
        # Get configuration from environment variables or use defaults
        self.s3_bucket = spark.conf.get("spark.s3.bucket", "stock-market-pipeline")
        self.raw_zone = spark.conf.get("spark.s3.raw_zone", "raw-data/batch")
        self.transformed_zone = spark.conf.get("spark.s3.transformed_zone", "transformed/batch")
    
    def run(self, process_date: str):
        """Run the ETL pipeline for a specific date."""
        try:
            logger.info(f"Starting Daily ETL Job for date: {process_date}")
            
            # Extract: Read raw data from S3
            raw_df = self._extract(process_date)
            if raw_df is None or raw_df.rdd.isEmpty():
                logger.warning(f"No data found for date: {process_date}")
                return
            
            # Transform: Apply business logic
            transformed_df = self._transform(raw_df, process_date)
            
            # Data Quality: Run quality checks
            quality_df = self._check_quality(transformed_df, process_date)
            
            # Load: Write to Snowflake and S3
            self._load(transformed_df, quality_df, process_date)
            
            logger.info("Daily ETL Job completed successfully")
            
        except Exception as e:
            logger.error(f"ETL Job failed: {str(e)}", exc_info=True)
            raise
    
    def _extract(self, process_date: str):
        logger.info(f"Extracting data for date: {process_date}")

        # Path pattern: <RAW_BASE_URI>/batch/year=YYYY/month=MM/day=DD/*.parquet
        try:
            dt = datetime.strptime(process_date, "%Y-%m-%d")
            year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        except Exception:
            logger.error(f"Invalid process_date format: {process_date}. Expected YYYY-MM-DD")
            return self.spark.createDataFrame([], raw_batch_schema)

        s3_path = (
            f"s3a://{self.s3_bucket}/{self.raw_zone}/"
            f"year={year}/month={month}/day={day}/*.parquet"
        )

        raw = None
        try:
            raw = self.spark.read.parquet(s3_path)
            logger.info(f"Raw read succeeded from {s3_path}")
        
        except Exception as _e:
                logger.warning(f"Failed to print raw schema: {_e}")   

        if raw is None:
            logger.warning(
                f"No raw parquet found at primary {s3_path} or fallback path; returning empty DataFrame"
            )
            return self.spark.createDataFrame([], raw_batch_schema)

        # Normalize tuple-like column names e.g., "('symbol', '')" -> "symbol"
        try:
            original_cols = list(raw.columns)
            pattern = r"^\('([^']+)',\s*''\)$"
            rename_map = {c: re.match(pattern, c).group(1) for c in original_cols if re.match(pattern, c)}
            if rename_map:
                logger.warning(f"Normalizing raw column names: {rename_map}")
                for old, new in rename_map.items():
                    if old != new:
                        raw = raw.withColumnRenamed(old, new)
        except Exception as _e:
            logger.warning(f"Failed to normalize raw column names: {_e}")

        # Ensure standardized columns and types
        cols = set(raw.columns)
        df = raw

        # symbol
        if "symbol" not in cols:
            df = df.withColumn("symbol", F.lit(None).cast("string"))
        else:
            df = df.withColumn("symbol", F.col("symbol").cast("string"))

        # date -> yyyy-MM-dd string
        if "date" in cols:
            df = df.withColumn("date", F.date_format(F.col("date").cast("timestamp"), "yyyy-MM-dd"))
        elif "datetime" in cols:
            df = df.withColumn("date", F.date_format(F.col("datetime").cast("timestamp"), "yyyy-MM-dd"))
        else:
            df = df.withColumn("date", F.lit(None).cast("string"))

        # numeric columns
        for c in ["open", "high", "low", "close", "adj_close"]:
            if c in cols:
                df = df.withColumn(c, F.col(c).cast("double"))
            else:
                df = df.withColumn(c, F.lit(None).cast("double"))
        if "volume" in cols:
            df = df.withColumn("volume", F.col("volume").cast("long"))
        else:
            df = df.withColumn("volume", F.lit(None).cast("long"))

        # ingestion_timestamp
        if "ingestion_timestamp" in cols:
            df = df.withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp"))
        else:
            df = df.withColumn("ingestion_timestamp", F.current_timestamp())

        selected = df.select(
            "symbol", "date", "open", "high", "low", "close", "volume", "adj_close", "ingestion_timestamp"
        )
        # Diagnostics on selected
        sel_count = selected.count()
        try:
            logger.info(f"Selected columns: {selected.columns}")
            selected.printSchema()
            nulls = selected.select(
                F.sum(F.col("symbol").isNull().cast("int")).alias("null_symbol"),
                F.sum(F.col("date").isNull().cast("int")).alias("null_date"),
                F.sum(F.col("open").isNull().cast("int")).alias("null_open"),
                F.sum(F.col("high").isNull().cast("int")).alias("null_high"),
                F.sum(F.col("low").isNull().cast("int")).alias("null_low"),
                F.sum(F.col("close").isNull().cast("int")).alias("null_close"),
                F.sum(F.col("volume").isNull().cast("int")).alias("null_volume"),
                F.sum(F.col("adj_close").isNull().cast("int")).alias("null_adj_close"),
            ).collect()[0]
            logger.info(
                f"Selected null counts: symbol={nulls['null_symbol']}, date={nulls['null_date']}, "
                f"open={nulls['null_open']}, high={nulls['null_high']}, low={nulls['null_low']}, "
                f"close={nulls['null_close']}, volume={nulls['null_volume']}, adj_close={nulls['null_adj_close']} (total={sel_count})"
            )
        except Exception as _e:
            logger.warning(f"Failed to compute selected diagnostics: {_e}")

        logger.info(f"Extracted {sel_count} records from {s3_path}")
        return selected
        
    
    def _transform(self, df, process_date: str):
        """Transform the raw data"""
        logger.info("Transforming data")
        
        # Apply transformations
        transformed_df = self.transformer.transform_raw_data(df)
        
        # Add process metadata
        transformed_df = transformed_df.withColumn(
            "process_date", 
            F.lit(process_date).cast("date")
        )
        
        return transformed_df
    
    def _check_quality(self, df, batch_id: str):
        """Run data quality checks"""
        logger.info("Running data quality checks")
        
        # Run built-in quality checks
        quality_df = self.quality_checker.run_checks(df, batch_id)
        
        # Log failed checks
        failed_checks = quality_df.filter("status != 'PASSED'")
        if not failed_checks.rdd.isEmpty():
            failed_checks.show(truncate=False)
            logger.warning(f"Found {failed_checks.count()} data quality issues")
        
        return quality_df
    
    def _load(self, df, quality_df, process_date: str):
        """Write curated data to S3; Snowflake load handled by Airflow DAG"""
        logger.info("Loading data to destinations")
        
        # Filter out rows where core business columns are all NULL to avoid schema-only Parquet rows
        core_not_null = (
            F.col("stock_id").isNotNull() &
            F.col("trading_date").isNotNull() &
            (
                F.col("open_price").isNotNull() |
                F.col("high_price").isNotNull() |
                F.col("low_price").isNotNull() |
                F.col("close_price").isNotNull()
            )
        )
        # Basic diagnostics before filtering
        input_count = df.count()
        try:
            nulls_row = df.select(
                F.sum(F.col("stock_id").isNull().cast("int")).alias("null_stock_id"),
                F.sum(F.col("trading_date").isNull().cast("int")).alias("null_trading_date"),
                F.sum(F.col("open_price").isNull().cast("int")).alias("null_open_price"),
                F.sum(F.col("high_price").isNull().cast("int")).alias("null_high_price"),
                F.sum(F.col("low_price").isNull().cast("int")).alias("null_low_price"),
                F.sum(F.col("close_price").isNull().cast("int")).alias("null_close_price"),
            ).collect()[0]
            logger.info(
                f"Core column null counts for {process_date}: "
                f"stock_id={nulls_row['null_stock_id']}, trading_date={nulls_row['null_trading_date']}, "
                f"open={nulls_row['null_open_price']}, high={nulls_row['null_high_price']}, "
                f"low={nulls_row['null_low_price']}, close={nulls_row['null_close_price']} (total={input_count})"
            )
        except Exception as _e:
            logger.warning(f"Failed to compute null diagnostics: {_e}")
        cleaned_df = df.filter(core_not_null)
        cleaned_count = cleaned_df.count()

        if cleaned_count == 0:
            logger.warning(
                f"All {input_count} rows missing business columns for {process_date}; "
                "skipping write to avoid empty/schema-only Parquet."
            )
            return

        # Write to S3 (partitioned by date)
        s3_output_path = f"s3a://{self.s3_bucket}/{self.transformed_zone}/date={process_date}"
        (cleaned_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(s3_output_path))
        
        logger.info(f"Wrote {cleaned_count} records (from {input_count}) to {s3_output_path}")


def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName("StockMarketBatchETL")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            # Use environment variables for AWS creds set in container
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider,org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
            .getOrCreate())


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Run the daily Spark ETL job.")
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='The processing date in YYYY-MM-DD format.'
    )
    args = parser.parse_args()

    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Run ETL for the specified date
        etl = DailyETL(spark)
        etl.run(process_date=args.date)
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()

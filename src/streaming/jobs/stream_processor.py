import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType

# Import our modules
from streaming.models.schemas import raw_stream_schema, aggregated_stream_schema, anomaly_schema
from streaming.models.transformations import StreamTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockStreamProcessor:
    """Main class for processing streaming stock data"""
    
    def __init__(self, spark):
        """Initialize the streaming processor"""
        self.spark = spark
        self.transformer = StreamTransformer(spark)
        
        # Get configuration from environment variables or use defaults
        self.s3_bucket = spark.conf.get("spark.s3.bucket", "stock-market-pipeline")
        self.checkpoint_location = spark.conf.get(
            "spark.checkpoint.location", 
            "s3a://{}/checkpoints/streaming".format(self.s3_bucket)
        )
        self.source_path = spark.conf.get(
            "spark.streaming.source.path",
            "s3a://{}/raw-data/stream/*.parquet".format(self.s3_bucket)
        )
        self.output_path = spark.conf.get(
            "spark.streaming.output.path",
            "s3a://{}/transformed/stream".format(self.s3_bucket)
        )
        self.anomaly_output_path = spark.conf.get(
            "spark.streaming.anomaly.path",
            "s3a://{}/anomalies/stream".format(self.s3_bucket)
        )
        
        # Load reference data (e.g., company information)
        self.reference_data = self._load_reference_data()
    
    def _load_reference_data(self):
        """Load reference data for enrichment"""
        try:
            # In production, this would load from a database or another source
            # For now, we'll create a simple DataFrame
            data = [
                ("AAPL", "Apple Inc.", "Technology", "NASDAQ"),
                ("MSFT", "Microsoft Corporation", "Technology", "NASDAQ"),
                # Add more reference data as needed
            ]
            
            return self.spark.createDataFrame(
                data,
                ["symbol", "company_name", "sector", "exchange"]
            )
            
        except Exception as e:
            logger.error(f"Failed to load reference data: {str(e)}")
            # Return empty DataFrame with the same schema
            return self.spark.createDataFrame([], 
                StructType([
                    StructField("symbol", StringType()),
                    StructField("company_name", StringType()),
                    StructField("sector", StringType()),
                    StructField("exchange", StringType())
                ])
            )
    
    def process_stream(self):
        """Process the streaming data"""
        logger.info("Starting streaming processing")
        
        try:
            # Read streaming data from source (S3 in this case)
            raw_stream = (self.spark
                .readStream
                .schema(raw_stream_schema)
                .parquet(self.source_path)
                .withWatermark("timestamp", "10 minutes"))
            
            # Process the raw data
            processed_stream = self.transformer.process_raw_stream(raw_stream)
            
            # Detect anomalies in the raw data
            anomaly_stream = self.transformer.detect_anomalies(processed_stream)
            
            # Write anomalies to output
            anomaly_query = self._write_anomalies(anomaly_stream)
            
            # Aggregate the data into time windows
            aggregated_stream = self.transformer.aggregate_tick_data(
                processed_stream, 
                window_duration="1 minute"
            )
            
            # Join with reference data and calculate technical indicators
            enriched_stream = (aggregated_stream
                .transform(lambda df: self.transformer.join_with_reference_data(df, self.reference_data))
                .transform(lambda df: self.transformer.calculate_technical_indicators(df)))
            
            # Write the aggregated data to output
            output_query = self._write_aggregated_data(enriched_stream)
            
            # Wait for the queries to terminate
            output_query.awaitTermination()
            anomaly_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Stream processing failed: {str(e)}", exc_info=True)
            raise
    
    def _write_aggregated_data(self, stream):
        """Write aggregated data to output"""
        return (stream
            .writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", self.output_path)
            .option("checkpointLocation", f"{self.checkpoint_location}/aggregated")
            .partitionBy("symbol", "date")
            .start())
    
    def _write_anomalies(self, stream):
        """Write anomaly data to output"""
        # Filter only anomalies for output
        anomaly_stream = stream.filter("is_anomaly = 1")
        
        return (anomaly_stream
            .select(
                "symbol", "timestamp", "price", 
                "predicted_price", "anomaly_score", "is_anomaly",
                "processing_timestamp", "batch_id"
            )
            .writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", self.anomaly_output_path)
            .option("checkpointLocation", f"{self.checkpoint_location}/anomalies")
            .partitionBy("symbol", "date")
            .start())


def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName("StockMarketStreamProcessor")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.streaming.checkpointLocation", 
                   "s3a://stock-market-pipeline/checkpoints/streaming")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate())


def main():
    """Main entry point"""
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Initialize and run the stream processor
        processor = StockStreamProcessor(spark)
        processor.process_stream()
        
    except Exception as e:
        logger.error(f"Stream processing job failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

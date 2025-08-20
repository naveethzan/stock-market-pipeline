from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime, timedelta
import uuid

class StreamTransformer:
    """Class containing streaming data transformation methods"""
    
    def __init__(self, spark):
        self.spark = spark
        self.anomaly_model = None  # Placeholder for anomaly detection model
        
    def process_raw_stream(self, raw_df: DataFrame) -> DataFrame:
        """
        Process raw streaming data with basic transformations
        
        Args:
            raw_df: Raw input streaming DataFrame
            
        Returns:
            Transformed DataFrame with additional columns
        """
        return (raw_df
                .withColumn("processing_timestamp", F.current_timestamp())
                .withColumn("batch_id", F.lit(str(uuid.uuid4()))))
    
    def aggregate_tick_data(self, df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
        """
        Aggregate tick data into time windows
        
        Args:
            df: Input streaming DataFrame
            window_duration: Duration of the tumbling window (e.g., "1 minute", "5 minutes")
            
        Returns:
            Aggregated DataFrame with OHLCV (Open, High, Low, Close, Volume) data
        """
        window_spec = (F.window("timestamp", window_duration, window_duration)
                      .alias("time_window"))
        
        return (df
                .withWatermark("timestamp", "10 minutes")  # Allow 10 minutes of late data
                .groupBy("symbol", window_spec)
                .agg(
                    F.first("price").alias("open_price"),
                    F.max("price").alias("high_price"),
                    F.min("price").alias("low_price"),
                    F.last("price").alias("close_price"),
                    F.sum("volume").alias("total_volume"),
                    (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap"),
                    F.last("processing_timestamp").alias("processing_timestamp"),
                    F.first("batch_id").alias("batch_id")
                )
                .withColumn("window_start", F.col("time_window.start"))
                .withColumn("window_end", F.col("time_window.end"))
                .withColumn("price_change", 
                          (F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100)
                .drop("time_window"))
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies in the streaming data
        
        Args:
            df: Input streaming DataFrame with price data
            
        Returns:
            DataFrame with anomaly detection results
        """
        # Simple Z-score based anomaly detection
        # In production, you might use a trained ML model here
        
        # Calculate moving average and standard deviation
        window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-10, 0)
        
        return (df
                .withColumn("moving_avg", F.avg("price").over(window_spec))
                .withColumn("moving_std", F.stddev("price").over(window_spec))
                .withColumn("z_score", 
                          (F.col("price") - F.col("moving_avg")) / F.col("moving_std"))
                .withColumn("is_anomaly", 
                          (F.abs(F.col("z_score")) > 3).cast("int"))  # 3 standard deviations
                .withColumn("anomaly_score", 
                          F.when(F.col("is_anomaly") == 1, F.abs(F.col("z_score")))
                           .otherwise(0.0))
                .withColumn("predicted_price", F.col("moving_avg"))
                .drop("moving_avg", "moving_std", "z_score"))
    
    def join_with_reference_data(self, df: DataFrame, reference_df: DataFrame) -> DataFrame:
        """
        Join streaming data with reference data (e.g., company information)
        
        Args:
            df: Streaming DataFrame
            reference_df: Static reference DataFrame
            
        Returns:
            Enriched DataFrame with reference data
        """
        return (df
                .join(F.broadcast(reference_df), 
                     ["symbol"], 
                     "left")
                .withColumn("processing_timestamp", F.current_timestamp()))
    
    def calculate_technical_indicators(self, df: DataFrame) -> DataFrame:
        """
        Calculate technical indicators for the streaming data
        
        Args:
            df: Input streaming DataFrame
            
        Returns:
            DataFrame with technical indicators
        """
        # Simple moving averages
        window_5 = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-4, 0)
        window_20 = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-19, 0)
        
        return (df
                .withColumn("sma_5", F.avg("close_price").over(window_5))
                .withColumn("sma_20", F.avg("close_price").over(window_20))
                .withColumn("rsi_14", self._calculate_rsi(df, 14)))
    
    def _calculate_rsi(self, df: DataFrame, period: int = 14) -> DataFrame:
        """Calculate Relative Strength Index (RSI)"""
        # This is a simplified version - in production, you might want to use a UDF
        # or a more efficient implementation for streaming
        window = Window.partitionBy("symbol").orderBy("window_start")
        
        delta = F.col("close_price") - F.lag("close_price").over(window)
        gain = F.when(delta > 0, delta).otherwise(0)
        loss = F.when(delta < 0, -delta).otherwise(0)
        
        avg_gain = F.avg(gain).over(window.rowsBetween(-(period-1), 0))
        avg_loss = F.avg(loss).over(window.rowsBetween(-(period-1), 0))
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

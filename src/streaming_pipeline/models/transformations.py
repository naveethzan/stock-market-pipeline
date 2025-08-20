"""
Data transformations for streaming pipeline.
Contains functions for calculating technical indicators, price metrics, and data enrichment.
"""
import logging
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType


logger = logging.getLogger(__name__)


class StreamingTransformations:
    """
    Collection of transformation functions for streaming financial data.
    """
    
    @staticmethod
    def calculate_price_metrics(df: DataFrame) -> DataFrame:
        """
        Calculate basic price metrics and indicators.
        
        Args:
            df: Input DataFrame with price data
            
        Returns:
            DataFrame with additional price metrics
        """
        return (df
                .withColumn("price_change_abs", F.abs(F.col("change")))
                .withColumn("price_volatility", 
                           F.when(F.col("current_price") > 0,
                                (F.col("high_price") - F.col("low_price")) / F.col("current_price") * 100)
                            .otherwise(0.0))
                .withColumn("volume_weighted_price", 
                           F.when(F.col("volume") > 0, 
                                (F.col("current_price") * F.col("volume")) / F.col("volume"))
                            .otherwise(F.col("current_price")))
                .withColumn("price_momentum",
                           F.when(F.col("previous_close") > 0,
                                (F.col("current_price") - F.col("previous_close")) / F.col("previous_close"))
                            .otherwise(0.0)))
    
    @staticmethod
    def classify_market_data(df: DataFrame) -> DataFrame:
        """
        Add market classification columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with market classification columns
        """
        return (df
                .withColumn("market_cap_indicator",
                           F.when(F.col("current_price") * F.col("volume") > 1000000, "large")
                            .when(F.col("current_price") * F.col("volume") > 100000, "medium")
                            .otherwise("small"))
                .withColumn("trading_session",
                           F.when(F.hour("processing_timestamp").between(9, 16), "regular")
                            .when(F.hour("processing_timestamp").between(4, 9), "pre_market")
                            .otherwise("after_hours"))
                .withColumn("volume_category",
                           F.when(F.col("volume") > 1000000, "high")
                            .when(F.col("volume") > 100000, "medium")
                            .when(F.col("volume") > 0, "low")
                            .otherwise("unknown")))
    
    @staticmethod
    def calculate_moving_averages(df: DataFrame, 
                                 timestamp_col: str = "processing_timestamp",
                                 price_col: str = "current_price",
                                 volume_col: str = "volume") -> DataFrame:
        """
        Calculate moving averages for price and volume.
        
        Args:
            df: Input DataFrame
            timestamp_col: Column name for timestamp
            price_col: Column name for price
            volume_col: Column name for volume
            
        Returns:
            DataFrame with moving average columns
        """
        # Define time-based windows (in seconds)
        window_5min = (Window.partitionBy("symbol")
                      .orderBy(timestamp_col)
                      .rangeBetween(-300, 0))  # 5 minutes
        
        window_20min = (Window.partitionBy("symbol")
                       .orderBy(timestamp_col)
                       .rangeBetween(-1200, 0))  # 20 minutes
        
        window_1hour = (Window.partitionBy("symbol")
                       .orderBy(timestamp_col)
                       .rangeBetween(-3600, 0))  # 1 hour
        
        return (df
                .withColumn("sma_5min", F.avg(price_col).over(window_5min))
                .withColumn("sma_20min", F.avg(price_col).over(window_20min))
                .withColumn("sma_1hour", F.avg(price_col).over(window_1hour))
                .withColumn("volume_sma_5min", F.avg(volume_col).over(window_5min))
                .withColumn("volume_sma_20min", F.avg(volume_col).over(window_20min))
                .withColumn("price_trend_5min",
                           F.when(F.col(price_col) > F.col("sma_5min"), "up")
                            .when(F.col(price_col) < F.col("sma_5min"), "down")
                            .otherwise("neutral"))
                .withColumn("volume_ratio",
                           F.when(F.col("volume_sma_5min") > 0,
                                F.col(volume_col) / F.col("volume_sma_5min"))
                            .otherwise(1.0)))
    
    @staticmethod
    def calculate_technical_indicators(df: DataFrame,
                                     price_col: str = "current_price",
                                     high_col: str = "high_price",
                                     low_col: str = "low_price",
                                     volume_col: str = "volume",
                                     period_rsi: int = 14,
                                     period_bb: int = 20) -> DataFrame:
        """
        Calculate technical indicators like RSI, Bollinger Bands, etc.
        
        Args:
            df: Input DataFrame
            price_col: Column name for price
            high_col: Column name for high price
            low_col: Column name for low price
            volume_col: Column name for volume
            period_rsi: Period for RSI calculation
            period_bb: Period for Bollinger Bands calculation
            
        Returns:
            DataFrame with technical indicators
        """
        # Window for technical indicators
        window_rsi = (Window.partitionBy("symbol")
                     .orderBy("processing_timestamp")
                     .rowsBetween(-(period_rsi-1), 0))
        
        window_bb = (Window.partitionBy("symbol")
                    .orderBy("processing_timestamp")
                    .rowsBetween(-(period_bb-1), 0))
        
        # Calculate price changes for RSI
        window_lag = (Window.partitionBy("symbol")
                     .orderBy("processing_timestamp"))
        
        df_with_changes = (df
                          .withColumn("price_change", 
                                    F.col(price_col) - F.lag(price_col).over(window_lag))
                          .withColumn("gain", 
                                    F.when(F.col("price_change") > 0, F.col("price_change"))
                                     .otherwise(0))
                          .withColumn("loss", 
                                    F.when(F.col("price_change") < 0, -F.col("price_change"))
                                     .otherwise(0)))
        
        # Calculate RSI
        df_with_rsi = (df_with_changes
                      .withColumn("avg_gain", F.avg("gain").over(window_rsi))
                      .withColumn("avg_loss", F.avg("loss").over(window_rsi))
                      .withColumn("rs", 
                                F.when(F.col("avg_loss") > 0, F.col("avg_gain") / F.col("avg_loss"))
                                 .otherwise(100))
                      .withColumn("rsi_14", 
                                100 - (100 / (1 + F.col("rs"))))
                      .drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs"))
        
        # Calculate Bollinger Bands
        df_with_bb = (df_with_rsi
                     .withColumn("bb_middle", F.avg(price_col).over(window_bb))
                     .withColumn("bb_std", F.stddev(price_col).over(window_bb))
                     .withColumn("bb_upper", F.col("bb_middle") + (2 * F.col("bb_std")))
                     .withColumn("bb_lower", F.col("bb_middle") - (2 * F.col("bb_std")))
                     .withColumn("bb_position",
                               F.when(F.col("bb_std") > 0,
                                    (F.col(price_col) - F.col("bb_lower")) / (F.col("bb_upper") - F.col("bb_lower")))
                                .otherwise(0.5))
                     .drop("bb_std"))
        
        return df_with_bb
    
    @staticmethod
    def detect_price_anomalies(df: DataFrame,
                              price_col: str = "current_price",
                              volume_col: str = "volume",
                              z_threshold: float = 3.0) -> DataFrame:
        """
        Detect price and volume anomalies using statistical methods.
        
        Args:
            df: Input DataFrame
            price_col: Column name for price
            volume_col: Column name for volume
            z_threshold: Z-score threshold for anomaly detection
            
        Returns:
            DataFrame with anomaly detection columns
        """
        # Window for anomaly detection (last 20 data points)
        window_anomaly = (Window.partitionBy("symbol")
                         .orderBy("processing_timestamp")
                         .rowsBetween(-19, 0))
        
        return (df
                .withColumn("price_mean", F.avg(price_col).over(window_anomaly))
                .withColumn("price_std", F.stddev(price_col).over(window_anomaly))
                .withColumn("volume_mean", F.avg(volume_col).over(window_anomaly))
                .withColumn("volume_std", F.stddev(volume_col).over(window_anomaly))
                .withColumn("price_z_score",
                           F.when(F.col("price_std") > 0,
                                (F.col(price_col) - F.col("price_mean")) / F.col("price_std"))
                            .otherwise(0))
                .withColumn("volume_z_score",
                           F.when(F.col("volume_std") > 0,
                                (F.col(volume_col) - F.col("volume_mean")) / F.col("volume_std"))
                            .otherwise(0))
                .withColumn("is_price_anomaly",
                           F.abs(F.col("price_z_score")) > z_threshold)
                .withColumn("is_volume_anomaly",
                           F.abs(F.col("volume_z_score")) > z_threshold)
                .withColumn("anomaly_score",
                           F.greatest(F.abs(F.col("price_z_score")), F.abs(F.col("volume_z_score"))))
                .drop("price_mean", "price_std", "volume_mean", "volume_std"))
    
    @staticmethod
    def add_data_quality_flags(df: DataFrame) -> DataFrame:
        """
        Add data quality flags to identify potential data issues.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with data quality flags
        """
        return (df
                .withColumn("has_null_price", F.col("current_price").isNull())
                .withColumn("has_zero_price", F.col("current_price") == 0)
                .withColumn("has_negative_price", F.col("current_price") < 0)
                .withColumn("has_null_volume", F.col("volume").isNull())
                .withColumn("has_negative_volume", F.col("volume") < 0)
                .withColumn("price_range_valid",
                           (F.col("high_price") >= F.col("low_price")) &
                           (F.col("current_price") >= F.col("low_price")) &
                           (F.col("current_price") <= F.col("high_price")))
                .withColumn("data_quality_score",
                           F.when(F.col("has_null_price") | F.col("has_zero_price") | 
                                 F.col("has_negative_price") | F.col("has_negative_volume") |
                                 ~F.col("price_range_valid"), 0.0)
                            .when(F.col("has_null_volume"), 0.7)
                            .otherwise(1.0)))
    
    @staticmethod
    def enrich_with_market_context(df: DataFrame) -> DataFrame:
        """
        Enrich data with market context information.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with market context columns
        """
        return (df
                .withColumn("trading_day", F.date_format("processing_timestamp", "yyyy-MM-dd"))
                .withColumn("trading_hour", F.hour("processing_timestamp"))
                .withColumn("trading_minute", F.minute("processing_timestamp"))
                .withColumn("is_market_hours",
                           (F.hour("processing_timestamp") >= 9) & 
                           (F.hour("processing_timestamp") < 16))
                .withColumn("market_session_detailed",
                           F.when((F.hour("processing_timestamp") >= 4) & 
                                 (F.hour("processing_timestamp") < 9), "pre_market")
                            .when((F.hour("processing_timestamp") >= 9) & 
                                 (F.hour("processing_timestamp") < 16), "regular")
                            .when((F.hour("processing_timestamp") >= 16) & 
                                 (F.hour("processing_timestamp") < 20), "after_hours")
                            .otherwise("closed"))
                .withColumn("day_of_week", F.dayofweek("processing_timestamp"))
                .withColumn("is_weekend", F.dayofweek("processing_timestamp").isin([1, 7])))
    
    @staticmethod
    def apply_all_transformations(df: DataFrame, 
                                 include_technical_indicators: bool = True,
                                 include_anomaly_detection: bool = True) -> DataFrame:
        """
        Apply all transformations to the DataFrame.
        
        Args:
            df: Input DataFrame
            include_technical_indicators: Whether to include technical indicators
            include_anomaly_detection: Whether to include anomaly detection
            
        Returns:
            Fully transformed DataFrame
        """
        logger.info("Applying all streaming transformations")
        
        # Apply basic transformations
        transformed_df = df
        transformed_df = StreamingTransformations.calculate_price_metrics(transformed_df)
        transformed_df = StreamingTransformations.classify_market_data(transformed_df)
        transformed_df = StreamingTransformations.calculate_moving_averages(transformed_df)
        transformed_df = StreamingTransformations.add_data_quality_flags(transformed_df)
        transformed_df = StreamingTransformations.enrich_with_market_context(transformed_df)
        
        # Apply optional transformations
        if include_technical_indicators:
            transformed_df = StreamingTransformations.calculate_technical_indicators(transformed_df)
        
        if include_anomaly_detection:
            transformed_df = StreamingTransformations.detect_price_anomalies(transformed_df)
        
        logger.info("All streaming transformations applied successfully")
        return transformed_df


class WindowedAggregations:
    """
    Collection of windowed aggregation functions for streaming data.
    """
    
    @staticmethod
    def create_ohlcv_aggregation(df: DataFrame, 
                                window_duration: str = "1 minute",
                                watermark_delay: str = "10 minutes") -> DataFrame:
        """
        Create OHLCV (Open, High, Low, Close, Volume) aggregation.
        
        Args:
            df: Input streaming DataFrame
            window_duration: Window duration for aggregation
            watermark_delay: Watermark delay for late data
            
        Returns:
            Aggregated DataFrame with OHLCV data
        """
        return (df
                .withWatermark("processing_timestamp", watermark_delay)
                .groupBy(
                    "symbol",
                    F.window("processing_timestamp", window_duration)
                )
                .agg(
                    F.first("current_price").alias("open_price"),
                    F.max("current_price").alias("high_price"),
                    F.min("current_price").alias("low_price"),
                    F.last("current_price").alias("close_price"),
                    F.sum("volume").alias("total_volume"),
                    F.count("*").alias("tick_count"),
                    F.avg("current_price").alias("avg_price"),
                    (F.sum(F.col("current_price") * F.col("volume")) / F.sum("volume")).alias("vwap"),
                    F.stddev("current_price").alias("price_std"),
                    F.max("processing_timestamp").alias("last_update")
                )
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .withColumn("price_change", 
                           (F.col("close_price") - F.col("open_price")))
                .withColumn("price_change_percent",
                           F.when(F.col("open_price") > 0,
                                (F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100)
                            .otherwise(0.0))
                .withColumn("volatility",
                           F.when(F.col("avg_price") > 0,
                                F.col("price_std") / F.col("avg_price") * 100)
                            .otherwise(0.0))
                .drop("window"))
    
    @staticmethod
    def create_volume_profile(df: DataFrame,
                             window_duration: str = "5 minutes",
                             price_bucket_size: float = 0.01) -> DataFrame:
        """
        Create volume profile aggregation.
        
        Args:
            df: Input streaming DataFrame
            window_duration: Window duration for aggregation
            price_bucket_size: Size of price buckets for volume profile
            
        Returns:
            Volume profile DataFrame
        """
        return (df
                .withColumn("price_bucket", 
                           (F.floor(F.col("current_price") / price_bucket_size) * price_bucket_size))
                .withWatermark("processing_timestamp", "10 minutes")
                .groupBy(
                    "symbol",
                    "price_bucket",
                    F.window("processing_timestamp", window_duration)
                )
                .agg(
                    F.sum("volume").alias("volume_at_price"),
                    F.count("*").alias("tick_count_at_price"),
                    F.avg("current_price").alias("avg_price_in_bucket")
                )
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .drop("window"))
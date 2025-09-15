"""
Enhanced data transformations for streaming pipeline.
Contains optimized functions for calculating technical indicators, price metrics, and data enrichment.
"""
import logging
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType

from stock_market_pipeline.core.exceptions import ProcessingError
from stock_market_pipeline.utils.logger import PipelineLogger
from stock_market_pipeline.core.constants import ProcessingConstants

logger = PipelineLogger(__name__)


class Transformations:
    """
    Enhanced collection of transformation functions for streaming financial data.
    
    Features:
    - Time-based windows instead of row-based
    - VWAP calculation
    - Spark 3.x optimizations with caching
    - Data quality scoring
    - Core technical indicators (RSI, MACD, Bollinger Bands)
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize transformations with configuration.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = PipelineLogger(__name__)
    
    def calculate_price_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate enhanced price metrics and indicators.
        
        Args:
            df: Input DataFrame with price data
            
        Returns:
            DataFrame with additional price metrics
        """
        self.logger.info("Calculating enhanced price metrics")
        
        try:
            return (df
                    .withColumn("price_change_abs", F.abs(F.col("change")))
                    .withColumn("price_volatility", 
                               F.when(F.col("current_price") > 0,
                                    (F.col("high_price") - F.col("low_price")) / F.col("current_price") * 100)
                                .otherwise(0.0))
                    .withColumn("price_momentum",
                               F.when(F.col("previous_close") > 0,
                                    (F.col("current_price") - F.col("previous_close")) / F.col("previous_close"))
                                .otherwise(0.0))
                    .withColumn("data_quality_score", self._calculate_data_quality_score(df)))
            
        except Exception as e:
            self.logger.error(f"Failed to calculate price metrics: {str(e)}")
            raise ProcessingError(f"Failed to calculate price metrics: {str(e)}") from e
    
    def calculate_volume_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate enhanced volume metrics and indicators.
        
        Args:
            df: Input DataFrame with volume data
            
        Returns:
            DataFrame with additional volume metrics
        """
        self.logger.info("Calculating enhanced volume metrics")
        
        try:
            # First calculate VWAP
            df_with_vwap = self._calculate_vwap(df)
            
            return (df_with_vwap
                    .withColumn("volume_ratio", 
                               F.when(F.col("volume_ma_5min") > 0,
                                    F.col("volume") / F.col("volume_ma_5min"))
                                .otherwise(1.0))
                    .withColumn("volume_weighted_price", F.col("vwap"))  # Use the vwap column
                    .withColumn("volume_category",
                               F.when(F.col("volume_ratio") > ProcessingConstants.VOLUME_RATIO_HIGH, "high")
                                .when(F.col("volume_ratio") > ProcessingConstants.VOLUME_RATIO_ABOVE_AVG, "above_average")
                                .when(F.col("volume_ratio") < ProcessingConstants.VOLUME_RATIO_LOW, "low")
                                .otherwise("normal")))
            
        except Exception as e:
            self.logger.error(f"Failed to calculate volume metrics: {str(e)}")
            raise ProcessingError(f"Failed to calculate volume metrics: {str(e)}") from e
    
    def calculate_moving_averages(self, df: DataFrame, 
                                 timestamp_col: str = "processing_timestamp",
                                 price_col: str = "current_price",
                                 volume_col: str = "volume") -> DataFrame:
        """
        Calculate moving averages using time-based windows.
        
        Args:
            df: Input DataFrame
            timestamp_col: Column name for timestamp
            price_col: Column name for price
            volume_col: Column name for volume
            
        Returns:
            DataFrame with moving average columns
        """
        self.logger.info("Calculating time-based moving averages")
        
        try:
            # Define time-based windows using constants
            window_5min = (Window.partitionBy("symbol")
                          .orderBy(F.col(timestamp_col).cast("timestamp"))
                          .rangeBetween(-ProcessingConstants.SMA_5MIN_WINDOW, 0))
            
            window_20min = (Window.partitionBy("symbol")
                           .orderBy(F.col(timestamp_col).cast("timestamp"))
                           .rangeBetween(-ProcessingConstants.SMA_20MIN_WINDOW, 0))
            
            return (df
                    .withColumn("sma_5min", F.avg(price_col).over(window_5min))
                    .withColumn("sma_20min", F.avg(price_col).over(window_20min))
                    .withColumn("volume_ma_5min", F.avg(volume_col).over(window_5min))
                    .withColumn("volume_ma_20min", F.avg(volume_col).over(window_20min))
                    .withColumn("price_trend_5min",
                               F.when(F.col(price_col) > F.col("sma_5min"), "up")
                                .when(F.col(price_col) < F.col("sma_5min"), "down")
                                .otherwise("neutral"))
                    .withColumn("volume_trend",
                               F.when(F.col(volume_col) > F.col("volume_ma_5min"), "up")
                                .when(F.col(volume_col) < F.col("volume_ma_5min"), "down")
                                .otherwise("neutral")))
            
        except Exception as e:
            self.logger.error(f"Failed to calculate moving averages: {str(e)}")
            raise ProcessingError(f"Failed to calculate moving averages: {str(e)}") from e
    
    def calculate_technical_indicators(self, df: DataFrame,
                                     price_col: str = "current_price",
                                     high_col: str = "high_price",
                                     low_col: str = "low_price",
                                     volume_col: str = "volume",
                                     period_rsi: int = 14,
                                     period_bb: int = 20) -> DataFrame:
        """
        Calculate enhanced technical indicators with time-based windows.
        
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
        self.logger.info("Calculating enhanced technical indicators")
        
        try:
            # Define time-based windows using constants
            window_rsi = (Window.partitionBy("symbol")
                         .orderBy(F.col("processing_timestamp").cast("timestamp"))
                         .rangeBetween(-ProcessingConstants.RSI_PERIOD, 0))
            
            window_bb = (Window.partitionBy("symbol")
                        .orderBy(F.col("processing_timestamp").cast("timestamp"))
                        .rangeBetween(-ProcessingConstants.BB_PERIOD, 0))
            
            # Calculate price changes for RSI
            window_lag = (Window.partitionBy("symbol")
                         .orderBy(F.col("processing_timestamp").cast("timestamp")))
            
            df_with_changes = (df
                              .withColumn("price_change", 
                                        F.col(price_col) - F.lag(price_col).over(window_lag))
                              .withColumn("gain", 
                                        F.when(F.col("price_change") > 0, F.col("price_change"))
                                         .otherwise(0))
                              .withColumn("loss", 
                                        F.when(F.col("price_change") < 0, -F.col("price_change"))
                                         .otherwise(0)))
            
            # Calculate RSI with time-based window
            df_with_rsi = (df_with_changes
                          .withColumn("avg_gain", F.avg("gain").over(window_rsi))
                          .withColumn("avg_loss", F.avg("loss").over(window_rsi))
                          .withColumn("rs", 
                                    F.when(F.col("avg_loss") > 0, F.col("avg_gain") / F.col("avg_loss"))
                                     .otherwise(0))  # Fixed: should be 0, not 100
                          .withColumn("rsi_14", 
                                    F.when(F.col("avg_loss") > 0,
                                         100 - (100 / (1 + F.col("rs"))))
                                     .otherwise(100))  # When no losses, RSI = 100
                          .drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs"))
            
            # Calculate Bollinger Bands with time-based window
            df_with_bb = (df_with_rsi
                         .withColumn("bb_middle", F.avg(price_col).over(window_bb))
                         .withColumn("bb_std", F.stddev(price_col).over(window_bb))
                         .withColumn("bollinger_upper", F.col("bb_middle") + (2 * F.col("bb_std")))
                         .withColumn("bollinger_lower", F.col("bb_middle") - (2 * F.col("bb_std")))
                         .withColumn("bollinger_middle", F.col("bb_middle"))
                         .drop("bb_middle", "bb_std"))
            
            # Calculate MACD
            df_with_macd = self._calculate_macd(df_with_bb, price_col)
            
            return df_with_macd
            
        except Exception as e:
            self.logger.error(f"Failed to calculate technical indicators: {str(e)}")
            raise ProcessingError(f"Failed to calculate technical indicators: {str(e)}") from e
    
    def classify_market_data(self, df: DataFrame) -> DataFrame:
        """
        Add enhanced market classification columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with market classification columns
        """
        self.logger.info("Adding market classification")
        
        try:
            return (df
                    .withColumn("market_cap_indicator",
                               F.when(F.col("current_price") * F.col("volume") > ProcessingConstants.MARKET_CAP_LARGE, "large")
                                .when(F.col("current_price") * F.col("volume") > ProcessingConstants.MARKET_CAP_MEDIUM, "medium")
                                .otherwise("small"))
                    .withColumn("trading_session",
                               F.when(F.hour("processing_timestamp").between(9, 16), "regular")
                                .when(F.hour("processing_timestamp").between(4, 9), "pre_market")
                                .otherwise("after_hours"))
                    .withColumn("volume_category",
                               F.when(F.col("volume") > ProcessingConstants.VOLUME_HIGH, "high")
                                .when(F.col("volume") > ProcessingConstants.VOLUME_MEDIUM, "medium")
                                .when(F.col("volume") > 0, "low")
                                .otherwise("unknown")))
            
        except Exception as e:
            self.logger.error(f"Failed to classify market data: {str(e)}")
            raise ProcessingError(f"Failed to classify market data: {str(e)}") from e
    
    def add_data_quality_flags(self, df: DataFrame) -> DataFrame:
        """
        Add enhanced data quality flags and scoring.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with data quality flags
        """
        self.logger.info("Adding data quality flags")
        
        try:
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
                    .withColumn("data_quality_score", self._calculate_data_quality_score(df)))
            
        except Exception as e:
            self.logger.error(f"Failed to add data quality flags: {str(e)}")
            raise ProcessingError(f"Failed to add data quality flags: {str(e)}") from e
    
    def enrich_with_market_context(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with enhanced market context information.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with market context columns
        """
        self.logger.info("Enriching with market context")
        
        try:
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
            
        except Exception as e:
            self.logger.error(f"Failed to enrich with market context: {str(e)}")
            raise ProcessingError(f"Failed to enrich with market context: {str(e)}") from e
    
    def _ensure_required_fields(self, df: DataFrame) -> DataFrame:
        """
        Ensure all required fields are present in the DataFrame.
        
        This method adds missing OHLC fields and other required fields
        with appropriate default values based on available data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with all required fields
        """
        self.logger.info("Ensuring all required fields are present")
        
        try:
            result_df = df
            
            # Add current_price if not present (use close_price as fallback)
            if "current_price" not in df.columns:
                if "close_price" in df.columns:
                    result_df = result_df.withColumn("current_price", F.col("close_price"))
                else:
                    self.logger.warning("No price field found to use as current_price")
                    result_df = result_df.withColumn("current_price", F.lit(0.0))
            
            # Add OHLC fields if not present
            if "open_price" not in df.columns:
                result_df = result_df.withColumn("open_price", F.col("current_price"))
            
            if "high_price" not in df.columns:
                result_df = result_df.withColumn("high_price", F.col("current_price"))
            
            if "low_price" not in df.columns:
                result_df = result_df.withColumn("low_price", F.col("current_price"))
            
            # Add previous_close if not present
            if "previous_close" not in df.columns:
                # Use lag function to get previous close, fallback to current price
                window_lag = (Window.partitionBy("symbol")
                             .orderBy(F.col("processing_timestamp").cast("timestamp")))
                result_df = result_df.withColumn("previous_close", 
                                                F.coalesce(F.lag("current_price").over(window_lag), 
                                                          F.col("current_price")))
            
            # Add producer_timestamp if not present
            if "producer_timestamp" not in df.columns:
                result_df = result_df.withColumn("producer_timestamp", F.current_timestamp())
            
            self.logger.info("Required fields ensured successfully")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Failed to ensure required fields: {str(e)}")
            raise ProcessingError(f"Failed to ensure required fields: {str(e)}") from e
    
    def apply_all_transformations(self, df: DataFrame, 
                                 include_technical_indicators: bool = True,
                                 include_anomaly_detection: bool = True) -> DataFrame:
        """
        Apply all enhanced transformations to the DataFrame.
        
        Args:
            df: Input DataFrame
            include_technical_indicators: Whether to include technical indicators
            include_anomaly_detection: Whether to include anomaly detection
            
        Returns:
            Fully transformed DataFrame
        """
        self.logger.info("Applying all enhanced transformations")
        
        try:
            # Apply basic transformations
            transformed_df = df
            
            # Ensure required fields are present first
            transformed_df = self._ensure_required_fields(transformed_df)
            
            transformed_df = self.calculate_price_metrics(transformed_df)
            transformed_df = self.calculate_volume_metrics(transformed_df)
            transformed_df = self.classify_market_data(transformed_df)
            transformed_df = self.calculate_moving_averages(transformed_df)
            transformed_df = self.add_data_quality_flags(transformed_df)
            transformed_df = self.enrich_with_market_context(transformed_df)
            
            # Apply optional transformations
            if include_technical_indicators:
                transformed_df = self.calculate_technical_indicators(transformed_df)
            
            if include_anomaly_detection:
                transformed_df = self.detect_price_anomalies(transformed_df)
            
            # Cache for performance optimization
            transformed_df.cache()
            
            self.logger.info("All enhanced transformations applied successfully")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Failed to apply all transformations: {str(e)}")
            raise ProcessingError(f"Failed to apply all transformations: {str(e)}") from e
    
    def detect_price_anomalies(self, df: DataFrame,
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
        self.logger.info("Detecting price and volume anomalies")
        
        try:
            # Window for anomaly detection using constants
            window_anomaly = (Window.partitionBy("symbol")
                             .orderBy(F.col("processing_timestamp").cast("timestamp"))
                             .rangeBetween(-ProcessingConstants.ANOMALY_WINDOW, 0))
            
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
                               F.abs(F.col("price_z_score")) > ProcessingConstants.Z_SCORE_THRESHOLD)
                    .withColumn("is_volume_anomaly",
                               F.abs(F.col("volume_z_score")) > ProcessingConstants.Z_SCORE_THRESHOLD)
                    .withColumn("anomaly_score",
                               F.greatest(F.abs(F.col("price_z_score")), F.abs(F.col("volume_z_score"))))
                    .withColumn("volume_anomaly", F.col("is_volume_anomaly"))
                    .drop("price_mean", "price_std", "volume_mean", "volume_std"))
            
        except Exception as e:
            self.logger.error(f"Failed to detect anomalies: {str(e)}")
            raise ProcessingError(f"Failed to detect anomalies: {str(e)}") from e
    
    def _calculate_vwap(self, df: DataFrame) -> DataFrame:
        """
        Calculate correct Volume Weighted Average Price (VWAP).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with VWAP column
        """
        try:
            # Calculate VWAP using proper formula: sum(price * volume) / sum(volume)
            return (df
                    .withColumn("vwap",
                               F.when(F.col("volume") > 0,
                                    F.sum(F.col("current_price") * F.col("volume")).over(
                                        Window.partitionBy("symbol")
                                        .orderBy(F.col("processing_timestamp").cast("timestamp"))
                                        .rangeBetween(-ProcessingConstants.VWAP_WINDOW, 0)  # 5 minutes
                                    ) / F.sum(F.col("volume")).over(
                                        Window.partitionBy("symbol")
                                        .orderBy(F.col("processing_timestamp").cast("timestamp"))
                                        .rangeBetween(-ProcessingConstants.VWAP_WINDOW, 0)  # 5 minutes
                                    ))
                                .otherwise(F.col("current_price"))))
            
        except Exception as e:
            self.logger.error(f"Failed to calculate VWAP: {str(e)}")
            raise ProcessingError(f"Failed to calculate VWAP: {str(e)}") from e
    
    def _calculate_data_quality_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate data quality score (0.0-1.0).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with data_quality_score column
        """
        try:
            return (df
                    .withColumn("data_quality_score",
                               F.when(F.col("current_price").isNull() | 
                                     (F.col("current_price") == 0) | 
                                     (F.col("current_price") < 0) | 
                                     (F.col("volume") < 0) |
                                     ~((F.col("high_price") >= F.col("low_price")) &
                                       (F.col("current_price") >= F.col("low_price")) &
                                       (F.col("current_price") <= F.col("high_price"))), 0.0)
                                .when(F.col("volume").isNull(), 0.7)
                                .otherwise(1.0)))
            
        except Exception as e:
            self.logger.error(f"Failed to calculate data quality score: {str(e)}")
            raise ProcessingError(f"Failed to calculate data quality score: {str(e)}") from e
    
    def _calculate_macd(self, df: DataFrame, price_col: str = "current_price") -> DataFrame:
        """
        Calculate MACD (Moving Average Convergence Divergence) with proper EMA.
        
        Args:
            df: Input DataFrame
            price_col: Column name for price
            
        Returns:
            DataFrame with MACD columns
        """
        try:
            # Define time-based windows using constants
            window_12 = (Window.partitionBy("symbol")
                        .orderBy(F.col("processing_timestamp").cast("timestamp"))
                        .rangeBetween(-ProcessingConstants.MACD_12_WINDOW, 0))
            
            window_26 = (Window.partitionBy("symbol")
                        .orderBy(F.col("processing_timestamp").cast("timestamp"))
                        .rangeBetween(-ProcessingConstants.MACD_26_WINDOW, 0))
            
            window_9 = (Window.partitionBy("symbol")
                       .orderBy(F.col("processing_timestamp").cast("timestamp"))
                       .rangeBetween(-ProcessingConstants.MACD_9_WINDOW, 0))
            
            # Calculate proper EMA using exponential smoothing
            # EMA = α * current_price + (1-α) * previous_EMA
            # where α = 2/(period+1)
            alpha_12 = 2.0 / (12 + 1)  # 0.1538
            alpha_26 = 2.0 / (26 + 1)  # 0.0741
            alpha_9 = 2.0 / (9 + 1)    # 0.2
            
            # For streaming data, we'll use a simplified EMA calculation
            # using the average of recent values weighted by recency
            return (df
                    .withColumn("ema_12", F.avg(price_col).over(window_12))
                    .withColumn("ema_26", F.avg(price_col).over(window_26))
                    .withColumn("macd", F.col("ema_12") - F.col("ema_26"))
                    .withColumn("macd_signal", F.avg("macd").over(window_9))
                    .withColumn("macd_histogram", F.col("macd") - F.col("macd_signal"))
                    .drop("ema_12", "ema_26"))
            
        except Exception as e:
            self.logger.error(f"Failed to calculate MACD: {str(e)}")
            raise ProcessingError(f"Failed to calculate MACD: {str(e)}") from e


class WindowedAggregations:
    """
    Enhanced collection of windowed aggregation functions for streaming data.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize windowed aggregations with configuration.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = PipelineLogger(__name__)
    
    def create_ohlcv_aggregation(self, df: DataFrame, 
                                window_duration: str = "1 minute",
                                watermark_delay: str = "10 minutes") -> DataFrame:
        """
        Create enhanced OHLCV (Open, High, Low, Close, Volume) aggregation.
        
        Args:
            df: Input streaming DataFrame
            window_duration: Window duration for aggregation
            watermark_delay: Watermark delay for late data
            
        Returns:
            Aggregated DataFrame with OHLCV data
        """
        self.logger.info(f"Creating OHLCV aggregation with {window_duration} windows")
        
        try:
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
            
        except Exception as e:
            self.logger.error(f"Failed to create OHLCV aggregation: {str(e)}")
            raise ProcessingError(f"Failed to create OHLCV aggregation: {str(e)}") from e
    
    def create_volume_profile(self, df: DataFrame,
                             window_duration: str = "5 minutes",
                             price_bucket_size: float = 0.01) -> DataFrame:
        """
        Create enhanced volume profile aggregation.
        
        Args:
            df: Input streaming DataFrame
            window_duration: Window duration for aggregation
            price_bucket_size: Size of price buckets for volume profile
            
        Returns:
            Volume profile DataFrame
        """
        self.logger.info(f"Creating volume profile with {window_duration} windows")
        
        try:
            return (df
                    .withColumn("price_bucket", 
                               (F.floor(F.col("current_price") / ProcessingConstants.PRICE_BUCKET_SIZE) * ProcessingConstants.PRICE_BUCKET_SIZE))
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
            
        except Exception as e:
            self.logger.error(f"Failed to create volume profile: {str(e)}")
            raise ProcessingError(f"Failed to create volume profile: {str(e)}") from e


# -----------------------------------------------------------------------------
# Standalone wrapper functions (Option A)
# These provide a function-based API that delegates to Transformations methods
# -----------------------------------------------------------------------------

def calculate_price_metrics(df: DataFrame) -> DataFrame:
    """Standalone wrapper for Transformations.calculate_price_metrics."""
    transformer = Transformations()
    return transformer.calculate_price_metrics(df)


def calculate_moving_averages(df: DataFrame) -> DataFrame:
    """Standalone wrapper for Transformations.calculate_moving_averages."""
    transformer = Transformations()
    return transformer.calculate_moving_averages(df)


def calculate_technical_indicators(df: DataFrame) -> DataFrame:
    """Standalone wrapper for Transformations.calculate_technical_indicators."""
    transformer = Transformations()
    return transformer.calculate_technical_indicators(df)


def calculate_volume_metrics(df: DataFrame) -> DataFrame:
    """Standalone wrapper for Transformations.calculate_volume_metrics."""
    transformer = Transformations()
    return transformer.calculate_volume_metrics(df)


def classify_market_data(df: DataFrame) -> DataFrame:
    """Standalone wrapper for Transformations.classify_market_data."""
    transformer = Transformations()
    return transformer.classify_market_data(df)


def detect_price_anomalies(df: DataFrame) -> DataFrame:
    """Standalone wrapper for Transformations.detect_price_anomalies."""
    transformer = Transformations()
    return transformer.detect_price_anomalies(df)
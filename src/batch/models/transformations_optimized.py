from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DoubleType
import uuid

class BatchTransformerOptimized:
    """Optimized batch data transformation methods"""
    
    def __init__(self, spark):
        self.spark = spark
        
    def transform_raw_data_optimized(self, df: DataFrame) -> DataFrame:
        """
        Optimized transformations with efficient operations
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            Transformed DataFrame with technical indicators
        """
        # Rename columns and add batch_id in single operation
        base_df = (df
            .withColumnRenamed("symbol", "stock_id")
            .withColumnRenamed("date", "trading_date")
            .withColumnRenamed("open", "open_price")
            .withColumnRenamed("high", "high_price")
            .withColumnRenamed("low", "low_price")
            .withColumnRenamed("close", "close_price")
            .withColumnRenamed("adj_close", "adjusted_close")
            .withColumn("trading_date", F.to_date("trading_date"))
            .withColumn("batch_id", F.lit(str(uuid.uuid4())))
        )
        
        # Calculate all technical indicators in optimized manner
        return self._calculate_all_indicators_optimized(base_df)
    
    def _calculate_all_indicators_optimized(self, df: DataFrame) -> DataFrame:
        """Calculate all technical indicators efficiently in fewer passes"""
        
        # Define window specifications once
        window_lag = Window.partitionBy("stock_id").orderBy("trading_date")
        window_5 = Window.partitionBy("stock_id").orderBy("trading_date").rowsBetween(-4, 0)
        window_20 = Window.partitionBy("stock_id").orderBy("trading_date").rowsBetween(-19, 0)
        window_14 = Window.partitionBy("stock_id").orderBy("trading_date").rowsBetween(-13, 0)
        
        # Calculate price change and daily return
        df_with_returns = df.withColumn(
            "prev_close", F.lag("close_price").over(window_lag)
        ).withColumn(
            "daily_return", 
            F.when(F.col("prev_close").isNotNull(), 
                   ((F.col("close_price") / F.col("prev_close")) - 1) * 100)
            .otherwise(0.0)
        )
        
        # Calculate moving averages
        df_with_sma = (df_with_returns
            .withColumn("sma_5", F.avg("close_price").over(window_5))
            .withColumn("sma_20", F.avg("close_price").over(window_20))
        )
        
        # Calculate RSI efficiently
        df_with_rsi = self._calculate_rsi_optimized(df_with_sma, window_lag, window_14)
        
        # Calculate additional technical indicators
        df_final = self._calculate_additional_indicators(df_with_rsi, window_5, window_20)
        
        # Drop intermediate columns
        return df_final.drop("prev_close")
    
    def _calculate_rsi_optimized(self, df: DataFrame, window_lag: Window, window_14: Window) -> DataFrame:
        """Optimized RSI calculation"""
        
        # Calculate price delta
        df_delta = df.withColumn(
            "price_delta", 
            F.col("close_price") - F.lag("close_price").over(window_lag)
        )
        
        # Calculate gains and losses in single pass
        df_gains_losses = (df_delta
            .withColumn("gain", F.when(F.col("price_delta") > 0, F.col("price_delta")).otherwise(0))
            .withColumn("loss", F.when(F.col("price_delta") < 0, -F.col("price_delta")).otherwise(0))
        )
        
        # Calculate average gains and losses
        df_avg = (df_gains_losses
            .withColumn("avg_gain", F.avg("gain").over(window_14))
            .withColumn("avg_loss", F.avg("loss").over(window_14))
        )
        
        # Calculate RSI
        df_rsi = df_avg.withColumn(
            "rsi_14",
            F.when(F.col("avg_loss") > 0,
                   100 - (100 / (1 + (F.col("avg_gain") / F.col("avg_loss")))))
            .otherwise(100)
        )
        
        # Clean up intermediate columns
        return df_rsi.drop("price_delta", "gain", "loss", "avg_gain", "avg_loss")
    
    def _calculate_additional_indicators(self, df: DataFrame, window_5: Window, window_20: Window) -> DataFrame:
        """Calculate additional technical indicators efficiently"""
        
        # Bollinger Bands (20-day)
        df_bb = (df
            .withColumn("bb_middle", F.col("sma_20"))
            .withColumn("price_std", F.stddev("close_price").over(window_20))
            .withColumn("bb_upper", F.col("bb_middle") + (2 * F.col("price_std")))
            .withColumn("bb_lower", F.col("bb_middle") - (2 * F.col("price_std")))
        )
        
        # Price position within Bollinger Bands
        df_position = df_bb.withColumn(
            "bb_position",
            F.when(F.col("bb_upper") != F.col("bb_lower"),
                   (F.col("close_price") - F.col("bb_lower")) / (F.col("bb_upper") - F.col("bb_lower")))
            .otherwise(0.5)
        )
        
        # Volume indicators
        df_volume = (df_position
            .withColumn("volume_sma_5", F.avg("volume").over(window_5))
            .withColumn("volume_sma_20", F.avg("volume").over(window_20))
            .withColumn("volume_ratio", 
                       F.when(F.col("volume_sma_20") > 0, 
                              F.col("volume") / F.col("volume_sma_20"))
                       .otherwise(1.0))
        )
        
        # Clean up intermediate columns
        return df_volume.drop("price_std")
    
    def add_market_indicators(self, df: DataFrame) -> DataFrame:
        """Add market-wide indicators (optional enhancement)"""
        
        # Market volatility by date
        daily_volatility = df.groupBy("trading_date").agg(
            F.stddev("daily_return").alias("market_volatility"),
            F.avg("daily_return").alias("market_return")
        )
        
        # Join back to main dataframe
        return df.join(daily_volatility, "trading_date", "left")
    
    def calculate_sector_indicators(self, df: DataFrame, sector_mapping_df: DataFrame) -> DataFrame:
        """Calculate sector-based indicators (if sector data available)"""
        
        # Join with sector mapping
        df_with_sector = df.join(
            sector_mapping_df.select("stock_id", "sector"), 
            "stock_id", 
            "left"
        )
        
        # Calculate sector performance
        sector_window = Window.partitionBy("sector", "trading_date")
        
        return (df_with_sector
            .withColumn("sector_avg_return", F.avg("daily_return").over(sector_window))
            .withColumn("relative_performance", 
                       F.col("daily_return") - F.col("sector_avg_return"))
        )
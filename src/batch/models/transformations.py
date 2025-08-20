from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import uuid

class BatchTransformer:
    """Class containing batch data transformation methods"""
    
    def __init__(self, spark):
        self.spark = spark
        
    def transform_raw_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to raw batch data
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Basic transformations
        transformed_df = (df
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
        
        # Calculate daily returns
        window_spec = Window.partitionBy("stock_id").orderBy("trading_date")
        transformed_df = transformed_df.withColumn(
            "daily_return",
            (F.col("close_price") / F.lag("close_price").over(window_spec) - 1) * 100
        )
        
        # Calculate technical indicators
        return self._calculate_technical_indicators(transformed_df)
    
    def _calculate_technical_indicators(self, df: DataFrame) -> DataFrame:
        """Calculate technical indicators for the stock data"""
        window_5 = Window.partitionBy("stock_id").orderBy("trading_date").rowsBetween(-4, 0)
        window_20 = Window.partitionBy("stock_id").orderBy("trading_date").rowsBetween(-19, 0)
        
        # Calculate SMAs
        df = (df
            .withColumn("sma_5", F.avg("close_price").over(window_5))
            .withColumn("sma_20", F.avg("close_price").over(window_20))
        )
        
        # Calculate RSI (14-day)
        return self._calculate_rsi(df, 14)
    
    def _calculate_rsi(self, df: DataFrame, period: int = 14) -> DataFrame:
        """Calculate Relative Strength Index (RSI)"""
        # Calculate price changes
        window = Window.partitionBy("stock_id").orderBy("trading_date")
        delta = F.col("close_price") - F.lag("close_price").over(window)
        
        # Calculate gains and losses
        gains = F.when(delta > 0, delta).otherwise(0)
        losses = F.when(delta < 0, -delta).otherwise(0)
        
        # Calculate average gains and losses
        avg_gain = F.avg(gains).over(window.rowsBetween(-(period-1), 0))
        avg_loss = F.avg(losses).over(window.rowsBetween(-(period-1), 0))
        
        # Calculate RS and RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return df.withColumn(f"rsi_{period}", rsi)

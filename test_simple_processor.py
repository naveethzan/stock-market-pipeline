#!/usr/bin/env python3
"""
Simplified streaming processor to test data flow.
This script focuses on the core issue - reading from Kafka and processing data.
"""

import os
import sys
import time
import logging
from typing import Optional

# Add the src directory to the path so we can import our modules
sys.path.insert(0, '/app/src')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with proper Kafka support."""
    return (SparkSession.builder
            .appName("SimpleStreamProcessor")
            .master("local[2]")
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.jars.ivy", "/home/streaming/.ivy2")
            .getOrCreate())

def test_kafka_read():
    """Test reading from Kafka topics directly."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Starting simple Kafka read test...")
    
    try:
        # Read from stock-quotes-realtime topic
        logger.info("Reading from stock-quotes-realtime topic...")
        quotes_df = (spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "kafka:9092")
                    .option("subscribe", "stock-quotes-realtime")
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load())
        
        # Simple processing - convert value to string and add timestamp
        processed_df = (quotes_df
                       .select(
                           col("key").cast("string").alias("message_key"),
                           col("value").cast("string").alias("message_value"),
                           col("topic"),
                           col("partition"),
                           col("offset"),
                           current_timestamp().alias("processing_time")
                       ))
        
        # Write to console for debugging
        query = (processed_df.writeStream
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .option("numRows", "10")
                .trigger(processingTime="30 seconds")
                .start())
        
        logger.info(f"Started console query: {query.id}")
        
        # Monitor for a few minutes
        for i in range(10):  # 10 iterations x 30 seconds = 5 minutes
            time.sleep(30)
            status = query.lastProgress
            if status:
                batch_id = status.get("batchId", -1)
                input_rate = status.get("inputRowsPerSecond", 0)
                logger.info(f"Iteration {i+1}: batch_id={batch_id}, input_rate={input_rate}")
            else:
                logger.info(f"Iteration {i+1}: No progress yet")
            
            if not query.isActive:
                logger.error("Query stopped unexpectedly")
                if query.exception():
                    logger.error(f"Query exception: {query.exception()}")
                break
        
        query.stop()
        logger.info("Test completed")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    test_kafka_read()
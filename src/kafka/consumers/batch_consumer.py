"""
Batch consumer for processing historical stock market data
"""

import logging
from datetime import datetime
from typing import Dict, Any

from src.kafka.consumers.base_consumer import BaseConsumer
from src.kafka.config import AppConfig

logger = logging.getLogger(__name__)


class BatchDataConsumer(BaseConsumer):
    """
    Consumer for batch stock market data from Yahoo Finance
    Stores data in S3 under raw-data/batch/ folder structure
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize batch consumer
        
        Args:
            config: Application configuration
        """
        super().__init__(config)
        self.data_type = "batch"
        self.s3_folder = "raw-data/batch"
        logger.info("BatchDataConsumer initialized")
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a batch data message and store to S3 as CSV
        
        Args:
            message: Kafka message containing batch stock data
            
        Returns:
            True if processing successful, False otherwise
        """
        try:
            # Extract data from message
            if 'data' not in message:
                logger.error("Message missing 'data' field")
                return False
            
            data = message['data']
            symbol = message.get('symbol', 'UNKNOWN')
            timestamp = message.get('timestamp', datetime.utcnow())
            
            # Convert timestamp string to datetime if needed
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    timestamp = datetime.utcnow()
            
            # Convert data to pandas DataFrame
            import pandas as pd
            from io import StringIO
            
            # Convert the data to a DataFrame
            df = pd.DataFrame(data)
            
            # Convert 'Date' column to datetime if it exists
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            elif 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # Group data by date and process each day's data separately
            success = True
            if 'Date' in df.columns or 'date' in df.columns:
                date_col = 'Date' if 'Date' in df.columns else 'date'
                
                for date, group in df.groupby(date_col):
                    # Create S3 key using the actual date from the record
                    s3_key = self.create_s3_key(
                        symbol=symbol,
                        timestamp=timestamp,  # Processing timestamp
                        data_type=self.data_type,
                        record_date=date.to_pydatetime() if hasattr(date, 'to_pydatetime') else date
                    )
                    
                    # Prepare the data for this date
                    group_df = group.copy()
                    group_df['symbol'] = symbol
                    group_df['data_type'] = self.data_type
                    group_df['source'] = 'yahoo_finance'
                    group_df['processed_at'] = datetime.utcnow().isoformat() + 'Z'
                    
                    # Convert to CSV
                    csv_buffer = StringIO()
                    group_df.to_csv(csv_buffer, index=False)
                    
                    # Store to S3
                    if not self.store_to_s3(csv_buffer.getvalue(), s3_key):
                        logger.error(f"Failed to store batch data for {symbol} on {date} to S3")
                        success = False
                    else:
                        logger.info(f"Stored {len(group_df)} records for {symbol} on {date} to {s3_key}")
            else:
                # If no date column, use the message timestamp
                s3_key = self.create_s3_key(symbol, timestamp, self.data_type)
                
                # Add metadata columns
                df['symbol'] = symbol
                df['data_type'] = self.data_type
                df['source'] = 'yahoo_finance'
                df['processed_at'] = datetime.utcnow().isoformat() + 'Z'
                
                # Convert to CSV
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                
                # Store to S3
                if not self.store_to_s3(csv_buffer.getvalue(), s3_key):
                    logger.error(f"Failed to store batch data for {symbol} to S3")
                    success = False
                else:
                    logger.info(f"Stored {len(df)} records for {symbol} to {s3_key}")
            
            return success
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def consume_batch_data(self, topic: str = None, max_messages: int = None, idle_timeout: int = 30):
        """
        Consume batch data messages from Kafka topic

        Args:
            topic: Kafka topic (defaults to batch topic from config)
            max_messages: Maximum number of messages to consume
            idle_timeout: Idle timeout in seconds
        """
        if topic is None:
            topic = self.config.batch_topic

        logger.info(f"Starting batch data consumption from topic: {topic} with idle timeout: {idle_timeout}s")
        self.consume_messages(topic, max_messages=max_messages, idle_timeout=idle_timeout)


if __name__ == "__main__":
    """Run batch consumer independently."""
    import argparse
    import sys
    import os

    # Add project root to path for imports when running as a script
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

    from src.kafka.config import AppConfig

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Batch Data Consumer for Stock Market Pipeline')
    parser.add_argument('--topic', help='Kafka topic to consume from (uses config default if not specified)')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to consume')
    parser.add_argument('--idle-timeout', type=int, default=60, help='Idle timeout in seconds for consumer shutdown')
    parser.add_argument('--config-file', help='Path to configuration file')

    args = parser.parse_args()

    try:
        # Load configuration
        if args.config_file and os.path.exists(args.config_file):
            from dotenv import load_dotenv
            load_dotenv(args.config_file)

        config = AppConfig.from_env()
        config.validate()

        # Create and run consumer using a context manager
        with BatchDataConsumer(config) as consumer:
            logger.info("Running batch consumer...")
            consumer.consume_batch_data(
                topic=args.topic, 
                max_messages=args.max_messages, 
                idle_timeout=args.idle_timeout
            )

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the batch consumer: {e}", exc_info=True)
        sys.exit(1)

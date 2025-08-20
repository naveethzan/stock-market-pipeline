"""
Stream consumer for processing real-time stock market data
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any

from src.kafka.consumers.base_consumer import BaseConsumer
from src.kafka.config import AppConfig

logger = logging.getLogger(__name__)


class StreamDataConsumer(BaseConsumer):
    """
    Consumer for real-time stock market data from Alpha Vantage
    Stores data in S3 under raw-data/stream/ folder structure
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize stream consumer
        
        Args:
            config: Application configuration
        """
        super().__init__(config)
        self.data_type = "stream"
        self.s3_folder = "raw-data/stream"
        logger.info("StreamDataConsumer initialized")
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a stream data message and store to S3
        
        Args:
            message: Kafka message containing real-time stock data
            
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
            
            # Create S3 key for this data
            s3_key = self.create_s3_key(symbol, timestamp, self.data_type)
            
            # Prepare data for S3 storage
            s3_data = {
                'symbol': symbol,
                'timestamp': timestamp.isoformat(),
                'data_type': self.data_type,
                'source': 'alpha_vantage',
                'data': data,
                'metadata': {
                    'processed_at': datetime.utcnow().isoformat() + 'Z',
                    'consumer': 'StreamDataConsumer',
                    'message_id': message.get('message_id', 'unknown'),
                    'real_time': True
                }
            }
            
            # Store to S3
            json_payload = json.dumps(s3_data)
            if self.store_to_s3(json_payload, s3_key, content_type='application/json'):
                logger.info(f"Stream data for {symbol} stored to S3 successfully")
                return True
            else:
                logger.error(f"Failed to store stream data for {symbol} to S3")
                return False
                
        except Exception as e:
            logger.error(f"Error processing stream message: {e}")
            return False
    
    def consume_stream_data(self, topic: str = None, max_messages: int = None):
        """
        Consume stream data messages from Kafka topic
        
        Args:
            topic: Kafka topic (defaults to stream topic from config)
            max_messages: Maximum number of messages to consume
        """
        if topic is None:
            topic = self.config.stream_topic
        
        logger.info(f"Starting stream data consumption from topic: {topic}")
        self.consume_messages(topic, max_messages)
    
    def continuous_consume(self, topic: str = None):
        """
        Continuously consume stream data messages (for real-time processing)
        
        Args:
            topic: Kafka topic (defaults to stream topic from config)
        """
        if topic is None:
            topic = self.config.stream_topic
        
        logger.info(f"Starting continuous stream data consumption from topic: {topic}")
        self.consume_messages(topic, max_messages=None)  # No limit for continuous consumption


if __name__ == "__main__":
    """Run stream consumer independently."""
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

    parser = argparse.ArgumentParser(description='Stream Data Consumer for Stock Market Pipeline')
    parser.add_argument('--topic', help='Kafka topic to consume from (uses config default if not specified)')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to consume')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode')
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
        with StreamDataConsumer(config) as consumer:
            if args.continuous:
                consumer.continuous_consume(topic=args.topic)
            else:
                consumer.consume_stream_data(topic=args.topic, max_messages=args.max_messages)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the stream consumer: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'consumer' in locals():
            consumer.close()

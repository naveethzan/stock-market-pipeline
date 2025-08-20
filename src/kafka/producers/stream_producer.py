import json
import logging
from typing import Dict, List
from datetime import datetime

from alpha_vantage.timeseries import TimeSeries
from confluent_kafka import Producer as KafkaProducer

from src.kafka.config import AppConfig, AlphaVantageConfig

logger = logging.getLogger(__name__)


class StreamDataProducer:
    """
    Kafka producer for real-time stock market data from Alpha Vantage
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize the stream data producer
        
        Args:
            config: Application configuration
        """
        self.config = config.kafka
        self.alpha_vantage_config = config.alpha_vantage
        
        # Initialize Kafka producer (confluent-kafka)
        self.producer = KafkaProducer({
            'bootstrap.servers': self.config.bootstrap_servers,
            # Optional tuning:
            # 'linger.ms': 50,
        })
        
        # Initialize Alpha Vantage client if API key provided
        if self.alpha_vantage_config.api_key:
            self.alpha_vantage = TimeSeries(
                key=self.alpha_vantage_config.api_key, 
                output_format=self.alpha_vantage_config.output_format
            )
            logger.info("StreamDataProducer initialized with Alpha Vantage")
        else:
            self.alpha_vantage = None
            logger.warning("StreamDataProducer initialized without Alpha Vantage API key")
    
    def send_to_kafka(self, topic: str, data: Dict, key: str = None):
        """
        Send data to Kafka topic
        
        Args:
            topic: Kafka topic name
            data: Data to send
            key: Optional message key
        """
        try:
            value_bytes = json.dumps(data).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            self.producer.produce(topic=topic, value=value_bytes, key=key_bytes, callback=self._delivery_report)
            # Serve delivery callbacks
            self.producer.poll(0)
            logger.info(f"Queued message to topic={topic} key={key}")
        except BufferError as e:
            logger.error(f"Local producer queue is full: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise

    def _delivery_report(self, err, msg):
        """Delivery callback to log the delivery result."""
        if err is not None:
            logger.error(f"Delivery failed for key={msg.key()} to {msg.topic()} [{msg.partition()}]: {err}")
        else:
            logger.info(
                f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} key={msg.key()}"
            )
    
    def get_alpha_vantage_data(self, symbols: List[str]) -> Dict:
        """
        Fetch real-time data from Alpha Vantage (Stream processing)
        
        Args:
            symbols: List of stock symbols
        
        Returns:
            Dictionary containing real-time stock data
        """
        if not self.alpha_vantage:
            logger.error("Alpha Vantage client not initialized - API key required")
            return {}
        
        data = {}
        
        for symbol in symbols:
            try:
                # Get real-time quote
                quote, meta_data = self.alpha_vantage.get_quote_endpoint(symbol)
                
                if not quote.empty:
                    stock_data = {
                        'symbol': symbol,
                        'source': 'alpha_vantage',
                        'timestamp': datetime.now().isoformat(),
                        'data_type': 'stream',
                        'data': quote.to_dict('records')[0]
                    }
                    
                    data[symbol] = stock_data
                    logger.info(f"Fetched real-time data for {symbol}")
                else:
                    logger.warning(f"No real-time data found for symbol: {symbol}")
                    
            except Exception as e:
                logger.error(f"Error fetching Alpha Vantage data for {symbol}: {e}")
        
        return data
    
    def produce_stream_data(self, symbols: List[str], topic: str = None):
        """
        Produce real-time data from Alpha Vantage to Kafka
        
        Args:
            symbols: List of stock symbols
            topic: Kafka topic for stream data (uses config default if None)
        """
        if not self.alpha_vantage:
            logger.error("Cannot produce stream data without Alpha Vantage API key")
            return
        
        if topic is None:
            topic = self.config.stream_topic
            
        logger.info(f"Starting stream data production for {len(symbols)} symbols")
        
        data = self.get_alpha_vantage_data(symbols)
        
        for symbol, stock_data in data.items():
            self.send_to_kafka(topic, stock_data, key=symbol)
        
        logger.info(f"Stream data production completed for {len(data)} symbols")
    
    def close(self):
        """Close the Kafka producer"""
        # Ensure all in-flight messages are delivered
        timeout = float(self.config.producer_timeout)
        remaining = self.producer.flush(timeout)
        if remaining:
            logger.warning(f"Flush timed out with {remaining} message(s) pending")
        logger.info("Kafka producer closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def continuous_stream_data(self, symbols: List[str], interval_seconds: int = 60, topic: str = None):
        """
        Continuously produce stream data at specified intervals
        
        Args:
            symbols: List of stock symbols
            interval_seconds: Interval between data fetches in seconds
            topic: Kafka topic for stream data
        """
        if not self.alpha_vantage:
            logger.error("Cannot produce stream data without Alpha Vantage API key")
            return
        
        if topic is None:
            topic = self.config.stream_topic
            
        logger.info(f"Starting continuous stream data production for {len(symbols)} symbols every {interval_seconds} seconds")
        
        try:
            while True:
                self.produce_stream_data(symbols, topic)
                logger.info(f"Waiting {interval_seconds} seconds before next fetch...")
                import time
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Continuous stream data production stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous stream data production: {e}")
            raise

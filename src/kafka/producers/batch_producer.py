import json
import logging
import sys
import time
import argparse
from typing import Dict, List
from datetime import datetime, timedelta

import yfinance as yf
from confluent_kafka import Producer as KafkaProducer

# Absolute imports from src package
from src.kafka.config import AppConfig

logger = logging.getLogger(__name__)


class BatchDataProducer:
    """
    Kafka producer for batch stock market data from Yahoo Finance.
    This class is designed to be used as a context manager to ensure
    the Kafka producer is properly closed.
    """

    def __init__(self, config: AppConfig):
        """
        Initialize the batch data producer.
        
        Args:
            config: Application configuration.
        """
        self.config = config.kafka
        self.yahoo_config = config.yahoo_finance
        self.producer = None  # Initialize producer to None

    def connect(self):
        """Connect to Kafka."""
        if not self.producer:
            self.producer = KafkaProducer({
                'bootstrap.servers': self.config.bootstrap_servers,
                'compression.type': 'gzip',
                'linger.ms': 50,
            })
            logger.info(f"Confluent Kafka producer connected to {self.config.bootstrap_servers}")

    def close(self):
        """Close the Kafka producer connection."""
        if self.producer:
            # Ensure all messages are delivered
            timeout = float(self.config.producer_timeout)
            remaining = self.producer.flush(timeout)
            if remaining:
                logger.warning(f"Flush timed out with {remaining} message(s) pending")
            logger.info("Kafka producer closed.")
            self.producer = None

    def __enter__(self):
        """Enter the context manager, connecting the producer."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager, closing the producer."""
        self.close()

    def send_to_kafka(self, topic: str, data: Dict, key: str = None):
        """
        Send data to a Kafka topic.
        
        Args:
            topic: Kafka topic name.
            data: Data to send.
            key: Optional message key.
        """
        if not self.producer:
            raise RuntimeError("Producer is not connected. Call connect() or use a context manager.")
        value_bytes = json.dumps(data).encode('utf-8')
        key_bytes = key.encode('utf-8') if key else None
        attempts = 3
        delay = 0.5
        for attempt in range(attempts):
            try:
                self.producer.produce(
                    topic=topic,
                    value=value_bytes,
                    key=key_bytes,
                    callback=self._delivery_report
                )
                # Serve delivery callbacks for previously produced messages
                self.producer.poll(0)
                logger.info(f"Queued message to topic={topic} key={key}")
                return
            except BufferError as e:
                if attempt < attempts - 1:
                    logger.warning(f"Producer queue full on attempt {attempt + 1}: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"Local producer queue is full after {attempts} attempts: {e}")
                    raise
            except Exception as e:
                if attempt < attempts - 1:
                    logger.warning(f"Produce failed on attempt {attempt + 1}: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"Failed to send message to Kafka after {attempts} attempts: {e}")
                    raise

    def _delivery_report(self, err, msg):
        """Delivery callback to log the delivery result."""
        if err is not None:
            logger.error(f"Delivery failed for key={msg.key()} to {msg.topic()} [{msg.partition()}]: {err}")
        else:
            logger.info(
                f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()} key={msg.key()}"
            )

    def get_yahoo_finance_data(self, symbols: List[str], start_date: str, end_date: str) -> Dict:
        """
        Fetch historical data from Yahoo Finance for a date range.
        
        Args:
            symbols: List of stock symbols.
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
        
        Returns:
            A dictionary containing stock data, keyed by symbol.
        """
        data = {}
        for symbol in symbols:
            attempts = 3
            delay = 0.5
            for attempt in range(attempts):
                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(start=start_date, end=end_date)
                    if not hist.empty:
                        records = hist.reset_index().to_dict('records')
                        for record in records:
                            for k, v in record.items():
                                if hasattr(v, 'isoformat'):
                                    record[k] = v.isoformat()
                        stock_data = {
                            'symbol': symbol,
                            'source': 'yahoo_finance',
                            'timestamp': datetime.utcnow().isoformat() + 'Z',
                            'data_type': 'batch',
                            'start_date': start_date,
                            'end_date': end_date,
                            'data': records
                        }
                        data[symbol] = stock_data
                        logger.info(f"Fetched {len(hist)} records for {symbol}")
                    else:
                        logger.warning(f"No data found for {symbol} from {start_date} to {end_date}")
                    break
                except Exception as e:
                    if attempt < attempts - 1:
                        logger.warning(f"Fetch failed for {symbol} on attempt {attempt + 1}: {e}. Retrying in {delay}s...")
                        time.sleep(delay)
                        delay *= 2
                    else:
                        logger.error(f"Error fetching data for {symbol} after {attempts} attempts: {e}")
        return data

    STOCKS = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "META",
        "TSLA", "NVDA", "INTC", "JPM", "V"
    ]

    def produce_batch_data(self, end_date_str: str, symbols: List[str] = None, topic: str = None):
        """
        Produce batch data from Yahoo Finance to Kafka for a specific date range.

        Args:
            end_date_str: The end date for the batch period (YYYY-MM-DD).
            symbols: Optional list of stock symbols.
            topic: Kafka topic name.
        """
        topic = topic or self.config.batch_topic
        symbols = symbols or self.STOCKS
        
        logger.info(f"Starting batch data production for {len(symbols)} symbols.")
        
        # yfinance 'end' parameter is EXCLUSIVE. To include end_date, add one day.
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')
        lookback_days = int(self.yahoo_config.lookback_days)
        # Inclusive window: start <= date <= end_dt
        # Convert to yfinance parameters: start=start_dt, end=(end_dt + 1 day)
        start_dt = end_dt - timedelta(days=max(lookback_days - 1, 0))
        end_exclusive_dt = end_dt + timedelta(days=1)
        start_date_str = start_dt.strftime('%Y-%m-%d')
        end_exclusive_str = end_exclusive_dt.strftime('%Y-%m-%d')

        logger.info(
            f"Fetching data (inclusive) from {start_date_str} to {end_date_str} (yfinance end-exclusive={end_exclusive_str})"
        )

        data = self.get_yahoo_finance_data(
            symbols,
            start_date=start_date_str,
            end_date=end_exclusive_str
        )
        
        produced_count = 0
        failed_count = 0
        for symbol, stock_data in data.items():
            try:
                self.send_to_kafka(topic=topic, key=symbol, data=stock_data)
                logger.info(f"Sent batch data for {symbol}")
                produced_count += 1
            except Exception as e:
                logger.error(f"Failed to send batch data for {symbol}: {e}")
                failed_count += 1

        logger.info(
            f"Batch production summary: symbols={len(symbols)}, with_data={len(data)}, produced={produced_count}, failed={failed_count}"
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka Batch Producer for Yahoo Finance Data.')
    parser.add_argument('--date', type=str, required=True, help='The end date for batch processing (YYYY-MM-DD).')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    try:
        config = AppConfig.from_env()
        config.validate()
        with BatchDataProducer(config) as producer:
            producer.produce_batch_data(end_date_str=args.date)
    except Exception as e:
        logger.error(f"An error occurred in the batch producer script: {e}", exc_info=True)
        sys.exit(1)

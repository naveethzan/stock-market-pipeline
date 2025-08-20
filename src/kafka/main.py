#!/usr/bin/env python3
"""
Main entry point for Kafka Stock Market Data Pipeline
Single script for both development and production use
"""

import argparse
import logging
import sys
import os
import signal
import time
from datetime import datetime
from typing import List

from src.kafka.config import AppConfig
from src.kafka.producers import BatchDataProducer, StreamDataProducer
from src.kafka.consumers import BatchDataConsumer, StreamDataConsumer

# Configure logging
def setup_logging(mode: str):
    """Setup logging based on mode"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if mode == "production":
        # Production: File and console logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{mode}_pipeline_{timestamp}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    else:
        # Development: Console only
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

logger = logging.getLogger(__name__)


class StreamPipeline:
    """Stream pipeline with graceful shutdown and monitoring"""
    
    def __init__(self, config: AppConfig, symbols: List[str], topic: str = None, 
                 interval: int = 60):
        self.config = config
        self.symbols = symbols
        self.topic = topic
        self.interval = interval
        self.running = False
        self.producer = None
        self.stats = {
            'start_time': None,
            'cycles': 0,
            'successful_fetches': 0,
            'failed_fetches': 0,
            'last_success': None,
            'last_error': None
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.stop()
    
    def start(self):
        """Start the stream pipeline"""
        if not self.config.alpha_vantage.api_key:
            logger.error("❌ Alpha Vantage API key not found!")
            logger.info("Please set ALPHA_VANTAGE_API_KEY environment variable")
            return False
        
        logger.info("=== Starting Stream Data Pipeline ===")
        logger.info(f"Symbols: {self.symbols}")
        logger.info(f"Topic: {self.topic or 'default'}")
        logger.info(f"Interval: {self.interval} seconds")
        logger.info("Press Ctrl+C to stop...")
        
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        try:
            self.producer = StreamDataProducer(config)
            
            while self.running:
                cycle_start = time.time()
                
                try:
                    # Fetch and produce stream data
                    self.producer.produce_stream_data(self.symbols, self.topic)
                    self.stats['successful_fetches'] += 1
                    self.stats['last_success'] = datetime.now()
                    self.stats['cycles'] += 1
                    
                    logger.info(f"✅ Cycle {self.stats['cycles']} completed successfully")
                    
                except Exception as e:
                    self.stats['failed_fetches'] += 1
                    self.stats['last_error'] = datetime.now()
                    logger.error(f"❌ Cycle {self.stats['cycles'] + 1} failed: {e}")
                
                # Calculate sleep time to maintain consistent intervals
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, self.interval - cycle_duration)
                
                if self.running:
                    logger.info(f"Waiting {sleep_time:.1f} seconds until next cycle...")
                    time.sleep(sleep_time)
                    
        except Exception as e:
            logger.error(f"❌ Stream pipeline failed: {e}")
            return False
        finally:
            self.stop()
        
        return True
    
    def stop(self):
        """Stop the stream pipeline"""
        logger.info("Stopping stream pipeline...")
        self.running = False
        
        if self.producer:
            self.producer.close()
        
        # Print final statistics
        self._print_stats()
    
    def _print_stats(self):
        """Print pipeline statistics"""
        if self.stats['start_time']:
            duration = datetime.now() - self.stats['start_time']
            logger.info("\n=== Pipeline Statistics ===")
            logger.info(f"Total runtime: {duration}")
            logger.info(f"Total cycles: {self.stats['cycles']}")
            logger.info(f"Successful fetches: {self.stats['successful_fetches']}")
            logger.info(f"Failed fetches: {self.stats['failed_fetches']}")
            logger.info(f"Success rate: {(self.stats['successful_fetches'] / max(1, self.stats['cycles'])) * 100:.1f}%")
            
            if self.stats['last_success']:
                logger.info(f"Last success: {self.stats['last_success']}")
            if self.stats['last_error']:
                logger.info(f"Last error: {self.stats['last_error']}")


def run_batch_pipeline(symbols: List[str], period: str = "1d", topic: str = None, 
                       retry_count: int = 3, retry_delay: int = 60):
    """Run batch pipeline with retry logic"""
    
    logger.info("=== Starting Batch Data Pipeline ===")
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Period: {period}")
    logger.info(f"Topic: {topic or 'default'}")
    logger.info(f"Retry attempts: {retry_count}")
    
    config = AppConfig.from_env()
    
    for attempt in range(retry_count):
        try:
            with BatchDataProducer(config) as producer:
                if topic:
                    producer.produce_batch_data_with_period(symbols, period, topic)
                else:
                    producer.produce_batch_data_with_period(symbols, period)
                
                logger.info("✅ Batch pipeline completed successfully!")
                return True
                
        except Exception as e:
            logger.error(f"❌ Attempt {attempt + 1} failed: {e}")
            
            if attempt < retry_count - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ All retry attempts failed. Pipeline failed.")
                return False
    
    return False


def run_stream_pipeline(symbols: List[str], topic: str = None, interval: int = 60):
    """Run stream pipeline"""
    config = AppConfig.from_env()
    
    pipeline = StreamPipeline(config, symbols, topic, interval)
    return pipeline.start()


def run_batch_consumer_pipeline(topic: str = None, max_messages: int = None, continuous: bool = False):
    """Run batch consumer pipeline"""
    config = AppConfig.from_env()
    
    logger.info("=== Starting Batch Consumer Pipeline ===")
    logger.info(f"Topic: {topic or 'default'}")
    logger.info(f"Max messages: {max_messages or 'unlimited'}")
    logger.info(f"Continuous: {continuous}")
    
    try:
        with BatchDataConsumer(config) as consumer:
            if continuous:
                consumer.consume_batch_data(topic, max_messages)
            else:
                consumer.consume_batch_data(topic, max_messages)
        
        logger.info("✅ Batch consumer pipeline completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Batch consumer pipeline failed: {e}")
        return False


def run_stream_consumer_pipeline(topic: str = None, max_messages: int = None, continuous: bool = False):
    """Run stream consumer pipeline"""
    config = AppConfig.from_env()
    
    logger.info("=== Starting Stream Consumer Pipeline ===")
    logger.info(f"Topic: {topic or 'default'}")
    logger.info(f"Max messages: {max_messages or 'unlimited'}")
    logger.info(f"Continuous: {continuous}")
    
    try:
        with StreamDataConsumer(config) as consumer:
            if continuous:
                consumer.continuous_consume(topic)
            else:
                consumer.consume_stream_data(topic, max_messages)
        
        logger.info("✅ Stream consumer pipeline completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Stream consumer pipeline failed: {e}")
        return False


def run_complete_pipeline(symbols: List[str], period: str = "1d", topic: str = None, 
                          interval: int = 60, max_messages: int = None, continuous: bool = False):
    """Run complete pipeline: produce data and consume it"""
    logger.info("=== Starting Complete Pipeline (Produce + Consume) ===")
    
    # First, produce data
    logger.info("Phase 1: Producing data to Kafka...")
    batch_success = run_batch_pipeline(symbols, period, topic)
    
    if not batch_success:
        logger.error("❌ Batch production failed, cannot proceed with consumption")
        return False
    
    # Wait a bit for data to be available
    logger.info("Waiting 5 seconds for data to be available in Kafka...")
    time.sleep(5)
    
    # Then, consume data
    logger.info("Phase 2: Consuming data from Kafka...")
    consumer_success = run_batch_consumer_pipeline(topic, max_messages, continuous)
    
    if consumer_success:
        logger.info("🎉 Complete pipeline executed successfully!")
        logger.info("Data flowed from Producer → Kafka → Consumer → S3")
    else:
        logger.error("❌ Complete pipeline failed at consumption phase")
    
    return consumer_success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Kafka Stock Market Data Pipeline')
    parser.add_argument('--mode', choices=['batch', 'stream', 'batch-consumer', 'stream-consumer', 'complete'], required=True,
                       help='Mode to run: batch, stream, batch-consumer, stream-consumer, or complete')
    parser.add_argument('--symbols', nargs='+', default=['AAPL', 'GOOGL', 'MSFT'],
                       help='Stock symbols to process')
    parser.add_argument('--period', default='1d',
                       help='Time period for batch data (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)')
    parser.add_argument('--topic', help='Custom Kafka topic (uses config default if not specified)')
    parser.add_argument('--interval', type=int, default=60,
                       help='Interval in seconds for continuous streaming')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to consume')
    parser.add_argument('--continuous', action='store_true',
                       help='Run consumer in continuous mode')
    parser.add_argument('--retry', type=int, default=3,
                       help='Number of retry attempts for batch pipeline (default: 3)')
    parser.add_argument('--retry-delay', type=int, default=60,
                       help='Delay between retries in seconds (default: 60)')
    parser.add_argument('--production', action='store_true',
                       help='Run in production mode with file logging')
    parser.add_argument('--config-file',
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Setup logging
    mode = "production" if args.production else "development"
    setup_logging(mode)
    
    # Set environment variables if config file provided
    if args.config_file and os.path.exists(args.config_file):
        from dotenv import load_dotenv
        load_dotenv(args.config_file)
        logger.info(f"Loaded configuration from {args.config_file}")
    
    logger.info(f"Starting pipeline in {args.mode} mode ({mode})")
    logger.info(f"Symbols: {args.symbols}")
    
    try:
        if args.mode == 'batch':
            success = run_batch_pipeline(
                args.symbols, 
                args.period, 
                args.topic, 
                args.retry, 
                args.retry_delay
            )
            
        elif args.mode == 'stream':
            success = run_stream_pipeline(args.symbols, args.topic, args.interval)
            

                
        elif args.mode == 'batch-consumer':
            success = run_batch_consumer_pipeline(args.topic, args.max_messages, args.continuous)
            
        elif args.mode == 'stream-consumer':
            success = run_stream_consumer_pipeline(args.topic, args.max_messages, args.continuous)
            
        elif args.mode == 'complete':
            success = run_complete_pipeline(
                args.symbols, 
                args.period, 
                args.topic, 
                args.interval, 
                args.max_messages, 
                args.continuous
            )
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        success = False
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

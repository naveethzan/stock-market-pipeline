#!/usr/bin/env python3
"""
Spark Structured Streaming Processor Entry Point

Simplified version focusing on core streaming functionality.
"""
import logging
import signal
import sys
from typing import Optional


# Configure paths for imports
sys.path.insert(0, '/app/src')
from streaming_pipeline.config.loader import initialize_configuration
from streaming_pipeline.processors.stream_processor import StreamProcessor


# Setup logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# Global processor for signal handling
processor: Optional[StreamProcessor] = None


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating graceful shutdown")
    if processor:
        processor.close()
    sys.exit(0)


def main():
    """Main entry point for the Spark streaming processor."""
    global processor
    
    logger.info("Starting Spark Structured Streaming Processor")
    
    try:
        # Load configuration
        config = initialize_configuration()
        logger.info("Configuration loaded successfully")
        
        # Initialize processor
        processor = StreamProcessor(config)
        logger.info("Stream processor initialized")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start streaming queries
        output_base_path = config.get_output_base_path()
        quotes_query = processor.process_stock_quotes_stream(output_base_path)
        
        logger.info(f"Stock quotes streaming query started: {quotes_query.id}")
        logger.info("Streaming processor running. Press Ctrl+C to stop.")
        
        # Wait for termination
        quotes_query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Cleaning up resources")
        if processor:
            processor.close()
        logger.info("Spark Streaming Processor shutdown complete")


if __name__ == "__main__":
    main()
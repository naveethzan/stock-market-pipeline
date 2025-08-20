"""
Example usage of the Spark Structured Streaming processor.
Demonstrates how to set up and run the streaming pipeline.
"""
import logging
import time
import signal
import sys
from pathlib import Path

from ..config.settings import ConfigManager
from .stream_processor import StreamProcessor


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamingPipelineRunner:
    """
    Runner class for the streaming pipeline with graceful shutdown handling.
    """
    
    def __init__(self):
        self.config = ConfigManager()
        self.processor = None
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def run(self, output_path: str = "/tmp/streaming-output"):
        """
        Run the streaming pipeline.
        
        Args:
            output_path: Base path for output files
        """
        logger.info("Starting streaming pipeline")
        
        try:
            # Initialize stream processor
            self.processor = StreamProcessor(self.config)
            
            # Start stock quotes processing
            query = self.processor.process_stock_quotes_stream(output_path)
            
            self.running = True
            logger.info("Streaming pipeline started successfully")
            
            # Monitor the streaming query
            while self.running and query.isActive:
                try:
                    # Wait for a bit
                    time.sleep(10)
                    
                    # Log query status
                    status = self.processor.get_query_status("stock_quotes")
                    logger.info(f"Query status: {status}")
                    
                    # Check for exceptions
                    if query.exception():
                        logger.error(f"Query exception: {query.exception()}")
                        break
                        
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt")
                    break
                except Exception as e:
                    logger.error(f"Error monitoring query: {str(e)}")
                    break
            
            logger.info("Stopping streaming pipeline")
            
        except Exception as e:
            logger.error(f"Error running streaming pipeline: {str(e)}", exc_info=True)
            
        finally:
            # Clean up
            if self.processor:
                self.processor.close()
            logger.info("Streaming pipeline stopped")


def main():
    """Main entry point."""
    # Check if output path is provided
    output_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/streaming-output"
    
    logger.info(f"Starting streaming pipeline with output path: {output_path}")
    
    # Create output directory
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    # Run the pipeline
    runner = StreamingPipelineRunner()
    runner.run(output_path)


if __name__ == "__main__":
    main()
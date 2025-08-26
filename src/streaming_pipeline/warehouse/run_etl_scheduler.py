#!/usr/bin/env python3
"""
ETL Scheduler CLI Runner

Command-line interface for running the ETL scheduler with various options.
This script provides easy access to scheduler functionality for production use.
"""

import sys
import json
import time
import signal
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from .etl_scheduler import create_etl_scheduler

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLSchedulerCLI:
    """Command-line interface for ETL scheduler operations"""
    
    def __init__(self):
        self.scheduler = None
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        if self.scheduler and self.scheduler.is_running:
            self.scheduler.stop_monitoring()
        sys.exit(0)
    
    def load_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from file or use defaults"""
        if config_path:
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                logger.info(f"Loaded configuration from {config_path}")
                return config
            except Exception as e:
                logger.error(f"Failed to load config from {config_path}: {e}")
                sys.exit(1)
        else:
            # Default configuration
            return {
                "schema": "STREAMING",
                "polling_interval_seconds": 300,  # 5 minutes
                "min_records_threshold": 1,
                "max_polling_errors": 5,
                "incremental_lookback_minutes": 60,
                "metadata_file": ".etl_scheduler_metadata.json",
                "enable_continuous_monitoring": True,
                "etl_timeout_minutes": 30
            }
    
    def start_monitoring(self, config: Dict[str, Any]) -> None:
        """Start continuous ETL monitoring"""
        logger.info("Starting ETL scheduler in continuous monitoring mode")
        
        try:
            self.scheduler = create_etl_scheduler(config)
            self.scheduler.start_monitoring()
            self.running = True
            
            logger.info(f"ETL scheduler started with {config['polling_interval_seconds']}s polling interval")
            logger.info("Press Ctrl+C to stop monitoring")
            
            # Keep main thread alive
            while self.running and self.scheduler.is_running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in monitoring mode: {e}")
            sys.exit(1)
        finally:
            if self.scheduler and self.scheduler.is_running:
                logger.info("Stopping ETL scheduler...")
                self.scheduler.stop_monitoring()
                logger.info("ETL scheduler stopped")
    
    def run_single_check(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single ETL check"""
        logger.info("Running single ETL check")
        
        try:
            self.scheduler = create_etl_scheduler(config)
            result = self.scheduler.run_single_etl_check()
            
            # Display results
            logger.info("ETL Check Results:")
            logger.info(f"  Check timestamp: {result['check_timestamp']}")
            logger.info(f"  Should trigger ETL: {result['should_trigger_etl']}")
            logger.info(f"  ETL triggered: {result['etl_triggered']}")
            
            # Show staging table status
            if 'staging_status' in result:
                logger.info("  Staging Table Status:")
                for table_type, status in result['staging_status'].items():
                    logger.info(f"    {status['table_name']}: {status['new_records_since_last_run']} new records")
            
            # Show ETL results if triggered
            if result['etl_triggered'] and 'etl_result' in result:
                etl_result = result['etl_result']
                logger.info(f"  ETL Result: {etl_result['status']}")
                if etl_result['status'] == 'success':
                    logger.info(f"  Records processed: {etl_result.get('records_processed', 0)}")
                elif etl_result['status'] == 'error':
                    logger.error(f"  ETL Error: {etl_result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in single check: {e}")
            return {"error": str(e)}
    
    def show_status(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Show scheduler status and metadata"""
        logger.info("Getting ETL scheduler status")
        
        try:
            self.scheduler = create_etl_scheduler(config)
            status = self.scheduler.get_scheduler_status()
            
            logger.info("Scheduler Status:")
            logger.info(f"  Running: {status['is_running']}")
            logger.info(f"  Polling interval: {status['polling_interval_seconds']}s")
            logger.info(f"  Staging tables monitored: {len(status['staging_tables'])}")
            logger.info(f"  Tables: {', '.join(status['staging_tables'])}")
            
            # Show last run metadata if available
            if status['last_run_metadata']:
                last_run = status['last_run_metadata']
                logger.info("Last ETL Run:")
                logger.info(f"  Run ID: {last_run['run_id']}")
                logger.info(f"  Status: {last_run['status']}")
                logger.info(f"  Start time: {last_run['start_time']}")
                logger.info(f"  Records processed: {last_run['records_processed']}")
                
                if last_run.get('last_processed_timestamps'):
                    logger.info("  Last processed timestamps:")
                    for table_type, timestamp in last_run['last_processed_timestamps'].items():
                        logger.info(f"    {table_type}: {timestamp}")
                
                if last_run.get('error_message'):
                    logger.error(f"  Error: {last_run['error_message']}")
            else:
                logger.info("No previous ETL runs found")
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {"error": str(e)}
    
    def reset_metadata(self, config: Dict[str, Any]) -> None:
        """Reset scheduler metadata"""
        logger.info("Resetting ETL scheduler metadata")
        
        try:
            self.scheduler = create_etl_scheduler(config)
            self.scheduler.reset_metadata()
            logger.info("Metadata reset completed successfully")
            
        except Exception as e:
            logger.error(f"Error resetting metadata: {e}")
            sys.exit(1)
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate scheduler configuration"""
        logger.info("Validating scheduler configuration")
        
        required_fields = [
            'schema', 'polling_interval_seconds', 'min_records_threshold',
            'max_polling_errors', 'incremental_lookback_minutes'
        ]
        
        for field in required_fields:
            if field not in config:
                logger.error(f"Missing required configuration field: {field}")
                return False
        
        # Validate numeric fields
        numeric_fields = {
            'polling_interval_seconds': (1, 3600),  # 1 second to 1 hour
            'min_records_threshold': (0, 10000),
            'max_polling_errors': (1, 100),
            'incremental_lookback_minutes': (1, 1440),  # 1 minute to 1 day
            'etl_timeout_minutes': (1, 120)  # 1 minute to 2 hours
        }
        
        for field, (min_val, max_val) in numeric_fields.items():
            if field in config:
                value = config[field]
                if not isinstance(value, (int, float)) or value < min_val or value > max_val:
                    logger.error(f"Invalid value for {field}: {value} (must be between {min_val} and {max_val})")
                    return False
        
        logger.info("Configuration validation passed")
        return True
    
    def create_sample_config(self, output_path: str) -> None:
        """Create a sample configuration file"""
        sample_config = {
            "schema": "STREAMING",
            "polling_interval_seconds": 300,
            "min_records_threshold": 1,
            "max_polling_errors": 5,
            "incremental_lookback_minutes": 60,
            "metadata_file": ".etl_scheduler_metadata.json",
            "enable_continuous_monitoring": True,
            "etl_timeout_minutes": 30,
            "_comments": {
                "schema": "Snowflake schema containing staging tables",
                "polling_interval_seconds": "How often to check for new data (seconds)",
                "min_records_threshold": "Minimum new records to trigger ETL",
                "max_polling_errors": "Max consecutive errors before stopping",
                "incremental_lookback_minutes": "Default lookback if no previous run",
                "metadata_file": "File to store scheduler metadata",
                "enable_continuous_monitoring": "Enable continuous monitoring mode",
                "etl_timeout_minutes": "Timeout for ETL operations"
            }
        }
        
        try:
            with open(output_path, 'w') as f:
                json.dump(sample_config, f, indent=2)
            logger.info(f"Sample configuration created at {output_path}")
        except Exception as e:
            logger.error(f"Failed to create sample config: {e}")
            sys.exit(1)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='ETL Scheduler CLI - Automated dimensional modeling pipeline scheduler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s start                          # Start continuous monitoring with defaults
  %(prog)s start --config config.json    # Start with custom configuration
  %(prog)s check                          # Run single ETL check
  %(prog)s status                         # Show scheduler status
  %(prog)s reset                          # Reset scheduler metadata
  %(prog)s create-config config.json     # Create sample configuration file
        """
    )
    
    parser.add_argument(
        'command',
        choices=['start', 'check', 'status', 'reset', 'create-config'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to configuration file (JSON format)'
    )
    
    parser.add_argument(
        '--polling-interval', '-p',
        type=int,
        default=300,
        help='Polling interval in seconds (default: 300)'
    )
    
    parser.add_argument(
        '--min-records', '-m',
        type=int,
        default=1,
        help='Minimum records to trigger ETL (default: 1)'
    )
    
    parser.add_argument(
        '--schema', '-s',
        type=str,
        default='STREAMING',
        help='Snowflake schema name (default: STREAMING)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output file path (for create-config command)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create CLI instance
    cli = ETLSchedulerCLI()
    
    try:
        if args.command == 'create-config':
            output_path = args.output or 'etl_scheduler_config.json'
            cli.create_sample_config(output_path)
            return
        
        # Load configuration
        if args.config:
            config = cli.load_config(args.config)
        else:
            # Use command-line arguments to override defaults
            config = cli.load_config(None)
            config['polling_interval_seconds'] = args.polling_interval
            config['min_records_threshold'] = args.min_records
            config['schema'] = args.schema
        
        # Validate configuration
        if not cli.validate_config(config):
            logger.error("Configuration validation failed")
            sys.exit(1)
        
        # Execute command
        if args.command == 'start':
            cli.start_monitoring(config)
        elif args.command == 'check':
            result = cli.run_single_check(config)
            if args.verbose:
                print(json.dumps(result, indent=2, default=str))
        elif args.command == 'status':
            status = cli.show_status(config)
            if args.verbose:
                print(json.dumps(status, indent=2, default=str))
        elif args.command == 'reset':
            cli.reset_metadata(config)
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
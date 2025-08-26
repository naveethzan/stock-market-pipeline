"""
ETL Scheduler for Automated Dimensional Modeling Pipeline

This module provides automated ETL scheduling and triggering functionality for the
dimensional modeling pipeline. It monitors staging tables for new data, implements
incremental processing, and triggers ETL runs when new data arrives.
"""

import logging
import time
import threading
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
import json
import os
from pathlib import Path

from .snowflake_client import SnowflakeClient
from .snowflake_dimensional_etl import SnowflakeDimensionalETL
from ..config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class ETLRunMetadata:
    """Metadata for ETL run tracking"""
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    records_processed: int = 0
    staging_tables_checked: List[str] = None
    last_processed_timestamps: Dict[str, datetime] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        if self.staging_tables_checked is None:
            self.staging_tables_checked = []
        if self.last_processed_timestamps is None:
            self.last_processed_timestamps = {}


@dataclass
class StagingTableStatus:
    """Status information for a staging table"""
    table_name: str
    total_records: int
    new_records_since_last_run: int
    last_processed_timestamp: Optional[datetime]
    latest_record_timestamp: Optional[datetime]
    has_new_data: bool = False


class ETLScheduler:
    """
    Automated ETL scheduler that monitors staging tables and triggers ETL runs.
    
    This class implements:
    - Monitoring of staging tables for new data
    - Incremental processing based on timestamps
    - Automated ETL triggering when new data arrives
    - Tracking of last processed timestamps
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize ETL scheduler
        
        Args:
            config: Optional configuration dict
        """
        self.settings = get_settings()
        self.config = config or self._get_scheduler_config()
        
        # Initialize components
        self.snowflake_client = SnowflakeClient()
        self.etl_orchestrator = SnowflakeDimensionalETL()
        
        # Staging table configuration
        self.staging_tables = {
            'stock_prices': 'FACT_STOCK_PRICES_STAGING',
            'trading_volume': 'FACT_TRADING_VOLUME_STAGING',
            'technical_indicators': 'TECHNICAL_INDICATORS_STAGING'
        }
        
        # Scheduler state
        self.is_running = False
        self.scheduler_thread = None
        self.last_run_metadata = None
        
        # Metadata persistence
        self.metadata_file = Path(self.config.get('metadata_file', '.etl_scheduler_metadata.json'))
        
        self.logger = logging.getLogger(__name__)
        
        # Load previous run metadata
        self._load_metadata()
    
    def _get_scheduler_config(self) -> Dict[str, Any]:
        """Get scheduler configuration"""
        return {
            "schema": self.settings.snowflake_schema or "STREAMING",
            "polling_interval_seconds": 300,  # 5 minutes
            "min_records_threshold": 1,  # Minimum new records to trigger ETL
            "max_polling_errors": 5,  # Max consecutive polling errors before stopping
            "incremental_lookback_minutes": 60,  # Default lookback for incremental processing
            "metadata_file": ".etl_scheduler_metadata.json",
            "enable_continuous_monitoring": True,
            "etl_timeout_minutes": 30
        }
    
    def start_monitoring(self) -> None:
        """
        Start continuous monitoring of staging tables for new data.
        
        This method starts a background thread that periodically checks staging
        tables and triggers ETL runs when new data is detected.
        """
        if self.is_running:
            self.logger.warning("Scheduler is already running")
            return
        
        self.logger.info("Starting ETL scheduler monitoring")
        self.is_running = True
        
        # Start monitoring thread
        self.scheduler_thread = threading.Thread(
            target=self._monitoring_loop,
            name="ETLSchedulerThread",
            daemon=True
        )
        self.scheduler_thread.start()
        
        self.logger.info(f"ETL scheduler started with {self.config['polling_interval_seconds']}s polling interval")
    
    def stop_monitoring(self) -> None:
        """Stop continuous monitoring"""
        if not self.is_running:
            self.logger.warning("Scheduler is not running")
            return
        
        self.logger.info("Stopping ETL scheduler monitoring")
        self.is_running = False
        
        # Wait for thread to finish
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=10)
        
        self.logger.info("ETL scheduler stopped")
    
    def _monitoring_loop(self) -> None:
        """Main monitoring loop that runs in background thread"""
        consecutive_errors = 0
        max_errors = self.config['max_polling_errors']
        
        while self.is_running:
            try:
                # Check staging tables for new data
                staging_status = self.check_staging_tables_for_new_data()
                
                # Determine if ETL should be triggered
                should_trigger = self._should_trigger_etl(staging_status)
                
                if should_trigger:
                    self.logger.info("New data detected, triggering ETL run")
                    self._trigger_etl_run(staging_status)
                else:
                    self.logger.debug("No new data detected, continuing monitoring")
                
                # Reset error counter on successful check
                consecutive_errors = 0
                
                # Wait for next polling interval
                time.sleep(self.config['polling_interval_seconds'])
                
            except Exception as e:
                consecutive_errors += 1
                self.logger.error(f"Error in monitoring loop (attempt {consecutive_errors}/{max_errors}): {e}")
                
                if consecutive_errors >= max_errors:
                    self.logger.error("Maximum consecutive errors reached, stopping scheduler")
                    self.is_running = False
                    break
                
                # Wait before retrying
                time.sleep(min(60, self.config['polling_interval_seconds']))
        
        self.logger.info("Monitoring loop ended")
    
    def check_staging_tables_for_new_data(self) -> Dict[str, StagingTableStatus]:
        """
        Check all staging tables for new data since last ETL run.
        
        Returns:
            Dictionary mapping table types to their status information
        """
        self.logger.debug("Checking staging tables for new data")
        
        staging_status = {}
        
        for table_type, table_name in self.staging_tables.items():
            try:
                status = self._check_single_staging_table(table_type, table_name)
                staging_status[table_type] = status
                
                self.logger.debug(f"{table_name}: {status.new_records_since_last_run} new records")
                
            except Exception as e:
                self.logger.error(f"Error checking staging table {table_name}: {e}")
                # Create error status
                staging_status[table_type] = StagingTableStatus(
                    table_name=table_name,
                    total_records=0,
                    new_records_since_last_run=0,
                    last_processed_timestamp=None,
                    latest_record_timestamp=None,
                    has_new_data=False
                )
        
        return staging_status
    
    def _check_single_staging_table(self, table_type: str, table_name: str) -> StagingTableStatus:
        """Check a single staging table for new data"""
        
        # Get last processed timestamp for this table
        last_processed = self._get_last_processed_timestamp(table_type)
        
        # Query for total records
        total_query = f"""
            SELECT COUNT(*) as total_records
            FROM {self.config['schema']}.{table_name}
        """
        
        total_result = self.snowflake_client.execute_query(total_query, fetch=True)
        total_records = total_result[0]['TOTAL_RECORDS'] if total_result else 0
        
        # Query for latest record timestamp
        latest_query = f"""
            SELECT MAX(RECORD_METADATA:CreateTime::TIMESTAMP) as latest_timestamp
            FROM {self.config['schema']}.{table_name}
        """
        
        latest_result = self.snowflake_client.execute_query(latest_query, fetch=True)
        latest_timestamp = latest_result[0]['LATEST_TIMESTAMP'] if latest_result and latest_result[0]['LATEST_TIMESTAMP'] else None
        
        # Query for new records since last processed timestamp
        if last_processed:
            new_records_query = f"""
                SELECT COUNT(*) as new_records
                FROM {self.config['schema']}.{table_name}
                WHERE RECORD_METADATA:CreateTime::TIMESTAMP > '{last_processed.isoformat()}'
            """
        else:
            # If no last processed timestamp, use incremental lookback
            lookback_minutes = self.config['incremental_lookback_minutes']
            new_records_query = f"""
                SELECT COUNT(*) as new_records
                FROM {self.config['schema']}.{table_name}
                WHERE RECORD_METADATA:CreateTime::TIMESTAMP >= DATEADD(MINUTE, -{lookback_minutes}, CURRENT_TIMESTAMP())
            """
        
        new_records_result = self.snowflake_client.execute_query(new_records_query, fetch=True)
        new_records = new_records_result[0]['NEW_RECORDS'] if new_records_result else 0
        
        # Convert latest_timestamp to datetime if it exists
        if latest_timestamp and isinstance(latest_timestamp, str):
            latest_timestamp = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
        
        return StagingTableStatus(
            table_name=table_name,
            total_records=total_records,
            new_records_since_last_run=new_records,
            last_processed_timestamp=last_processed,
            latest_record_timestamp=latest_timestamp,
            has_new_data=new_records >= self.config['min_records_threshold']
        )
    
    def _should_trigger_etl(self, staging_status: Dict[str, StagingTableStatus]) -> bool:
        """
        Determine if ETL should be triggered based on staging table status.
        
        Args:
            staging_status: Status of all staging tables
            
        Returns:
            True if ETL should be triggered
        """
        # Check if any table has new data above threshold
        has_new_data = any(status.has_new_data for status in staging_status.values())
        
        if not has_new_data:
            return False
        
        # Additional checks can be added here:
        # - Time-based triggers (e.g., minimum time since last run)
        # - Business hours restrictions
        # - Data volume thresholds
        
        return True
    
    def _trigger_etl_run(self, staging_status: Dict[str, StagingTableStatus]) -> Dict[str, Any]:
        """
        Trigger an ETL run and track the execution.
        
        Args:
            staging_status: Current staging table status
            
        Returns:
            ETL execution results
        """
        run_id = f"scheduled_etl_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        # Create run metadata
        run_metadata = ETLRunMetadata(
            run_id=run_id,
            start_time=datetime.now(timezone.utc),
            staging_tables_checked=list(self.staging_tables.keys()),
            last_processed_timestamps={}
        )
        
        self.logger.info(f"Starting ETL run {run_id}")
        
        try:
            # Update ETL orchestrator with incremental processing parameters
            self._configure_incremental_processing(staging_status)
            
            # Run ETL
            etl_result = self.etl_orchestrator.run_automated_etl()
            
            # Update run metadata
            run_metadata.end_time = datetime.now(timezone.utc)
            run_metadata.status = etl_result.get('status', 'unknown')
            run_metadata.records_processed = etl_result.get('records_processed', 0)
            
            if etl_result.get('status') == 'success':
                # Update last processed timestamps
                self._update_last_processed_timestamps(staging_status)
                run_metadata.last_processed_timestamps = {
                    table_type: status.latest_record_timestamp.isoformat() if status.latest_record_timestamp else None
                    for table_type, status in staging_status.items()
                }
                
                self.logger.info(f"ETL run {run_id} completed successfully, processed {run_metadata.records_processed} records")
            else:
                run_metadata.error_message = etl_result.get('error', 'Unknown error')
                self.logger.error(f"ETL run {run_id} failed: {run_metadata.error_message}")
            
            # Save metadata
            self.last_run_metadata = run_metadata
            self._save_metadata()
            
            # Add scheduler metadata to ETL result
            result = etl_result.copy()
            result['scheduler_metadata'] = asdict(run_metadata)
            
            return result
            
        except Exception as e:
            # Handle ETL execution errors
            run_metadata.end_time = datetime.now(timezone.utc)
            run_metadata.status = 'error'
            run_metadata.error_message = str(e)
            
            self.logger.error(f"ETL run {run_id} failed with exception: {e}")
            
            # Save error metadata
            self.last_run_metadata = run_metadata
            self._save_metadata()
            
            return {
                'status': 'error',
                'error': str(e),
                'scheduler_metadata': asdict(run_metadata)
            }
    
    def _configure_incremental_processing(self, staging_status: Dict[str, StagingTableStatus]) -> None:
        """Configure ETL orchestrator for incremental processing"""
        # Update ETL orchestrator configuration with incremental parameters
        incremental_config = {}
        
        for table_type, status in staging_status.items():
            if status.last_processed_timestamp:
                incremental_config[f'{table_type}_since'] = status.last_processed_timestamp
        
        # Update ETL orchestrator config if it supports incremental processing
        if hasattr(self.etl_orchestrator, 'config'):
            self.etl_orchestrator.config.update(incremental_config)
    
    def _get_last_processed_timestamp(self, table_type: str) -> Optional[datetime]:
        """Get the last processed timestamp for a staging table type"""
        if not self.last_run_metadata or not self.last_run_metadata.last_processed_timestamps:
            return None
        
        timestamp_str = self.last_run_metadata.last_processed_timestamps.get(table_type)
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                return None
        
        return None
    
    def _update_last_processed_timestamps(self, staging_status: Dict[str, StagingTableStatus]) -> None:
        """Update last processed timestamps based on current staging status"""
        if not self.last_run_metadata:
            return
        
        for table_type, status in staging_status.items():
            if status.latest_record_timestamp:
                self.last_run_metadata.last_processed_timestamps[table_type] = status.latest_record_timestamp.isoformat()
    
    def _load_metadata(self) -> None:
        """Load scheduler metadata from file"""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r') as f:
                    metadata_dict = json.load(f)
                
                # Convert timestamps back to datetime objects
                if 'start_time' in metadata_dict:
                    metadata_dict['start_time'] = datetime.fromisoformat(metadata_dict['start_time'])
                if 'end_time' in metadata_dict and metadata_dict['end_time']:
                    metadata_dict['end_time'] = datetime.fromisoformat(metadata_dict['end_time'])
                
                self.last_run_metadata = ETLRunMetadata(**metadata_dict)
                self.logger.info(f"Loaded scheduler metadata from {self.metadata_file}")
            else:
                self.logger.info("No previous scheduler metadata found")
                
        except Exception as e:
            self.logger.warning(f"Could not load scheduler metadata: {e}")
            self.last_run_metadata = None
    
    def _save_metadata(self) -> None:
        """Save scheduler metadata to file"""
        try:
            if self.last_run_metadata:
                # Convert to dict and handle datetime serialization
                metadata_dict = asdict(self.last_run_metadata)
                
                # Convert datetime objects to ISO strings
                if metadata_dict['start_time']:
                    metadata_dict['start_time'] = metadata_dict['start_time'].isoformat()
                if metadata_dict['end_time']:
                    metadata_dict['end_time'] = metadata_dict['end_time'].isoformat()
                
                with open(self.metadata_file, 'w') as f:
                    json.dump(metadata_dict, f, indent=2, default=str)
                
                self.logger.debug(f"Saved scheduler metadata to {self.metadata_file}")
                
        except Exception as e:
            self.logger.error(f"Could not save scheduler metadata: {e}")
    
    def run_single_etl_check(self) -> Dict[str, Any]:
        """
        Run a single ETL check and trigger if needed (for manual/testing use).
        
        Returns:
            Dictionary with check results and ETL execution results if triggered
        """
        self.logger.info("Running single ETL check")
        
        try:
            # Check staging tables
            staging_status = self.check_staging_tables_for_new_data()
            
            # Log status
            for table_type, status in staging_status.items():
                self.logger.info(f"{status.table_name}: {status.new_records_since_last_run} new records")
            
            # Check if ETL should be triggered
            should_trigger = self._should_trigger_etl(staging_status)
            
            result = {
                'check_timestamp': datetime.now(timezone.utc).isoformat(),
                'staging_status': {k: asdict(v) for k, v in staging_status.items()},
                'should_trigger_etl': should_trigger,
                'etl_triggered': False
            }
            
            if should_trigger:
                self.logger.info("Triggering ETL run based on new data")
                etl_result = self._trigger_etl_run(staging_status)
                result['etl_result'] = etl_result
                result['etl_triggered'] = True
            else:
                self.logger.info("No ETL trigger needed")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in single ETL check: {e}")
            return {
                'check_timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
                'should_trigger_etl': False,
                'etl_triggered': False
            }
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """
        Get current scheduler status and metadata.
        
        Returns:
            Dictionary with scheduler status information
        """
        return {
            'is_running': self.is_running,
            'polling_interval_seconds': self.config['polling_interval_seconds'],
            'staging_tables': list(self.staging_tables.keys()),
            'last_run_metadata': asdict(self.last_run_metadata) if self.last_run_metadata else None,
            'config': self.config
        }
    
    def reset_metadata(self) -> None:
        """Reset scheduler metadata (for testing or recovery)"""
        self.logger.info("Resetting scheduler metadata")
        self.last_run_metadata = None
        
        if self.metadata_file.exists():
            self.metadata_file.unlink()
            self.logger.info(f"Deleted metadata file {self.metadata_file}")


def create_etl_scheduler(config: Optional[Dict[str, Any]] = None) -> ETLScheduler:
    """
    Factory function to create ETL scheduler instance.
    
    Args:
        config: Optional scheduler configuration
        
    Returns:
        Configured ETL scheduler instance
    """
    return ETLScheduler(config)


# CLI interface for scheduler operations
if __name__ == "__main__":
    import sys
    import argparse
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='ETL Scheduler Operations')
    parser.add_argument('command', choices=['start', 'check', 'status', 'reset'], 
                       help='Scheduler command to execute')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--polling-interval', type=int, default=300, 
                       help='Polling interval in seconds (default: 300)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = None
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
        except Exception as e:
            logger.error(f"Could not load config file {args.config}: {e}")
            sys.exit(1)
    else:
        config = {'polling_interval_seconds': args.polling_interval}
    
    # Create scheduler
    scheduler = create_etl_scheduler(config)
    
    try:
        if args.command == 'start':
            logger.info("Starting ETL scheduler monitoring...")
            scheduler.start_monitoring()
            
            # Keep main thread alive
            try:
                while scheduler.is_running:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                scheduler.stop_monitoring()
                
        elif args.command == 'check':
            logger.info("Running single ETL check...")
            result = scheduler.run_single_etl_check()
            print(json.dumps(result, indent=2, default=str))
            
        elif args.command == 'status':
            logger.info("Getting scheduler status...")
            status = scheduler.get_scheduler_status()
            print(json.dumps(status, indent=2, default=str))
            
        elif args.command == 'reset':
            logger.info("Resetting scheduler metadata...")
            scheduler.reset_metadata()
            logger.info("Metadata reset complete")
            
    except Exception as e:
        logger.error(f"Error executing command {args.command}: {e}")
        sys.exit(1)
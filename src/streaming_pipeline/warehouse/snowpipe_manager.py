"""
Snowpipe Manager for Automatic Data Loading

This module manages Snowpipe operations for automatic data loading from S3 to Snowflake,
including pipe creation, monitoring, and error handling.
"""

import logging
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta
import time

from .snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class SnowpipeManager:
    """Manager for Snowpipe operations"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        """
        Initialize Snowpipe manager
        
        Args:
            snowflake_client: Snowflake client instance
        """
        self.client = snowflake_client
        self.pipes = {}  # Cache for pipe configurations
        
    def create_pipe(
        self,
        pipe_name: str,
        table_name: str,
        stage_name: str,
        file_format: str = "STREAMING.PARQUET_FORMAT",
        auto_ingest: bool = True,
        copy_options: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create a Snowpipe for automatic data loading
        
        Args:
            pipe_name: Name of the pipe
            table_name: Target table name
            stage_name: Source stage name
            file_format: File format to use
            auto_ingest: Enable auto-ingest from S3
            copy_options: Additional COPY command options
            
        Returns:
            True if successful, False otherwise
        """
        copy_options = copy_options or {}
        
        # Build COPY statement
        copy_statement = f"""
            COPY INTO STREAMING.{table_name}
            FROM @STREAMING.{stage_name}
            FILE_FORMAT = {file_format}
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        
        # Add copy options
        if copy_options:
            options_str = ", ".join([f"{k} = {v}" for k, v in copy_options.items()])
            copy_statement += f"\n{options_str}"
        
        # Create pipe DDL
        pipe_ddl = f"""
            CREATE OR REPLACE PIPE STREAMING.{pipe_name}
            AUTO_INGEST = {auto_ingest}
            AS
            {copy_statement}
        """
        
        try:
            self.client.execute_query(pipe_ddl)
            
            # Cache pipe configuration
            self.pipes[pipe_name] = {
                'table_name': table_name,
                'stage_name': stage_name,
                'file_format': file_format,
                'auto_ingest': auto_ingest,
                'copy_options': copy_options,
                'created_at': datetime.now(timezone.utc)
            }
            
            logger.info(f"Successfully created pipe: {pipe_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating pipe {pipe_name}: {e}")
            return False
    
    def create_stock_prices_pipe(self) -> bool:
        """Create pipe for stock prices fact table"""
        copy_options = {
            "ON_ERROR": "'CONTINUE'",
            "PURGE": "TRUE",
            "FORCE": "FALSE"
        }
        
        return self.create_pipe(
            pipe_name="STOCK_PRICES_PIPE",
            table_name="FACT_STOCK_PRICES",
            stage_name="STREAMING_STAGE",
            copy_options=copy_options
        )
    
    def create_trading_volume_pipe(self) -> bool:
        """Create pipe for trading volume fact table"""
        copy_options = {
            "ON_ERROR": "'CONTINUE'",
            "PURGE": "TRUE",
            "FORCE": "FALSE"
        }
        
        return self.create_pipe(
            pipe_name="TRADING_VOLUME_PIPE",
            table_name="FACT_TRADING_VOLUME",
            stage_name="STREAMING_STAGE",
            copy_options=copy_options
        )
    
    def create_data_quality_pipe(self) -> bool:
        """Create pipe for data quality results"""
        copy_options = {
            "ON_ERROR": "'CONTINUE'",
            "PURGE": "TRUE"
        }
        
        return self.create_pipe(
            pipe_name="DATA_QUALITY_PIPE",
            table_name="DATA_QUALITY_RESULTS",
            stage_name="STREAMING_STAGE",
            copy_options=copy_options
        )
    
    def get_pipe_status(self, pipe_name: str) -> Optional[Dict[str, Any]]:
        """
        Get status information for a pipe
        
        Args:
            pipe_name: Name of the pipe
            
        Returns:
            Dictionary with pipe status information
        """
        query = f"""
            SELECT 
                PIPE_NAME,
                IS_AUTOINGEST_ENABLED,
                NOTIFICATION_CHANNEL_NAME,
                PIPE_EXECUTION_PAUSED,
                DEFINITION,
                CREATED_ON,
                MODIFIED_ON
            FROM INFORMATION_SCHEMA.PIPES
            WHERE PIPE_SCHEMA = 'STREAMING' AND PIPE_NAME = '{pipe_name.upper()}'
        """
        
        try:
            result = self.client.execute_query(query, fetch=True)
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting pipe status for {pipe_name}: {e}")
            return None
    
    def get_pipe_execution_history(
        self, 
        pipe_name: str, 
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get execution history for a pipe
        
        Args:
            pipe_name: Name of the pipe
            hours: Number of hours to look back
            
        Returns:
            List of execution history records
        """
        query = f"""
            SELECT 
                PIPE_NAME,
                FILE_NAME,
                STAGE_LOCATION,
                SENT_TIME,
                RECEIVED_TIME,
                LAST_LOAD_TIME,
                ROWS_PARSED,
                ROWS_LOADED,
                ERROR_SEEN,
                ERROR_CODE,
                ERROR_MESSAGE,
                STATUS
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE PIPE_NAME = 'STREAMING.{pipe_name.upper()}'
            AND LAST_LOAD_TIME >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
            ORDER BY LAST_LOAD_TIME DESC
        """
        
        try:
            return self.client.execute_query(query, fetch=True) or []
        except Exception as e:
            logger.error(f"Error getting pipe execution history for {pipe_name}: {e}")
            return []
    
    def get_pipe_load_statistics(self, pipe_name: str, hours: int = 24) -> Dict[str, Any]:
        """
        Get load statistics for a pipe
        
        Args:
            pipe_name: Name of the pipe
            hours: Number of hours to look back
            
        Returns:
            Dictionary with load statistics
        """
        query = f"""
            SELECT 
                COUNT(*) as total_files,
                SUM(ROWS_LOADED) as total_rows_loaded,
                SUM(ROWS_PARSED) as total_rows_parsed,
                COUNT(CASE WHEN ERROR_SEEN = TRUE THEN 1 END) as error_files,
                AVG(DATEDIFF(SECOND, SENT_TIME, LAST_LOAD_TIME)) as avg_load_time_seconds,
                MIN(LAST_LOAD_TIME) as first_load_time,
                MAX(LAST_LOAD_TIME) as last_load_time
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE PIPE_NAME = 'STREAMING.{pipe_name.upper()}'
            AND LAST_LOAD_TIME >= DATEADD(HOUR, -{hours}, CURRENT_TIMESTAMP())
        """
        
        try:
            result = self.client.execute_query(query, fetch=True)
            return result[0] if result else {}
        except Exception as e:
            logger.error(f"Error getting pipe load statistics for {pipe_name}: {e}")
            return {}
    
    def pause_pipe(self, pipe_name: str) -> bool:
        """
        Pause a pipe
        
        Args:
            pipe_name: Name of the pipe to pause
            
        Returns:
            True if successful, False otherwise
        """
        query = f"ALTER PIPE STREAMING.{pipe_name} SET PIPE_EXECUTION_PAUSED = TRUE"
        
        try:
            self.client.execute_query(query)
            logger.info(f"Paused pipe: {pipe_name}")
            return True
        except Exception as e:
            logger.error(f"Error pausing pipe {pipe_name}: {e}")
            return False
    
    def resume_pipe(self, pipe_name: str) -> bool:
        """
        Resume a paused pipe
        
        Args:
            pipe_name: Name of the pipe to resume
            
        Returns:
            True if successful, False otherwise
        """
        query = f"ALTER PIPE STREAMING.{pipe_name} SET PIPE_EXECUTION_PAUSED = FALSE"
        
        try:
            self.client.execute_query(query)
            logger.info(f"Resumed pipe: {pipe_name}")
            return True
        except Exception as e:
            logger.error(f"Error resuming pipe {pipe_name}: {e}")
            return False
    
    def refresh_pipe(self, pipe_name: str, path: Optional[str] = None) -> bool:
        """
        Manually refresh a pipe to process files
        
        Args:
            pipe_name: Name of the pipe to refresh
            path: Optional specific path to refresh
            
        Returns:
            True if successful, False otherwise
        """
        if path:
            query = f"ALTER PIPE STREAMING.{pipe_name} REFRESH PREFIX = '{path}'"
        else:
            query = f"ALTER PIPE STREAMING.{pipe_name} REFRESH"
        
        try:
            self.client.execute_query(query)
            logger.info(f"Refreshed pipe: {pipe_name}")
            return True
        except Exception as e:
            logger.error(f"Error refreshing pipe {pipe_name}: {e}")
            return False
    
    def drop_pipe(self, pipe_name: str) -> bool:
        """
        Drop a pipe
        
        Args:
            pipe_name: Name of the pipe to drop
            
        Returns:
            True if successful, False otherwise
        """
        query = f"DROP PIPE IF EXISTS STREAMING.{pipe_name}"
        
        try:
            self.client.execute_query(query)
            
            # Remove from cache
            if pipe_name in self.pipes:
                del self.pipes[pipe_name]
            
            logger.info(f"Dropped pipe: {pipe_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping pipe {pipe_name}: {e}")
            return False
    
    def list_pipes(self) -> List[Dict[str, Any]]:
        """
        List all pipes in the streaming schema
        
        Returns:
            List of pipe information dictionaries
        """
        query = """
            SELECT 
                PIPE_NAME,
                IS_AUTOINGEST_ENABLED,
                NOTIFICATION_CHANNEL_NAME,
                PIPE_EXECUTION_PAUSED,
                CREATED_ON,
                MODIFIED_ON
            FROM INFORMATION_SCHEMA.PIPES
            WHERE PIPE_SCHEMA = 'STREAMING'
            ORDER BY PIPE_NAME
        """
        
        try:
            return self.client.execute_query(query, fetch=True) or []
        except Exception as e:
            logger.error(f"Error listing pipes: {e}")
            return []
    
    def monitor_pipe_health(self, pipe_name: str, hours: int = 1) -> Dict[str, Any]:
        """
        Monitor pipe health and performance
        
        Args:
            pipe_name: Name of the pipe to monitor
            hours: Number of hours to analyze
            
        Returns:
            Dictionary with health metrics
        """
        stats = self.get_pipe_load_statistics(pipe_name, hours)
        history = self.get_pipe_execution_history(pipe_name, hours)
        
        health_metrics = {
            'pipe_name': pipe_name,
            'monitoring_period_hours': hours,
            'total_files_processed': stats.get('TOTAL_FILES', 0),
            'total_rows_loaded': stats.get('TOTAL_ROWS_LOADED', 0),
            'error_files': stats.get('ERROR_FILES', 0),
            'error_rate': 0,
            'avg_load_time_seconds': stats.get('AVG_LOAD_TIME_SECONDS', 0),
            'last_activity': stats.get('LAST_LOAD_TIME'),
            'health_status': 'UNKNOWN',
            'recent_errors': []
        }
        
        # Calculate error rate
        if health_metrics['total_files_processed'] > 0:
            health_metrics['error_rate'] = (
                health_metrics['error_files'] / health_metrics['total_files_processed']
            ) * 100
        
        # Determine health status
        if health_metrics['total_files_processed'] == 0:
            health_metrics['health_status'] = 'NO_ACTIVITY'
        elif health_metrics['error_rate'] > 10:
            health_metrics['health_status'] = 'UNHEALTHY'
        elif health_metrics['error_rate'] > 5:
            health_metrics['health_status'] = 'WARNING'
        else:
            health_metrics['health_status'] = 'HEALTHY'
        
        # Get recent errors
        health_metrics['recent_errors'] = [
            {
                'file_name': record['FILE_NAME'],
                'error_code': record['ERROR_CODE'],
                'error_message': record['ERROR_MESSAGE'],
                'timestamp': record['LAST_LOAD_TIME']
            }
            for record in history
            if record.get('ERROR_SEEN') is True
        ][:5]  # Last 5 errors
        
        return health_metrics
    
    def setup_all_pipes(self) -> Dict[str, bool]:
        """
        Set up all required pipes for the streaming pipeline
        
        Returns:
            Dictionary with pipe creation results
        """
        results = {}
        
        pipe_creators = [
            ('STOCK_PRICES_PIPE', self.create_stock_prices_pipe),
            ('TRADING_VOLUME_PIPE', self.create_trading_volume_pipe),
            ('DATA_QUALITY_PIPE', self.create_data_quality_pipe)
        ]
        
        for pipe_name, creator_func in pipe_creators:
            try:
                results[pipe_name] = creator_func()
                if results[pipe_name]:
                    logger.info(f"Successfully set up pipe: {pipe_name}")
                else:
                    logger.error(f"Failed to set up pipe: {pipe_name}")
            except Exception as e:
                logger.error(f"Error setting up pipe {pipe_name}: {e}")
                results[pipe_name] = False
        
        return results
    
    def get_notification_channel_info(self, pipe_name: str) -> Optional[Dict[str, Any]]:
        """
        Get notification channel information for a pipe
        
        Args:
            pipe_name: Name of the pipe
            
        Returns:
            Dictionary with notification channel information
        """
        query = f"""
            SHOW PIPES LIKE '{pipe_name.upper()}' IN SCHEMA STREAMING
        """
        
        try:
            result = self.client.execute_query(query, fetch=True)
            if result:
                # Extract notification channel from the result
                pipe_info = result[0]
                return {
                    'notification_channel': pipe_info.get('notification_channel'),
                    'pipe_name': pipe_info.get('name'),
                    'auto_ingest': pipe_info.get('is_autoingest_enabled')
                }
            return None
        except Exception as e:
            logger.error(f"Error getting notification channel info for {pipe_name}: {e}")
            return None
"""
Snowflake Integration Orchestrator

This module orchestrates the complete Snowflake data warehouse integration,
coordinating schema setup, S3 staging, and Snowpipe configuration.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import pandas as pd

from .snowflake_client import SnowflakeClient
from .schema_manager import SchemaManager
from .s3_staging import S3StagingManager
from .snowpipe_manager import SnowpipeManager
from ..config.settings import get_settings

logger = logging.getLogger(__name__)


class SnowflakeIntegration:
    """Complete Snowflake data warehouse integration orchestrator"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Snowflake integration
        
        Args:
            config: Optional configuration dict, uses settings if not provided
        """
        self.settings = get_settings()
        self.config = config or self._get_integration_config()
        
        # Initialize components
        self.snowflake_client = SnowflakeClient()
        self.schema_manager = SchemaManager(self.snowflake_client)
        self.s3_staging = S3StagingManager()
        self.snowpipe_manager = SnowpipeManager(self.snowflake_client)
        
        self.is_initialized = False
        
    def _get_integration_config(self) -> Dict[str, Any]:
        """Get integration configuration from settings"""
        return {
            "s3_bucket": self.settings.s3_bucket_name,
            "aws_role_arn": self.settings.aws_role_arn,
            "setup_dimensions": True,
            "enable_monitoring": True,
            "auto_optimize": True
        }
    
    def initialize_warehouse(self, force_recreate: bool = False) -> bool:
        """
        Initialize the complete data warehouse setup
        
        Args:
            force_recreate: Whether to recreate existing objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting Snowflake data warehouse initialization...")
            
            # Set up complete schema
            self.schema_manager.setup_complete_schema(
                s3_bucket=self.config["s3_bucket"],
                aws_role_arn=self.config["aws_role_arn"],
                populate_dimensions=self.config["setup_dimensions"]
            )
            
            # Set up all Snowpipes
            pipe_results = self.snowpipe_manager.setup_all_pipes()
            
            # Check if all pipes were created successfully
            failed_pipes = [name for name, success in pipe_results.items() if not success]
            if failed_pipes:
                logger.warning(f"Some pipes failed to create: {failed_pipes}")
            else:
                logger.info("All Snowpipes created successfully")
            
            self.is_initialized = True
            logger.info("Snowflake data warehouse initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing data warehouse: {e}")
            return False
    
    def load_stock_prices_data(
        self, 
        df: pd.DataFrame, 
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Load stock prices data through the complete pipeline
        
        Args:
            df: DataFrame with stock prices data
            timestamp: Optional timestamp for file naming
            
        Returns:
            Dictionary with load results
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        try:
            # Upload to S3 staging
            s3_key = self.s3_staging.upload_dataframe_as_parquet(
                df=df,
                table_name="fact_stock_prices",
                timestamp=timestamp
            )
            
            if not s3_key:
                return {
                    "success": False,
                    "error": "Failed to upload to S3",
                    "s3_key": None,
                    "records_processed": 0
                }
            
            # Trigger Snowpipe refresh (optional, as auto-ingest should handle this)
            self.snowpipe_manager.refresh_pipe("STOCK_PRICES_PIPE")
            
            # Log the load operation
            self._log_load_operation(
                table_name="FACT_STOCK_PRICES",
                s3_key=s3_key,
                records_count=len(df),
                status="STAGED"
            )
            
            return {
                "success": True,
                "s3_key": s3_key,
                "records_processed": len(df),
                "timestamp": timestamp.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error loading stock prices data: {e}")
            return {
                "success": False,
                "error": str(e),
                "s3_key": None,
                "records_processed": 0
            }
    
    def load_trading_volume_data(
        self, 
        df: pd.DataFrame, 
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Load trading volume data through the complete pipeline
        
        Args:
            df: DataFrame with trading volume data
            timestamp: Optional timestamp for file naming
            
        Returns:
            Dictionary with load results
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        try:
            # Upload to S3 staging
            s3_key = self.s3_staging.upload_dataframe_as_parquet(
                df=df,
                table_name="fact_trading_volume",
                timestamp=timestamp
            )
            
            if not s3_key:
                return {
                    "success": False,
                    "error": "Failed to upload to S3",
                    "s3_key": None,
                    "records_processed": 0
                }
            
            # Trigger Snowpipe refresh
            self.snowpipe_manager.refresh_pipe("TRADING_VOLUME_PIPE")
            
            # Log the load operation
            self._log_load_operation(
                table_name="FACT_TRADING_VOLUME",
                s3_key=s3_key,
                records_count=len(df),
                status="STAGED"
            )
            
            return {
                "success": True,
                "s3_key": s3_key,
                "records_processed": len(df),
                "timestamp": timestamp.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error loading trading volume data: {e}")
            return {
                "success": False,
                "error": str(e),
                "s3_key": None,
                "records_processed": 0
            }
    
    def load_data_quality_results(
        self, 
        df: pd.DataFrame, 
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Load data quality results
        
        Args:
            df: DataFrame with data quality results
            timestamp: Optional timestamp for file naming
            
        Returns:
            Dictionary with load results
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        
        try:
            # Upload to S3 staging
            s3_key = self.s3_staging.upload_dataframe_as_parquet(
                df=df,
                table_name="data_quality_results",
                timestamp=timestamp
            )
            
            if not s3_key:
                return {
                    "success": False,
                    "error": "Failed to upload to S3",
                    "s3_key": None,
                    "records_processed": 0
                }
            
            # Trigger Snowpipe refresh
            self.snowpipe_manager.refresh_pipe("DATA_QUALITY_PIPE")
            
            return {
                "success": True,
                "s3_key": s3_key,
                "records_processed": len(df),
                "timestamp": timestamp.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error loading data quality results: {e}")
            return {
                "success": False,
                "error": str(e),
                "s3_key": None,
                "records_processed": 0
            }
    
    def _log_load_operation(
        self,
        table_name: str,
        s3_key: str,
        records_count: int,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """
        Log load operation to monitoring table
        
        Args:
            table_name: Target table name
            s3_key: S3 key of the loaded file
            records_count: Number of records processed
            status: Load status
            error_message: Optional error message
        """
        try:
            log_query = """
                INSERT INTO STREAMING.LOAD_HISTORY 
                (TABLE_NAME, S3_KEY, LOAD_STATUS, RECORDS_LOADED, LOAD_START_TIME, ERROR_MESSAGE)
                VALUES (%(table_name)s, %(s3_key)s, %(status)s, %(records_count)s, %(timestamp)s, %(error_message)s)
            """
            
            self.snowflake_client.execute_query(
                log_query,
                params={
                    'table_name': table_name,
                    's3_key': s3_key,
                    'status': status,
                    'records_count': records_count,
                    'timestamp': datetime.now(timezone.utc),
                    'error_message': error_message
                }
            )
            
        except Exception as e:
            logger.warning(f"Could not log load operation: {e}")
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """
        Get overall pipeline health status
        
        Returns:
            Dictionary with health metrics
        """
        try:
            health_data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'overall_status': 'HEALTHY',
                'pipes': {},
                's3_staging': {},
                'recent_loads': []
            }
            
            # Check pipe health
            pipes = ['STOCK_PRICES_PIPE', 'TRADING_VOLUME_PIPE', 'DATA_QUALITY_PIPE']
            unhealthy_pipes = 0
            
            for pipe_name in pipes:
                pipe_health = self.snowpipe_manager.monitor_pipe_health(pipe_name, hours=1)
                health_data['pipes'][pipe_name] = pipe_health
                
                if pipe_health['health_status'] in ['UNHEALTHY', 'WARNING']:
                    unhealthy_pipes += 1
            
            # Check S3 staging
            staging_stats = self.s3_staging.get_staging_statistics()
            health_data['s3_staging'] = staging_stats
            
            # Get recent load history
            recent_loads_query = """
                SELECT 
                    TABLE_NAME,
                    LOAD_STATUS,
                    RECORDS_LOADED,
                    LOAD_START_TIME,
                    ERROR_MESSAGE
                FROM STREAMING.LOAD_HISTORY
                WHERE LOAD_START_TIME >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                ORDER BY LOAD_START_TIME DESC
                LIMIT 10
            """
            
            health_data['recent_loads'] = self.snowflake_client.execute_query(
                recent_loads_query, 
                fetch=True
            ) or []
            
            # Determine overall status
            if unhealthy_pipes > 0:
                health_data['overall_status'] = 'WARNING' if unhealthy_pipes == 1 else 'UNHEALTHY'
            
            return health_data
            
        except Exception as e:
            logger.error(f"Error getting pipeline health: {e}")
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'overall_status': 'ERROR',
                'error': str(e)
            }
    
    def optimize_tables(self) -> Dict[str, bool]:
        """
        Optimize all tables for better performance
        
        Returns:
            Dictionary with optimization results
        """
        tables_to_optimize = [
            'FACT_STOCK_PRICES',
            'FACT_TRADING_VOLUME',
            'DIM_COMPANY',
            'DIM_DATE',
            'DIM_TIME'
        ]
        
        results = {}
        
        for table_name in tables_to_optimize:
            try:
                self.snowflake_client.optimize_table(table_name, schema='STREAMING')
                results[table_name] = True
                logger.info(f"Optimized table: {table_name}")
            except Exception as e:
                logger.error(f"Error optimizing table {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> Dict[str, Any]:
        """
        Clean up old data and files
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Dictionary with cleanup results
        """
        results = {
            's3_files_deleted': 0,
            'load_history_cleaned': 0,
            'data_quality_cleaned': 0
        }
        
        try:
            # Clean up old S3 files
            results['s3_files_deleted'] = self.s3_staging.cleanup_old_files(days_to_keep)
            
            # Clean up old load history
            cleanup_load_history_query = f"""
                DELETE FROM STREAMING.LOAD_HISTORY
                WHERE CREATED_AT < DATEADD(DAY, -{days_to_keep}, CURRENT_TIMESTAMP())
            """
            
            self.snowflake_client.execute_query(cleanup_load_history_query)
            
            # Clean up old data quality results
            cleanup_dq_query = f"""
                DELETE FROM STREAMING.DATA_QUALITY_RESULTS
                WHERE CHECK_TIMESTAMP < DATEADD(DAY, -{days_to_keep}, CURRENT_TIMESTAMP())
            """
            
            self.snowflake_client.execute_query(cleanup_dq_query)
            
            logger.info(f"Cleanup completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            results['error'] = str(e)
            return results
    
    def get_warehouse_usage_report(self, days: int = 7) -> Dict[str, Any]:
        """
        Get warehouse usage report
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with usage statistics
        """
        try:
            # Get warehouse usage
            usage_data = self.snowflake_client.get_warehouse_usage(hours=days * 24)
            
            # Get query history
            query_history = self.snowflake_client.get_query_history(hours=days * 24)
            
            # Calculate summary statistics
            total_credits = sum(record.get('CREDITS_USED', 0) for record in usage_data)
            total_queries = len(query_history)
            failed_queries = len([q for q in query_history if q.get('EXECUTION_STATUS') != 'SUCCESS'])
            
            return {
                'period_days': days,
                'total_credits_used': total_credits,
                'total_queries': total_queries,
                'failed_queries': failed_queries,
                'success_rate': ((total_queries - failed_queries) / total_queries * 100) if total_queries > 0 else 0,
                'usage_details': usage_data,
                'recent_queries': query_history[:10]  # Last 10 queries
            }
            
        except Exception as e:
            logger.error(f"Error generating usage report: {e}")
            return {'error': str(e)}
    
    def close(self) -> None:
        """Close all connections and clean up resources"""
        try:
            self.snowflake_client.close()
            logger.info("Snowflake integration closed successfully")
        except Exception as e:
            logger.warning(f"Error closing Snowflake integration: {e}")
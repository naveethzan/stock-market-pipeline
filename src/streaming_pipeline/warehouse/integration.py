"""
Snowflake Integration Orchestrator

This module orchestrates the Snowflake data warehouse integration,
coordinating schema setup for Kafka Connect streaming.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from .snowflake_client import SnowflakeClient
from .schema_manager import SchemaManager
from ..config.settings import get_settings

logger = logging.getLogger(__name__)


class SnowflakeIntegration:
    """Snowflake data warehouse integration for Kafka Connect streaming"""
    
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
        
        self.is_initialized = False
        
    def _get_integration_config(self) -> Dict[str, Any]:
        """Get integration configuration from settings"""
        return {
            "setup_dimensions": True,
            "enable_monitoring": True,
            "auto_optimize": True
        }
    
    def initialize_warehouse(self, force_recreate: bool = False) -> bool:
        """
        Initialize the data warehouse schema for Kafka Connect streaming
        
        Args:
            force_recreate: Whether to recreate existing objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting Snowflake data warehouse initialization for Kafka Connect...")
            
            # Set up complete schema for Kafka Connect
            self.schema_manager.setup_complete_schema(
                populate_dimensions=self.config["setup_dimensions"]
            )
            
            self.is_initialized = True
            logger.info("Snowflake data warehouse initialization completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing data warehouse: {e}")
            return False
    
    def get_kafka_connect_status(self) -> Dict[str, Any]:
        """
        Get status of Kafka Connect connectors for Snowflake
        
        Returns:
            Dictionary with connector status information
        """
        try:
            # This would typically call Kafka Connect REST API
            # For now, return a placeholder structure
            return {
                "gold-snowflake-sink-connector": {
                    "status": "RUNNING",
                    "tasks": [
                        {
                            "id": 0,
                            "state": "RUNNING",
                            "worker_id": "kafka-connect:8083"
                        }
                    ]
                }
            }
        except Exception as e:
            logger.error(f"Error getting Kafka Connect status: {e}")
            return {"error": str(e)}
    
    def get_streaming_table_stats(self) -> Dict[str, Any]:
        """
        Get statistics for streaming tables populated by Kafka Connect
        
        Returns:
            Dictionary with table statistics
        """
        try:
            stats = {}
            
            # Get stats for staging tables (populated by Kafka Connect)
            staging_tables = [
                "FACT_STOCK_PRICES_STAGING",
                "FACT_TRADING_VOLUME_STAGING", 
                "TECHNICAL_INDICATORS_STAGING"
            ]
            
            for table_name in staging_tables:
                query = f"""
                    SELECT 
                        COUNT(*) as total_records,
                        MAX(RECORD_METADATA:CreateTime) as last_ingestion_time,
                        COUNT(DISTINCT RECORD_METADATA:topic) as topics_count
                    FROM STREAMING.{table_name}
                    WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                """
                
                result = self.snowflake_client.execute_query(query, fetch=True)
                if result:
                    stats[table_name] = result[0]
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting streaming table stats: {e}")
            return {"error": str(e)}
    
    def _log_streaming_operation(
        self,
        operation_type: str,
        table_name: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        """
        Log streaming operation to monitoring table
        
        Args:
            operation_type: Type of operation (e.g., 'kafka_connect_check')
            table_name: Target table name
            status: Operation status
            details: Optional operation details
            error_message: Optional error message
        """
        try:
            log_query = """
                INSERT INTO STREAMING.STREAMING_OPERATIONS_LOG 
                (OPERATION_TYPE, TABLE_NAME, STATUS, OPERATION_TIMESTAMP, DETAILS, ERROR_MESSAGE)
                VALUES (%(operation_type)s, %(table_name)s, %(status)s, %(timestamp)s, %(details)s, %(error_message)s)
            """
            
            self.snowflake_client.execute_query(
                log_query,
                params={
                    'operation_type': operation_type,
                    'table_name': table_name,
                    'status': status,
                    'timestamp': datetime.now(timezone.utc),
                    'details': str(details) if details else None,
                    'error_message': error_message
                }
            )
            
        except Exception as e:
            logger.warning(f"Could not log streaming operation: {e}")
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """
        Get overall streaming pipeline health status
        
        Returns:
            Dictionary with health metrics
        """
        try:
            health_data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'overall_status': 'HEALTHY',
                'kafka_connect': {},
                'streaming_tables': {},
                'recent_operations': []
            }
            
            # Check Kafka Connect status
            kafka_connect_status = self.get_kafka_connect_status()
            health_data['kafka_connect'] = kafka_connect_status
            
            # Check streaming table statistics
            table_stats = self.get_streaming_table_stats()
            health_data['streaming_tables'] = table_stats
            
            # Get recent streaming operations
            recent_ops_query = """
                SELECT 
                    OPERATION_TYPE,
                    TABLE_NAME,
                    STATUS,
                    OPERATION_TIMESTAMP,
                    ERROR_MESSAGE
                FROM STREAMING.STREAMING_OPERATIONS_LOG
                WHERE OPERATION_TIMESTAMP >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                ORDER BY OPERATION_TIMESTAMP DESC
                LIMIT 10
            """
            
            try:
                health_data['recent_operations'] = self.snowflake_client.execute_query(
                    recent_ops_query, 
                    fetch=True
                ) or []
            except Exception:
                # Table might not exist yet
                health_data['recent_operations'] = []
            
            # Determine overall status based on Kafka Connect and table activity
            if 'error' in kafka_connect_status:
                health_data['overall_status'] = 'WARNING'
            elif not table_stats or 'error' in table_stats:
                health_data['overall_status'] = 'WARNING'
            
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
        Clean up old streaming data
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Dictionary with cleanup results
        """
        results = {
            'operations_log_cleaned': 0,
            'staging_tables_cleaned': 0
        }
        
        try:
            # Clean up old streaming operations log
            cleanup_ops_query = f"""
                DELETE FROM STREAMING.STREAMING_OPERATIONS_LOG
                WHERE OPERATION_TIMESTAMP < DATEADD(DAY, -{days_to_keep}, CURRENT_TIMESTAMP())
            """
            
            try:
                self.snowflake_client.execute_query(cleanup_ops_query)
                results['operations_log_cleaned'] = 1
            except Exception:
                # Table might not exist
                pass
            
            # Clean up old staging table data (keep recent data for analysis)
            staging_tables = [
                "FACT_STOCK_PRICES_STAGING",
                "FACT_TRADING_VOLUME_STAGING", 
                "TECHNICAL_INDICATORS_STAGING"
            ]
            
            for table_name in staging_tables:
                cleanup_staging_query = f"""
                    DELETE FROM STREAMING.{table_name}
                    WHERE RECORD_METADATA:CreateTime < DATEADD(DAY, -{days_to_keep}, CURRENT_TIMESTAMP())
                """
                
                try:
                    self.snowflake_client.execute_query(cleanup_staging_query)
                    results['staging_tables_cleaned'] += 1
                except Exception as e:
                    logger.warning(f"Could not clean up {table_name}: {e}")
            
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
"""
Snowflake Client for Data Warehouse Operations

This module provides a client for connecting to Snowflake and executing
data warehouse operations including DDL, DML, and monitoring queries.
"""

import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config.settings import get_settings

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """Client for Snowflake data warehouse operations"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Snowflake client
        
        Args:
            config: Optional configuration dict, uses settings if not provided
        """
        self.settings = get_settings()
        self.config = config or self._get_snowflake_config()
        self._connection = None
        self._connection_pool = []
        self.max_pool_size = 5
        
    def _get_snowflake_config(self) -> Dict[str, Any]:
        """Get Snowflake configuration from settings"""
        return {
            "account": self.settings.snowflake_account,
            "user": self.settings.snowflake_user,
            "password": self.settings.snowflake_password,
            "warehouse": self.settings.snowflake_warehouse,
            "database": self.settings.snowflake_database,
            "schema": self.settings.snowflake_schema,
            "role": self.settings.snowflake_role,
            "client_session_keep_alive": True,
            "autocommit": False
        }
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Create connection to Snowflake with retry logic
        
        Returns:
            Snowflake connection object
        """
        try:
            connection = snowflake.connector.connect(**self.config)
            logger.info("Successfully connected to Snowflake")
            return connection
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a Snowflake connection
        
        Yields:
            Snowflake connection object
        """
        connection = None
        try:
            connection = self.connect()
            yield connection
        except Exception as e:
            logger.error(f"Error in Snowflake connection context: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        fetch: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, None otherwise
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor(DictCursor)
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch:
                    results = cursor.fetchall()
                    logger.info(f"Query executed successfully, fetched {len(results)} rows")
                    return results
                else:
                    conn.commit()
                    logger.info("Query executed successfully")
                    return None
                    
            except (DatabaseError, ProgrammingError) as e:
                logger.error(f"Database error executing query: {e}")
                conn.rollback()
                raise
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
    
    def execute_many(
        self, 
        query: str, 
        data: List[Dict[str, Any]]
    ) -> None:
        """
        Execute a query with multiple parameter sets
        
        Args:
            query: SQL query to execute
            data: List of parameter dictionaries
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.executemany(query, data)
                conn.commit()
                logger.info(f"Batch query executed successfully for {len(data)} records")
                
            except (DatabaseError, ProgrammingError) as e:
                logger.error(f"Database error in batch execution: {e}")
                conn.rollback()
                raise
            except Exception as e:
                logger.error(f"Unexpected error in batch execution: {e}")
                conn.rollback()
                raise
            finally:
                cursor.close()
    
    def bulk_insert_from_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append"
    ) -> None:
        """
        Bulk insert data from pandas DataFrame
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            schema: Target schema (uses default if not provided)
            if_exists: What to do if table exists ('append', 'replace', 'fail')
        """
        schema = schema or self.config["schema"]
        full_table_name = f"{schema}.{table_name}"
        
        try:
            with self.get_connection() as conn:
                # Use Snowflake's pandas integration
                success, nchunks, nrows, _ = df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    method='multi'
                )
                
                if success:
                    logger.info(f"Successfully inserted {nrows} rows into {full_table_name}")
                else:
                    raise Exception(f"Failed to insert data into {full_table_name}")
                    
        except Exception as e:
            logger.error(f"Error in bulk insert: {e}")
            raise
    
    def check_table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if not provided)
            
        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.config["schema"]
        
        query = """
        SELECT COUNT(*) as count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        
        try:
            result = self.execute_query(
                query, 
                params=(schema.upper(), table_name.upper()),
                fetch=True
            )
            return result[0]["COUNT"] > 0 if result else False
            
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get table column information
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if not provided)
            
        Returns:
            List of column information dictionaries
        """
        schema = schema or self.config["schema"]
        
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            return self.execute_query(
                query,
                params=(schema.upper(), table_name.upper()),
                fetch=True
            )
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return []
    
    def get_warehouse_usage(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get warehouse usage statistics
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of usage statistics
        """
        query = """
        SELECT 
            WAREHOUSE_NAME,
            START_TIME,
            END_TIME,
            CREDITS_USED,
            CREDITS_USED_COMPUTE,
            CREDITS_USED_CLOUD_SERVICES
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(HOUR, -%s, CURRENT_TIMESTAMP())
        ORDER BY START_TIME DESC
        """
        
        try:
            return self.execute_query(query, params=(hours,), fetch=True)
        except Exception as e:
            logger.error(f"Error getting warehouse usage: {e}")
            return []
    
    def optimize_table(self, table_name: str, schema: Optional[str] = None) -> None:
        """
        Optimize table by running ANALYZE TABLE
        
        Args:
            table_name: Name of the table
            schema: Schema name (uses default if not provided)
        """
        schema = schema or self.config["schema"]
        full_table_name = f"{schema}.{table_name}"
        
        query = f"ANALYZE TABLE {full_table_name}"
        
        try:
            self.execute_query(query)
            logger.info(f"Table {full_table_name} optimized successfully")
        except Exception as e:
            logger.error(f"Error optimizing table {full_table_name}: {e}")
            raise
    
    def get_query_history(self, hours: int = 1) -> List[Dict[str, Any]]:
        """
        Get recent query history
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of query history records
        """
        query = """
        SELECT 
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            WAREHOUSE_NAME,
            START_TIME,
            END_TIME,
            TOTAL_ELAPSED_TIME,
            EXECUTION_STATUS,
            ERROR_MESSAGE
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(HOUR, -%s, CURRENT_TIMESTAMP())
        ORDER BY START_TIME DESC
        LIMIT 100
        """
        
        try:
            return self.execute_query(query, params=(hours,), fetch=True)
        except Exception as e:
            logger.error(f"Error getting query history: {e}")
            return []
    
    def close(self) -> None:
        """Close all connections"""
        if self._connection:
            self._connection.close()
            self._connection = None
        
        for conn in self._connection_pool:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing pooled connection: {e}")
        
        self._connection_pool.clear()
        logger.info("All Snowflake connections closed")
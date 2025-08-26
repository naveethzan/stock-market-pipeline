"""
Snowflake Dimensional ETL Orchestrator

This module provides ETL orchestration for transforming Kafka Connect staging data
into dimensional tables in Snowflake. It reads JSON data from staging tables,
applies dimensional modeling transformations, and loads the results into fact
and dimension tables.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone
import json
import pandas as pd

from .snowflake_client import SnowflakeClient
from ..models.dimensional import DimensionalModelBuilder, DimensionConfig
from ..config.settings import get_settings

logger = logging.getLogger(__name__)


class SnowflakeDimensionalETL:
    """
    ETL orchestrator for transforming Kafka Connect staging data into dimensional tables.
    
    This class reads JSON data from Kafka Connect staging tables in Snowflake,
    applies dimensional modeling transformations using existing logic, and loads
    the results into fact and dimension tables.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Snowflake Dimensional ETL orchestrator
        
        Args:
            config: Optional configuration dict, uses settings if not provided
        """
        self.settings = get_settings()
        self.config = config or self._get_etl_config()
        
        # Initialize components
        self.snowflake_client = SnowflakeClient()
        
        # Staging table names
        self.staging_tables = {
            'stock_prices': 'FACT_STOCK_PRICES_STAGING',
            'trading_volume': 'FACT_TRADING_VOLUME_STAGING',
            'technical_indicators': 'TECHNICAL_INDICATORS_STAGING'
        }
        
        # Dimensional table names
        self.dimensional_tables = {
            'fact_stock_prices': 'FACT_STOCK_PRICES',
            'fact_trading_volume': 'FACT_TRADING_VOLUME',
            'dim_company': 'DIM_COMPANY',
            'dim_date': 'DIM_DATE',
            'dim_time': 'DIM_TIME'
        }
        
        self.logger = logging.getLogger(__name__)
    
    def _get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration from settings"""
        return {
            "batch_size": 1000,
            "enable_scd_type2": True,
            "data_source": "kafka_connect",
            "schema": self.settings.snowflake_schema or "STREAMING"
        }
    
    def process_staging_data(self) -> Dict[str, Any]:
        """
        Main entry point for processing staging data into dimensional tables.
        
        Returns:
            Dictionary with processing results and statistics
        """
        self.logger.info("Starting Snowflake dimensional ETL process")
        
        try:
            # Check for new staging data
            staging_stats = self._check_staging_data()
            if not any(stats['new_records'] > 0 for stats in staging_stats.values()):
                self.logger.info("No new staging data found, skipping ETL process")
                return {"status": "skipped", "reason": "no_new_data"}
            
            # Extract and parse staging data
            parsed_data = self._extract_staging_data()
            if not parsed_data:
                self.logger.warning("No valid data extracted from staging tables")
                return {"status": "skipped", "reason": "no_valid_data"}
            
            # Transform data using dimensional modeling logic
            dimensional_data = self._transform_to_dimensional(parsed_data)
            
            # Load data into dimensional tables
            load_results = self._load_to_snowflake(dimensional_data)
            
            # Update processing metadata
            self._update_processing_metadata(load_results)
            
            result = {
                "status": "success",
                "staging_stats": staging_stats,
                "records_processed": sum(len(df) for df in parsed_data.values() if df is not None),
                "load_results": load_results,
                "processing_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.logger.info(f"ETL process completed successfully: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in ETL process: {e}")
            return {
                "status": "error",
                "error": str(e),
                "processing_timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _check_staging_data(self) -> Dict[str, Dict[str, Any]]:
        """
        Check staging tables for new data since last ETL run.
        
        Returns:
            Dictionary with staging table statistics
        """
        stats = {}
        
        for table_type, table_name in self.staging_tables.items():
            try:
                # Get total record count
                count_query = f"""
                    SELECT COUNT(*) as total_records
                    FROM {self.config['schema']}.{table_name}
                """
                
                result = self.snowflake_client.execute_query(count_query, fetch=True)
                total_records = result[0]['TOTAL_RECORDS'] if result else 0
                
                # Get incremental processing timestamp for this table type
                since_timestamp = self.config.get(f'{table_type}_since')
                
                if since_timestamp:
                    # Use specific timestamp for incremental processing
                    if isinstance(since_timestamp, str):
                        since_timestamp_str = since_timestamp
                    else:
                        since_timestamp_str = since_timestamp.isoformat()
                    
                    new_records_query = f"""
                        SELECT COUNT(*) as new_records
                        FROM {self.config['schema']}.{table_name}
                        WHERE RECORD_METADATA:CreateTime::TIMESTAMP > '{since_timestamp_str}'
                    """
                else:
                    # Default to last hour if no incremental timestamp provided
                    new_records_query = f"""
                        SELECT COUNT(*) as new_records
                        FROM {self.config['schema']}.{table_name}
                        WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
                    """
                
                result = self.snowflake_client.execute_query(new_records_query, fetch=True)
                new_records = result[0]['NEW_RECORDS'] if result else 0
                
                stats[table_type] = {
                    'table_name': table_name,
                    'total_records': total_records,
                    'new_records': new_records,
                    'incremental_since': since_timestamp_str if since_timestamp else None
                }
                
            except Exception as e:
                self.logger.warning(f"Error checking staging table {table_name}: {e}")
                stats[table_type] = {
                    'table_name': table_name,
                    'total_records': 0,
                    'new_records': 0,
                    'error': str(e)
                }
        
        return stats
    
    def _extract_staging_data(self) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Extract and parse JSON data from staging tables.
        
        Returns:
            Dictionary with parsed DataFrames for each data type
        """
        parsed_data = {}
        
        # Extract stock prices data
        parsed_data['stock_prices'] = self._extract_stock_prices_staging()
        
        # Extract trading volume data  
        parsed_data['trading_volume'] = self._extract_trading_volume_staging()
        
        # Extract technical indicators data
        parsed_data['technical_indicators'] = self._extract_technical_indicators_staging()
        
        return parsed_data
    
    def _extract_stock_prices_staging(self) -> Optional[pd.DataFrame]:
        """Extract stock price data from staging table with incremental processing support."""
        try:
            # Get incremental processing timestamp
            since_timestamp = self.config.get('stock_prices_since')
            
            if since_timestamp:
                # Use specific timestamp for incremental processing
                if isinstance(since_timestamp, str):
                    since_timestamp_str = since_timestamp
                else:
                    since_timestamp_str = since_timestamp.isoformat()
                
                where_clause = f"WHERE RECORD_METADATA:CreateTime::TIMESTAMP > '{since_timestamp_str}'"
            else:
                # Default to last hour if no incremental timestamp provided
                where_clause = "WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())"
            
            query = f"""
                SELECT 
                    RECORD_CONTENT:symbol::STRING as symbol,
                    RECORD_CONTENT:timestamp::TIMESTAMP as timestamp,
                    RECORD_CONTENT:open::FLOAT as open_price,
                    RECORD_CONTENT:high::FLOAT as high_price,
                    RECORD_CONTENT:low::FLOAT as low_price,
                    RECORD_CONTENT:close::FLOAT as close_price,
                    RECORD_CONTENT:volume::INTEGER as volume,
                    RECORD_CONTENT:adjusted_close::FLOAT as adjusted_close,
                    RECORD_CONTENT:dividend_amount::FLOAT as dividend_amount,
                    RECORD_CONTENT:split_coefficient::FLOAT as split_coefficient,
                    RECORD_CONTENT:company_name::STRING as company_name,
                    RECORD_CONTENT:sector::STRING as sector,
                    RECORD_CONTENT:industry::STRING as industry,
                    RECORD_CONTENT:exchange::STRING as exchange,
                    RECORD_CONTENT:currency::STRING as currency,
                    RECORD_CONTENT:country::STRING as country,
                    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_timestamp
                FROM {self.config['schema']}.{self.staging_tables['stock_prices']}
                {where_clause}
                AND RECORD_CONTENT:symbol IS NOT NULL
                AND RECORD_CONTENT:timestamp IS NOT NULL
                ORDER BY RECORD_METADATA:CreateTime
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                df = pd.DataFrame(result)
                self.logger.info(f"Extracted {len(df)} stock price records from staging")
                return df
            else:
                self.logger.info("No stock price data found in staging")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting stock prices staging data: {e}")
            return None
    
    def _extract_trading_volume_staging(self) -> Optional[pd.DataFrame]:
        """Extract trading volume data from staging table with incremental processing support."""
        try:
            # Get incremental processing timestamp
            since_timestamp = self.config.get('trading_volume_since')
            
            if since_timestamp:
                # Use specific timestamp for incremental processing
                if isinstance(since_timestamp, str):
                    since_timestamp_str = since_timestamp
                else:
                    since_timestamp_str = since_timestamp.isoformat()
                
                where_clause = f"WHERE RECORD_METADATA:CreateTime::TIMESTAMP > '{since_timestamp_str}'"
            else:
                # Default to last hour if no incremental timestamp provided
                where_clause = "WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())"
            
            query = f"""
                SELECT 
                    RECORD_CONTENT:symbol::STRING as symbol,
                    RECORD_CONTENT:timestamp::TIMESTAMP as timestamp,
                    RECORD_CONTENT:volume::INTEGER as volume,
                    RECORD_CONTENT:volume_weighted_price::FLOAT as volume_weighted_price,
                    RECORD_CONTENT:trade_count::INTEGER as trade_count,
                    RECORD_CONTENT:buy_volume::INTEGER as buy_volume,
                    RECORD_CONTENT:sell_volume::INTEGER as sell_volume,
                    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_timestamp
                FROM {self.config['schema']}.{self.staging_tables['trading_volume']}
                {where_clause}
                AND RECORD_CONTENT:symbol IS NOT NULL
                AND RECORD_CONTENT:timestamp IS NOT NULL
                AND RECORD_CONTENT:volume IS NOT NULL
                ORDER BY RECORD_METADATA:CreateTime
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                df = pd.DataFrame(result)
                self.logger.info(f"Extracted {len(df)} trading volume records from staging")
                return df
            else:
                self.logger.info("No trading volume data found in staging")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting trading volume staging data: {e}")
            return None
    
    def _extract_technical_indicators_staging(self) -> Optional[pd.DataFrame]:
        """Extract technical indicators data from staging table with incremental processing support."""
        try:
            # Get incremental processing timestamp
            since_timestamp = self.config.get('technical_indicators_since')
            
            if since_timestamp:
                # Use specific timestamp for incremental processing
                if isinstance(since_timestamp, str):
                    since_timestamp_str = since_timestamp
                else:
                    since_timestamp_str = since_timestamp.isoformat()
                
                where_clause = f"WHERE RECORD_METADATA:CreateTime::TIMESTAMP > '{since_timestamp_str}'"
            else:
                # Default to last hour if no incremental timestamp provided
                where_clause = "WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())"
            
            query = f"""
                SELECT 
                    RECORD_CONTENT:symbol::STRING as symbol,
                    RECORD_CONTENT:timestamp::TIMESTAMP as timestamp,
                    RECORD_CONTENT:sma_20::FLOAT as sma_20,
                    RECORD_CONTENT:sma_50::FLOAT as sma_50,
                    RECORD_CONTENT:ema_12::FLOAT as ema_12,
                    RECORD_CONTENT:ema_26::FLOAT as ema_26,
                    RECORD_CONTENT:rsi_14::FLOAT as rsi_14,
                    RECORD_CONTENT:macd::FLOAT as macd,
                    RECORD_CONTENT:macd_signal::FLOAT as macd_signal,
                    RECORD_CONTENT:volume_sma_20::INTEGER as volume_sma_20,
                    RECORD_CONTENT:volume_ratio::FLOAT as volume_ratio,
                    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_timestamp
                FROM {self.config['schema']}.{self.staging_tables['technical_indicators']}
                {where_clause}
                AND RECORD_CONTENT:symbol IS NOT NULL
                AND RECORD_CONTENT:timestamp IS NOT NULL
                ORDER BY RECORD_METADATA:CreateTime
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                df = pd.DataFrame(result)
                self.logger.info(f"Extracted {len(df)} technical indicator records from staging")
                return df
            else:
                self.logger.info("No technical indicators data found in staging")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting technical indicators staging data: {e}")
            return None
    
    def _transform_to_dimensional(self, parsed_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
        """
        Transform parsed staging data into dimensional model format.
        
        Args:
            parsed_data: Dictionary of parsed DataFrames
            
        Returns:
            Dictionary of dimensional tables ready for loading
        """
        dimensional_data = {}
        
        # Combine all data sources for comprehensive transformation
        stock_data = parsed_data.get('stock_prices')
        volume_data = parsed_data.get('trading_volume')
        indicators_data = parsed_data.get('technical_indicators')
        
        if stock_data is not None and not stock_data.empty:
            # Merge technical indicators if available
            if indicators_data is not None and not indicators_data.empty:
                stock_data = stock_data.merge(
                    indicators_data[['symbol', 'timestamp', 'sma_20', 'sma_50', 'ema_12', 'ema_26', 
                                   'rsi_14', 'macd', 'macd_signal', 'volume_sma_20', 'volume_ratio']],
                    on=['symbol', 'timestamp'],
                    how='left'
                )
            
            # Transform company dimension data
            dimensional_data['dim_company'] = self._transform_company_dimension(stock_data)
            
            # Transform fact stock prices
            dimensional_data['fact_stock_prices'] = self._transform_fact_stock_prices(stock_data)
        
        # Transform trading volume facts if available
        if volume_data is not None and not volume_data.empty:
            dimensional_data['fact_trading_volume'] = self._transform_fact_trading_volume(volume_data)
        
        return dimensional_data
    
    def _transform_company_dimension(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Transform company data for dimension table."""
        # Get unique company information
        company_data = stock_data[['symbol', 'company_name', 'sector', 'industry', 
                                 'exchange', 'currency', 'country']].drop_duplicates()
        
        # Add dimension metadata
        company_data['effective_date'] = datetime.now().date()
        company_data['expiry_date'] = None
        company_data['is_current'] = True
        company_data['created_at'] = datetime.now()
        company_data['updated_at'] = datetime.now()
        
        return company_data
    
    def _transform_fact_stock_prices(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Transform stock price data for fact table."""
        # Select and rename columns for fact table
        fact_data = stock_data.copy()
        
        # Add fact table metadata
        fact_data['data_source'] = self.config['data_source']
        fact_data['processing_timestamp'] = datetime.now()
        
        # Ensure required columns exist with defaults
        required_columns = {
            'dividend_amount': 0.0,
            'split_coefficient': 1.0,
            'sma_20': None,
            'sma_50': None,
            'ema_12': None,
            'ema_26': None,
            'rsi_14': None,
            'macd': None,
            'macd_signal': None
        }
        
        for col, default_val in required_columns.items():
            if col not in fact_data.columns:
                fact_data[col] = default_val
        
        return fact_data
    
    def _transform_fact_trading_volume(self, volume_data: pd.DataFrame) -> pd.DataFrame:
        """Transform trading volume data for fact table."""
        fact_data = volume_data.copy()
        
        # Add fact table metadata
        fact_data['data_source'] = self.config['data_source']
        fact_data['processing_timestamp'] = datetime.now()
        
        # Ensure required columns exist with defaults
        if 'volume_sma_20' not in fact_data.columns:
            fact_data['volume_sma_20'] = None
        if 'volume_ratio' not in fact_data.columns:
            fact_data['volume_ratio'] = None
        
        return fact_data
    
    def _load_to_snowflake(self, dimensional_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Load dimensional data into Snowflake tables.
        
        Args:
            dimensional_data: Dictionary of dimensional tables
            
        Returns:
            Dictionary with load results
        """
        load_results = {}
        
        # Load dimension tables first (for referential integrity)
        if 'dim_company' in dimensional_data:
            load_results['dim_company'] = self._load_company_dimension(dimensional_data['dim_company'])
        
        # Load fact tables
        if 'fact_stock_prices' in dimensional_data:
            load_results['fact_stock_prices'] = self._load_fact_stock_prices(dimensional_data['fact_stock_prices'])
        
        if 'fact_trading_volume' in dimensional_data:
            load_results['fact_trading_volume'] = self._load_fact_trading_volume(dimensional_data['fact_trading_volume'])
        
        return load_results
    
    def _load_company_dimension(self, company_data: pd.DataFrame) -> Dict[str, Any]:
        """Load company dimension with SCD Type 2 logic."""
        try:
            records_inserted = 0
            records_updated = 0
            
            # Get existing company data for SCD Type 2 processing
            existing_companies = self._get_existing_companies()
            
            if self.config['enable_scd_type2'] and existing_companies is not None and not existing_companies.empty:
                # Apply SCD Type 2 logic
                insert_data, update_data = self._apply_scd_type2_companies(company_data, existing_companies)
                
                # Update expired records first
                if not update_data.empty:
                    records_updated = self._update_expired_companies(update_data)
                
                # Insert new/changed records
                if not insert_data.empty:
                    records_inserted = self._insert_new_companies(insert_data)
                    
            else:
                # Simple insert for new companies (first time setup)
                if not company_data.empty:
                    records_inserted = self._insert_new_companies(company_data)
            
            return {
                'status': 'success',
                'records_inserted': records_inserted,
                'records_updated': records_updated,
                'table': self.dimensional_tables['dim_company']
            }
            
        except Exception as e:
            self.logger.error(f"Error loading company dimension: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'table': self.dimensional_tables['dim_company']
            }
    
    def _load_fact_stock_prices(self, stock_data: pd.DataFrame) -> Dict[str, Any]:
        """Load fact stock prices with dimension key lookups and deduplication."""
        try:
            self.logger.info(f"Loading {len(stock_data)} stock price records to fact table")
            
            # Get dimension keys - join with dimension tables to get proper foreign keys
            enriched_data = self._enrich_with_dimension_keys(stock_data, 'stock_prices')
            
            if enriched_data.empty:
                self.logger.warning("No records enriched with dimension keys")
                return {
                    'status': 'success',
                    'records_loaded': 0,
                    'records_skipped': len(stock_data),
                    'skipped_reason': 'no_dimension_keys',
                    'table': self.dimensional_tables['fact_stock_prices']
                }
            
            self.logger.info(f"Enriched {len(enriched_data)} records with dimension keys")
            
            # Implement business key checking to prevent duplicate fact records
            deduplicated_data = self._check_duplicate_facts(enriched_data, 'stock_prices')
            
            records_loaded = 0
            records_skipped = len(enriched_data) - len(deduplicated_data)
            
            if not deduplicated_data.empty:
                # Prepare data for insertion - ensure column order and types
                insert_data = self._prepare_fact_stock_prices_data(deduplicated_data)
                
                # Load parsed stock price data into existing FACT_STOCK_PRICES table
                self.snowflake_client.bulk_insert_from_dataframe(
                    insert_data,
                    self.dimensional_tables['fact_stock_prices'],
                    schema=self.config['schema'],
                    if_exists='append'
                )
                records_loaded = len(insert_data)
                self.logger.info(f"Successfully loaded {records_loaded} stock price records")
            else:
                self.logger.info("All records were duplicates, no new data loaded")
            
            return {
                'status': 'success',
                'records_loaded': records_loaded,
                'records_skipped': records_skipped,
                'duplicate_records_filtered': records_skipped,
                'table': self.dimensional_tables['fact_stock_prices']
            }
            
        except Exception as e:
            self.logger.error(f"Error loading fact stock prices: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'table': self.dimensional_tables['fact_stock_prices']
            }
    
    def _load_fact_trading_volume(self, volume_data: pd.DataFrame) -> Dict[str, Any]:
        """Load fact trading volume with dimension key lookups and deduplication."""
        try:
            self.logger.info(f"Loading {len(volume_data)} trading volume records to fact table")
            
            # Join staging data with dimension tables to get proper foreign keys
            enriched_data = self._enrich_with_dimension_keys(volume_data, 'trading_volume')
            
            if enriched_data.empty:
                self.logger.warning("No records enriched with dimension keys")
                return {
                    'status': 'success',
                    'records_loaded': 0,
                    'records_skipped': len(volume_data),
                    'skipped_reason': 'no_dimension_keys',
                    'table': self.dimensional_tables['fact_trading_volume']
                }
            
            self.logger.info(f"Enriched {len(enriched_data)} records with dimension keys")
            
            # Implement business key checking to prevent duplicate fact records
            deduplicated_data = self._check_duplicate_facts(enriched_data, 'trading_volume')
            
            records_loaded = 0
            records_skipped = len(enriched_data) - len(deduplicated_data)
            
            if not deduplicated_data.empty:
                # Prepare data for insertion - ensure column order and types
                insert_data = self._prepare_fact_trading_volume_data(deduplicated_data)
                
                # Load trading volume data into existing FACT_TRADING_VOLUME table
                self.snowflake_client.bulk_insert_from_dataframe(
                    insert_data,
                    self.dimensional_tables['fact_trading_volume'],
                    schema=self.config['schema'],
                    if_exists='append'
                )
                records_loaded = len(insert_data)
                self.logger.info(f"Successfully loaded {records_loaded} trading volume records")
            else:
                self.logger.info("All records were duplicates, no new data loaded")
            
            return {
                'status': 'success',
                'records_loaded': records_loaded,
                'records_skipped': records_skipped,
                'duplicate_records_filtered': records_skipped,
                'table': self.dimensional_tables['fact_trading_volume']
            }
            
        except Exception as e:
            self.logger.error(f"Error loading fact trading volume: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'table': self.dimensional_tables['fact_trading_volume']
            }
    
    def _get_existing_companies(self) -> Optional[pd.DataFrame]:
        """Get existing company dimension data for SCD processing."""
        try:
            query = f"""
                SELECT 
                    COMPANY_KEY,
                    SYMBOL,
                    COMPANY_NAME,
                    SECTOR,
                    INDUSTRY,
                    MARKET_CAP_CATEGORY,
                    EXCHANGE,
                    CURRENCY,
                    COUNTRY,
                    EFFECTIVE_DATE,
                    EXPIRY_DATE,
                    IS_CURRENT,
                    CREATED_AT,
                    UPDATED_AT
                FROM {self.config['schema']}.{self.dimensional_tables['dim_company']}
                WHERE IS_CURRENT = TRUE
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                return pd.DataFrame(result)
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"Could not get existing companies: {e}")
            return None
    
    def _apply_scd_type2_companies(self, new_data: pd.DataFrame, existing_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply SCD Type 2 logic for company dimension.
        
        Args:
            new_data: New company data from staging
            existing_data: Current company dimension data
            
        Returns:
            Tuple of (records_to_insert, records_to_update)
        """
        current_date = datetime.now().date()
        records_to_insert = []
        records_to_update = []
        
        # Create lookup for existing companies
        existing_lookup = {}
        for _, row in existing_data.iterrows():
            existing_lookup[row['SYMBOL']] = row
        
        # Process each new company record
        for _, new_row in new_data.iterrows():
            symbol = new_row['symbol']
            
            if symbol not in existing_lookup:
                # New company - create new record
                new_record = new_row.copy()
                new_record['effective_date'] = current_date
                new_record['expiry_date'] = None
                new_record['is_current'] = True
                new_record['created_at'] = datetime.now()
                new_record['updated_at'] = datetime.now()
                records_to_insert.append(new_record)
                
            else:
                # Existing company - check for changes
                existing_row = existing_lookup[symbol]
                
                # Define SCD Type 2 columns (columns that trigger new version)
                scd_columns = ['company_name', 'sector', 'industry', 'exchange', 'currency', 'country']
                
                # Check if any SCD columns have changed
                has_changes = False
                for col in scd_columns:
                    new_val = new_row.get(col)
                    existing_val = existing_row.get(col.upper())  # Snowflake returns uppercase
                    
                    # Handle None/null comparisons
                    if pd.isna(new_val) and pd.isna(existing_val):
                        continue
                    elif pd.isna(new_val) or pd.isna(existing_val):
                        has_changes = True
                        break
                    elif str(new_val).strip() != str(existing_val).strip():
                        has_changes = True
                        break
                
                if has_changes:
                    # Expire the existing record
                    expired_record = existing_row.copy()
                    expired_record['expiry_date'] = current_date
                    expired_record['is_current'] = False
                    expired_record['updated_at'] = datetime.now()
                    records_to_update.append(expired_record)
                    
                    # Create new current record
                    new_record = new_row.copy()
                    new_record['effective_date'] = current_date
                    new_record['expiry_date'] = None
                    new_record['is_current'] = True
                    new_record['created_at'] = datetime.now()
                    new_record['updated_at'] = datetime.now()
                    records_to_insert.append(new_record)
        
        insert_df = pd.DataFrame(records_to_insert) if records_to_insert else pd.DataFrame()
        update_df = pd.DataFrame(records_to_update) if records_to_update else pd.DataFrame()
        
        self.logger.info(f"SCD Type 2 processing: {len(records_to_insert)} new/changed companies, {len(records_to_update)} records to expire")
        
        return insert_df, update_df
    
    def _enrich_with_dimension_keys(self, fact_data: pd.DataFrame, fact_type: str) -> pd.DataFrame:
        """
        Enrich fact data with dimension keys from dimension tables.
        
        This method joins staging data with dimension tables to get proper foreign keys
        for the fact table relationships.
        
        Args:
            fact_data: Fact data to enrich
            fact_type: Type of fact data ('stock_prices' or 'trading_volume')
            
        Returns:
            DataFrame enriched with dimension keys (COMPANY_KEY, DATE_KEY, TIME_KEY)
        """
        enriched_data = fact_data.copy()
        
        try:
            self.logger.info(f"Enriching {len(fact_data)} {fact_type} records with dimension keys")
            
            # Join with company dimension to get COMPANY_KEY
            company_keys = self._get_company_keys()
            if company_keys is not None and not company_keys.empty:
                initial_count = len(enriched_data)
                enriched_data = enriched_data.merge(
                    company_keys[['SYMBOL', 'COMPANY_KEY']],
                    left_on='symbol',
                    right_on='SYMBOL',
                    how='inner'  # Inner join to only keep records with valid company keys
                )
                
                if len(enriched_data) == 0:
                    self.logger.warning("No matching company keys found - all records filtered out")
                    return enriched_data
                elif len(enriched_data) < initial_count:
                    filtered_count = initial_count - len(enriched_data)
                    self.logger.warning(f"Filtered out {filtered_count} records without matching company keys")
                    
                # Drop the duplicate SYMBOL column
                enriched_data = enriched_data.drop('SYMBOL', axis=1)
                self.logger.info(f"Successfully joined with company dimension: {len(enriched_data)} records")
            else:
                self.logger.error("No company keys available - cannot enrich fact data")
                return pd.DataFrame()  # Return empty DataFrame
            
            # Join with date dimension to get DATE_KEY
            date_keys = self._get_date_keys(enriched_data['timestamp'])
            if date_keys is not None and not date_keys.empty:
                # Create date key lookup based on date portion of timestamp
                enriched_data['temp_date_key'] = enriched_data['timestamp'].dt.strftime('%Y%m%d').astype(int)
                initial_count = len(enriched_data)
                enriched_data = enriched_data.merge(
                    date_keys[['DATE_KEY']],
                    left_on='temp_date_key',
                    right_on='DATE_KEY',
                    how='inner'  # Inner join to ensure valid date keys
                )
                enriched_data = enriched_data.drop('temp_date_key', axis=1)
                
                if len(enriched_data) < initial_count:
                    filtered_count = initial_count - len(enriched_data)
                    self.logger.warning(f"Filtered out {filtered_count} records without matching date keys")
                    
                self.logger.info(f"Successfully joined with date dimension: {len(enriched_data)} records")
            else:
                # Fallback to calculated date keys if dimension table lookup fails
                enriched_data['DATE_KEY'] = enriched_data['timestamp'].dt.strftime('%Y%m%d').astype(int)
                self.logger.warning("Using calculated date keys - date dimension lookup failed")
            
            # Join with time dimension to get TIME_KEY
            time_keys = self._get_time_keys(enriched_data['timestamp'])
            if time_keys is not None and not time_keys.empty:
                # Create time key lookup based on hour and minute
                enriched_data['temp_time_key'] = (enriched_data['timestamp'].dt.hour * 100 + 
                                                enriched_data['timestamp'].dt.minute)
                initial_count = len(enriched_data)
                enriched_data = enriched_data.merge(
                    time_keys[['TIME_KEY']],
                    left_on='temp_time_key',
                    right_on='TIME_KEY',
                    how='inner'  # Inner join to ensure valid time keys
                )
                enriched_data = enriched_data.drop('temp_time_key', axis=1)
                
                if len(enriched_data) < initial_count:
                    filtered_count = initial_count - len(enriched_data)
                    self.logger.warning(f"Filtered out {filtered_count} records without matching time keys")
                    
                self.logger.info(f"Successfully joined with time dimension: {len(enriched_data)} records")
            else:
                # Fallback to calculated time keys if dimension table lookup fails
                enriched_data['TIME_KEY'] = (enriched_data['timestamp'].dt.hour * 100 + 
                                           enriched_data['timestamp'].dt.minute)
                self.logger.warning("Using calculated time keys - time dimension lookup failed")
            
            # Verify all required dimension keys are present
            required_keys = ['COMPANY_KEY', 'DATE_KEY', 'TIME_KEY']
            missing_keys = [key for key in required_keys if key not in enriched_data.columns]
            
            if missing_keys:
                self.logger.error(f"Missing required dimension keys: {missing_keys}")
                return pd.DataFrame()
            
            self.logger.info(f"Successfully enriched {len(enriched_data)} {fact_type} records with all dimension keys")
            return enriched_data
            
        except Exception as e:
            self.logger.error(f"Error enriching fact data with dimension keys: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def _insert_new_companies(self, company_data: pd.DataFrame) -> int:
        """Insert new company records."""
        try:
            # Get next available company key
            max_key_query = f"""
                SELECT COALESCE(MAX(COMPANY_KEY), 0) as max_key
                FROM {self.config['schema']}.{self.dimensional_tables['dim_company']}
            """
            
            result = self.snowflake_client.execute_query(max_key_query, fetch=True)
            next_key = (result[0]['MAX_KEY'] if result else 0) + 1
            
            # Prepare data for insertion
            insert_data = company_data.copy()
            insert_data['company_key'] = range(next_key, next_key + len(insert_data))
            
            # Ensure column order matches table schema
            column_order = [
                'company_key', 'symbol', 'company_name', 'sector', 'industry',
                'market_cap_category', 'exchange', 'currency', 'country',
                'effective_date', 'expiry_date', 'is_current', 'created_at', 'updated_at'
            ]
            
            # Add missing columns with defaults
            for col in column_order:
                if col not in insert_data.columns:
                    if col == 'market_cap_category':
                        insert_data[col] = None
                    elif col == 'expiry_date':
                        insert_data[col] = None
                    elif col == 'is_current':
                        insert_data[col] = True
                    elif col in ['created_at', 'updated_at']:
                        insert_data[col] = datetime.now()
                    elif col == 'effective_date':
                        insert_data[col] = datetime.now().date()
            
            # Reorder columns
            insert_data = insert_data[column_order]
            
            # Insert using bulk insert
            self.snowflake_client.bulk_insert_from_dataframe(
                insert_data,
                self.dimensional_tables['dim_company'],
                schema=self.config['schema'],
                if_exists='append'
            )
            
            self.logger.info(f"Inserted {len(insert_data)} new company records")
            return len(insert_data)
            
        except Exception as e:
            self.logger.error(f"Error inserting new companies: {e}")
            raise
    
    def _update_expired_companies(self, update_data: pd.DataFrame) -> int:
        """Update expired company records."""
        try:
            records_updated = 0
            
            for _, row in update_data.iterrows():
                update_query = f"""
                    UPDATE {self.config['schema']}.{self.dimensional_tables['dim_company']}
                    SET 
                        EXPIRY_DATE = %(expiry_date)s,
                        IS_CURRENT = %(is_current)s,
                        UPDATED_AT = %(updated_at)s
                    WHERE COMPANY_KEY = %(company_key)s
                """
                
                params = {
                    'expiry_date': row['expiry_date'],
                    'is_current': row['is_current'],
                    'updated_at': row['updated_at'],
                    'company_key': row['COMPANY_KEY']
                }
                
                self.snowflake_client.execute_query(update_query, params=params)
                records_updated += 1
            
            self.logger.info(f"Updated {records_updated} expired company records")
            return records_updated
            
        except Exception as e:
            self.logger.error(f"Error updating expired companies: {e}")
            raise
    
    def _get_company_keys(self) -> Optional[pd.DataFrame]:
        """Get company dimension keys for fact table joins."""
        try:
            query = f"""
                SELECT COMPANY_KEY, SYMBOL
                FROM {self.config['schema']}.{self.dimensional_tables['dim_company']}
                WHERE IS_CURRENT = TRUE
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                return pd.DataFrame(result)
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"Could not get company keys: {e}")
            return None
    
    def _get_date_keys(self, dates: pd.Series) -> Optional[pd.DataFrame]:
        """
        Get date dimension keys for fact table joins.
        
        Args:
            dates: Series of dates to lookup
            
        Returns:
            DataFrame with date keys
        """
        try:
            # Convert dates to YYYYMMDD format for lookup
            date_keys = dates.dt.strftime('%Y%m%d').astype(int).unique()
            
            # Create comma-separated list for IN clause
            date_key_list = ','.join(map(str, date_keys))
            
            query = f"""
                SELECT DATE_KEY, DATE_VALUE
                FROM {self.config['schema']}.{self.dimensional_tables['dim_date']}
                WHERE DATE_KEY IN ({date_key_list})
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                return pd.DataFrame(result)
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"Could not get date keys: {e}")
            return None
    
    def _get_time_keys(self, timestamps: pd.Series) -> Optional[pd.DataFrame]:
        """
        Get time dimension keys for fact table joins.
        
        Args:
            timestamps: Series of timestamps to lookup
            
        Returns:
            DataFrame with time keys
        """
        try:
            # Convert timestamps to HHMM format for lookup
            time_keys = (timestamps.dt.hour * 100 + timestamps.dt.minute).unique()
            
            # Create comma-separated list for IN clause
            time_key_list = ','.join(map(str, time_keys))
            
            query = f"""
                SELECT TIME_KEY, HOUR, MINUTE
                FROM {self.config['schema']}.{self.dimensional_tables['dim_time']}
                WHERE TIME_KEY IN ({time_key_list})
            """
            
            result = self.snowflake_client.execute_query(query, fetch=True)
            
            if result:
                return pd.DataFrame(result)
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"Could not get time keys: {e}")
            return None
    
    def _check_duplicate_facts(self, fact_data: pd.DataFrame, fact_type: str) -> pd.DataFrame:
        """
        Check for and remove duplicate fact records based on business keys.
        
        This method implements business key checking to prevent duplicate fact records
        by comparing incoming data against existing records in the fact tables.
        
        Args:
            fact_data: Fact data to check for duplicates
            fact_type: Type of fact data ('stock_prices' or 'trading_volume')
            
        Returns:
            DataFrame with duplicates removed
        """
        try:
            if fact_data.empty:
                return fact_data
            
            # Define business keys for each fact type - these uniquely identify a fact record
            business_keys = {
                'stock_prices': ['COMPANY_KEY', 'DATE_KEY', 'TIME_KEY'],
                'trading_volume': ['COMPANY_KEY', 'DATE_KEY', 'TIME_KEY']
            }
            
            if fact_type not in business_keys:
                self.logger.warning(f"Unknown fact type {fact_type}, skipping duplicate check")
                return fact_data
            
            keys = business_keys[fact_type]
            table_name = self.dimensional_tables[f'fact_{fact_type}']
            
            # Verify all business key columns are present
            missing_keys = [key for key in keys if key not in fact_data.columns]
            if missing_keys:
                self.logger.warning(f"Missing business key columns {missing_keys}, skipping duplicate check")
                return fact_data
            
            self.logger.info(f"Checking for duplicates using business keys: {keys}")
            
            # Create a list of business key combinations to check
            key_combinations = []
            for _, row in fact_data.iterrows():
                key_combo = tuple(row[key] for key in keys)
                key_combinations.append(key_combo)
            
            if not key_combinations:
                return fact_data
            
            # Build efficient query to check for existing records in batches
            batch_size = 100  # Process in batches to avoid query size limits
            existing_keys = set()
            
            for i in range(0, len(key_combinations), batch_size):
                batch = key_combinations[i:i + batch_size]
                
                # Build query conditions for this batch
                conditions = []
                for combo in batch:
                    condition_parts = []
                    for key, value in zip(keys, combo):
                        # Handle NULL values properly
                        if pd.isna(value) or value is None:
                            condition_parts.append(f"{key} IS NULL")
                        else:
                            condition_parts.append(f"{key} = {value}")
                    conditions.append(f"({' AND '.join(condition_parts)})")
                
                if conditions:
                    where_clause = " OR ".join(conditions)
                    
                    check_query = f"""
                        SELECT {', '.join(keys)}
                        FROM {self.config['schema']}.{table_name}
                        WHERE {where_clause}
                    """
                    
                    try:
                        existing_records = self.snowflake_client.execute_query(check_query, fetch=True)
                        
                        if existing_records:
                            # Add to existing keys set
                            for record in existing_records:
                                key_tuple = tuple(record[key] for key in keys)
                                existing_keys.add(key_tuple)
                                
                    except Exception as e:
                        self.logger.warning(f"Error checking batch for duplicates: {e}")
                        continue
            
            # Filter out duplicates if any were found
            if existing_keys:
                def is_not_duplicate(row):
                    key_tuple = tuple(row[key] for key in keys)
                    return key_tuple not in existing_keys
                
                filtered_data = fact_data[fact_data.apply(is_not_duplicate, axis=1)]
                
                duplicates_removed = len(fact_data) - len(filtered_data)
                if duplicates_removed > 0:
                    self.logger.info(f"Filtered out {duplicates_removed} duplicate {fact_type} records based on business keys")
                else:
                    self.logger.info(f"No duplicate {fact_type} records found")
                
                return filtered_data
            else:
                self.logger.info(f"No existing records found, all {len(fact_data)} {fact_type} records are new")
                return fact_data
            
        except Exception as e:
            self.logger.error(f"Error checking for duplicate facts: {e}")
            # Return original data if duplicate check fails to avoid data loss
            self.logger.warning("Duplicate check failed, proceeding with all records")
            return fact_data
    
    def _update_processing_metadata(self, load_results: Dict[str, Any]) -> None:
        """Update processing metadata and logs."""
        try:
            # Check if ETL_PROCESSING_LOG table exists, create if not
            log_table_exists = self.snowflake_client.check_table_exists('ETL_PROCESSING_LOG', self.config['schema'])
            
            if not log_table_exists:
                create_log_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {self.config['schema']}.ETL_PROCESSING_LOG (
                        LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
                        OPERATION_TYPE VARCHAR(50),
                        STATUS VARCHAR(20),
                        PROCESSING_TIMESTAMP TIMESTAMP_NTZ,
                        DETAILS VARIANT,
                        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                """
                self.snowflake_client.execute_query(create_log_table_query)
                self.logger.info("Created ETL_PROCESSING_LOG table")
            
            # Log ETL operation
            log_query = f"""
                INSERT INTO {self.config['schema']}.ETL_PROCESSING_LOG 
                (OPERATION_TYPE, STATUS, PROCESSING_TIMESTAMP, DETAILS)
                VALUES (%(operation_type)s, %(status)s, %(timestamp)s, %(details)s)
            """
            
            overall_status = 'SUCCESS' if all(
                result.get('status') == 'success' for result in load_results.values()
            ) else 'PARTIAL_SUCCESS'
            
            self.snowflake_client.execute_query(
                log_query,
                params={
                    'operation_type': 'dimensional_etl',
                    'status': overall_status,
                    'timestamp': datetime.now(timezone.utc),
                    'details': json.dumps(load_results, default=str)
                }
            )
            
        except Exception as e:
            self.logger.warning(f"Could not update processing metadata: {e}")
    
    def _prepare_fact_stock_prices_data(self, enriched_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare stock prices data for insertion into fact table."""
        try:
            # Define expected columns in correct order
            fact_columns = [
                'COMPANY_KEY', 'DATE_KEY', 'TIME_KEY',
                'open_price', 'high_price', 'low_price', 'close_price', 'volume',
                'adjusted_close', 'dividend_amount', 'split_coefficient',
                'sma_20', 'sma_50', 'ema_12', 'ema_26', 'rsi_14', 'macd', 'macd_signal',
                'data_source', 'ingestion_timestamp', 'processing_timestamp'
            ]
            
            # Prepare the data
            insert_data = enriched_data.copy()
            
            # Ensure all required columns exist with proper defaults
            column_defaults = {
                'dividend_amount': 0.0,
                'split_coefficient': 1.0,
                'sma_20': None,
                'sma_50': None,
                'ema_12': None,
                'ema_26': None,
                'rsi_14': None,
                'macd': None,
                'macd_signal': None,
                'data_source': self.config['data_source'],
                'processing_timestamp': datetime.now()
            }
            
            for col, default_val in column_defaults.items():
                if col not in insert_data.columns:
                    insert_data[col] = default_val
            
            # Select and order columns
            available_columns = [col for col in fact_columns if col in insert_data.columns]
            insert_data = insert_data[available_columns]
            
            return insert_data
            
        except Exception as e:
            self.logger.error(f"Error preparing stock prices data: {e}")
            raise
    
    def _prepare_fact_trading_volume_data(self, enriched_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare trading volume data for insertion into fact table."""
        try:
            # Define expected columns in correct order
            fact_columns = [
                'COMPANY_KEY', 'DATE_KEY', 'TIME_KEY',
                'volume', 'volume_weighted_price', 'trade_count', 'buy_volume', 'sell_volume',
                'volume_sma_20', 'volume_ratio',
                'data_source', 'ingestion_timestamp', 'processing_timestamp'
            ]
            
            # Prepare the data
            insert_data = enriched_data.copy()
            
            # Ensure all required columns exist with proper defaults
            column_defaults = {
                'volume_weighted_price': None,
                'trade_count': None,
                'buy_volume': None,
                'sell_volume': None,
                'volume_sma_20': None,
                'volume_ratio': None,
                'data_source': self.config['data_source'],
                'processing_timestamp': datetime.now()
            }
            
            for col, default_val in column_defaults.items():
                if col not in insert_data.columns:
                    insert_data[col] = default_val
            
            # Select and order columns
            available_columns = [col for col in fact_columns if col in insert_data.columns]
            insert_data = insert_data[available_columns]
            
            return insert_data
            
        except Exception as e:
            self.logger.error(f"Error preparing trading volume data: {e}")
            raise
    
    def run_automated_etl(self) -> Dict[str, Any]:
        """
        Main entry point for automated ETL execution.
        
        This method can be called by schedulers or triggers to run the complete
        ETL process from staging tables to dimensional tables.
        
        Returns:
            Dictionary with ETL execution results
        """
        self.logger.info("Starting automated dimensional ETL run")
        
        start_time = datetime.now(timezone.utc)
        
        try:
            # Run the main ETL process
            result = self.process_staging_data()
            
            # Add execution metadata
            result['execution_time_seconds'] = (datetime.now(timezone.utc) - start_time).total_seconds()
            result['etl_run_id'] = f"etl_{start_time.strftime('%Y%m%d_%H%M%S')}"
            
            self.logger.info(f"Automated ETL run completed: {result['status']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Automated ETL run failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "execution_time_seconds": (datetime.now(timezone.utc) - start_time).total_seconds(),
                "etl_run_id": f"etl_{start_time.strftime('%Y%m%d_%H%M%S')}"
            }
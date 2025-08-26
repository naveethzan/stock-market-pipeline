"""
Staging Data Parser for Kafka Connect VARIANT Columns

This module provides parsing and validation functionality for extracting JSON data
from Kafka Connect staging tables in Snowflake. It handles VARIANT column parsing,
data validation, and format standardization for downstream ETL processing.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone
import json
import pandas as pd
from dataclasses import dataclass

from .snowflake_client import SnowflakeClient
from ..config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class StagingRecord:
    """Represents a parsed staging record with metadata"""
    symbol: str
    timestamp: datetime
    data: Dict[str, Any]
    ingestion_timestamp: datetime
    record_source: str
    is_valid: bool = True
    validation_errors: List[str] = None
    
    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []


@dataclass
class ParsedStagingData:
    """Container for parsed staging data with validation results"""
    records: List[StagingRecord]
    total_records: int
    valid_records: int
    invalid_records: int
    parsing_errors: List[str]
    
    @property
    def success_rate(self) -> float:
        """Calculate parsing success rate"""
        return (self.valid_records / self.total_records) if self.total_records > 0 else 0.0


class StagingDataParser:
    """
    Parser for extracting and validating JSON data from Kafka Connect staging tables.
    
    This class handles parsing RECORD_CONTENT from VARIANT columns in staging tables,
    validates data formats, and prepares structured data for dimensional ETL processing.
    """
    
    def __init__(self, snowflake_client: Optional[SnowflakeClient] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize staging data parser
        
        Args:
            snowflake_client: Optional SnowflakeClient instance
            config: Optional configuration dict
        """
        self.settings = get_settings()
        self.snowflake_client = snowflake_client or SnowflakeClient()
        self.config = config or self._get_parser_config()
        self.logger = logging.getLogger(__name__)
        
        # Define required fields for each data type
        self.required_fields = {
            'stock_prices': ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume'],
            'trading_volume': ['symbol', 'timestamp', 'volume'],
            'technical_indicators': ['symbol', 'timestamp'],
            'company_metadata': ['symbol']
        }
        
        # Define optional fields with defaults
        self.optional_fields = {
            'stock_prices': {
                'adjusted_close': None,
                'dividend_amount': 0.0,
                'split_coefficient': 1.0,
                'company_name': None,
                'sector': None,
                'industry': None,
                'exchange': None,
                'currency': 'USD',
                'country': None
            },
            'trading_volume': {
                'volume_weighted_price': None,
                'trade_count': None,
                'buy_volume': None,
                'sell_volume': None
            },
            'technical_indicators': {
                'sma_20': None,
                'sma_50': None,
                'ema_12': None,
                'ema_26': None,
                'rsi_14': None,
                'macd': None,
                'macd_signal': None,
                'volume_sma_20': None,
                'volume_ratio': None
            }
        }
    
    def _get_parser_config(self) -> Dict[str, Any]:
        """Get parser configuration"""
        return {
            "schema": self.settings.snowflake_schema or "STREAMING",
            "batch_size": 1000,
            "max_parsing_errors": 100,
            "strict_validation": True,
            "default_lookback_hours": 1
        }
    
    def parse_stock_price_staging(self, lookback_hours: Optional[int] = None) -> ParsedStagingData:
        """
        Extract and parse stock price data from FACT_STOCK_PRICES_STAGING table.
        
        Args:
            lookback_hours: Hours to look back for new data (default from config)
            
        Returns:
            ParsedStagingData with stock price records
        """
        lookback = lookback_hours or self.config['default_lookback_hours']
        
        self.logger.info(f"Parsing stock price staging data (lookback: {lookback} hours)")
        
        try:
            # Query staging table for stock price data
            query = f"""
                SELECT 
                    RECORD_CONTENT,
                    RECORD_METADATA,
                    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_timestamp
                FROM {self.config['schema']}.FACT_STOCK_PRICES_STAGING
                WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -{lookback}, CURRENT_TIMESTAMP())
                ORDER BY RECORD_METADATA:CreateTime
            """
            
            raw_results = self.snowflake_client.execute_query(query, fetch=True)
            
            if not raw_results:
                self.logger.info("No stock price staging data found")
                return ParsedStagingData([], 0, 0, 0, [])
            
            # Parse each record
            parsed_records = []
            parsing_errors = []
            
            for row in raw_results:
                try:
                    record = self._parse_stock_price_record(
                        row['RECORD_CONTENT'],
                        row['RECORD_METADATA'],
                        row['INGESTION_TIMESTAMP']
                    )
                    parsed_records.append(record)
                    
                except Exception as e:
                    error_msg = f"Error parsing stock price record: {e}"
                    parsing_errors.append(error_msg)
                    self.logger.warning(error_msg)
                    
                    if len(parsing_errors) >= self.config['max_parsing_errors']:
                        self.logger.error("Maximum parsing errors reached, stopping")
                        break
            
            # Calculate statistics
            valid_records = sum(1 for r in parsed_records if r.is_valid)
            invalid_records = len(parsed_records) - valid_records
            
            result = ParsedStagingData(
                records=parsed_records,
                total_records=len(raw_results),
                valid_records=valid_records,
                invalid_records=invalid_records,
                parsing_errors=parsing_errors
            )
            
            self.logger.info(f"Parsed stock price data: {valid_records}/{len(raw_results)} valid records")
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing stock price staging data: {e}")
            return ParsedStagingData([], 0, 0, 0, [str(e)])
    
    def parse_trading_volume_staging(self, lookback_hours: Optional[int] = None) -> ParsedStagingData:
        """
        Extract and parse trading volume data from FACT_TRADING_VOLUME_STAGING table.
        
        Args:
            lookback_hours: Hours to look back for new data (default from config)
            
        Returns:
            ParsedStagingData with trading volume records
        """
        lookback = lookback_hours or self.config['default_lookback_hours']
        
        self.logger.info(f"Parsing trading volume staging data (lookback: {lookback} hours)")
        
        try:
            query = f"""
                SELECT 
                    RECORD_CONTENT,
                    RECORD_METADATA,
                    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_timestamp
                FROM {self.config['schema']}.FACT_TRADING_VOLUME_STAGING
                WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -{lookback}, CURRENT_TIMESTAMP())
                ORDER BY RECORD_METADATA:CreateTime
            """
            
            raw_results = self.snowflake_client.execute_query(query, fetch=True)
            
            if not raw_results:
                self.logger.info("No trading volume staging data found")
                return ParsedStagingData([], 0, 0, 0, [])
            
            # Parse each record
            parsed_records = []
            parsing_errors = []
            
            for row in raw_results:
                try:
                    record = self._parse_trading_volume_record(
                        row['RECORD_CONTENT'],
                        row['RECORD_METADATA'],
                        row['INGESTION_TIMESTAMP']
                    )
                    parsed_records.append(record)
                    
                except Exception as e:
                    error_msg = f"Error parsing trading volume record: {e}"
                    parsing_errors.append(error_msg)
                    self.logger.warning(error_msg)
                    
                    if len(parsing_errors) >= self.config['max_parsing_errors']:
                        break
            
            # Calculate statistics
            valid_records = sum(1 for r in parsed_records if r.is_valid)
            invalid_records = len(parsed_records) - valid_records
            
            result = ParsedStagingData(
                records=parsed_records,
                total_records=len(raw_results),
                valid_records=valid_records,
                invalid_records=invalid_records,
                parsing_errors=parsing_errors
            )
            
            self.logger.info(f"Parsed trading volume data: {valid_records}/{len(raw_results)} valid records")
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing trading volume staging data: {e}")
            return ParsedStagingData([], 0, 0, 0, [str(e)])
    
    def parse_technical_indicators_staging(self, lookback_hours: Optional[int] = None) -> ParsedStagingData:
        """
        Extract and parse technical indicators data from TECHNICAL_INDICATORS_STAGING table.
        
        Args:
            lookback_hours: Hours to look back for new data (default from config)
            
        Returns:
            ParsedStagingData with technical indicators records
        """
        lookback = lookback_hours or self.config['default_lookback_hours']
        
        self.logger.info(f"Parsing technical indicators staging data (lookback: {lookback} hours)")
        
        try:
            query = f"""
                SELECT 
                    RECORD_CONTENT,
                    RECORD_METADATA,
                    RECORD_METADATA:CreateTime::TIMESTAMP as ingestion_timestamp
                FROM {self.config['schema']}.TECHNICAL_INDICATORS_STAGING
                WHERE RECORD_METADATA:CreateTime >= DATEADD(HOUR, -{lookback}, CURRENT_TIMESTAMP())
                ORDER BY RECORD_METADATA:CreateTime
            """
            
            raw_results = self.snowflake_client.execute_query(query, fetch=True)
            
            if not raw_results:
                self.logger.info("No technical indicators staging data found")
                return ParsedStagingData([], 0, 0, 0, [])
            
            # Parse each record
            parsed_records = []
            parsing_errors = []
            
            for row in raw_results:
                try:
                    record = self._parse_technical_indicators_record(
                        row['RECORD_CONTENT'],
                        row['RECORD_METADATA'],
                        row['INGESTION_TIMESTAMP']
                    )
                    parsed_records.append(record)
                    
                except Exception as e:
                    error_msg = f"Error parsing technical indicators record: {e}"
                    parsing_errors.append(error_msg)
                    self.logger.warning(error_msg)
                    
                    if len(parsing_errors) >= self.config['max_parsing_errors']:
                        break
            
            # Calculate statistics
            valid_records = sum(1 for r in parsed_records if r.is_valid)
            invalid_records = len(parsed_records) - valid_records
            
            result = ParsedStagingData(
                records=parsed_records,
                total_records=len(raw_results),
                valid_records=valid_records,
                invalid_records=invalid_records,
                parsing_errors=parsing_errors
            )
            
            self.logger.info(f"Parsed technical indicators data: {valid_records}/{len(raw_results)} valid records")
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing technical indicators staging data: {e}")
            return ParsedStagingData([], 0, 0, 0, [str(e)])
    
    def parse_company_metadata(self, staging_data: List[ParsedStagingData]) -> ParsedStagingData:
        """
        Extract company metadata for dimension updates from parsed staging data.
        
        Args:
            staging_data: List of parsed staging data containing company information
            
        Returns:
            ParsedStagingData with unique company metadata records
        """
        self.logger.info("Extracting company metadata from staging data")
        
        try:
            company_records = []
            parsing_errors = []
            seen_companies = set()
            
            # Extract company metadata from all staging data sources
            for parsed_data in staging_data:
                for record in parsed_data.records:
                    if not record.is_valid:
                        continue
                    
                    # Skip if we've already processed this company
                    if record.symbol in seen_companies:
                        continue
                    
                    try:
                        company_record = self._extract_company_metadata(record)
                        if company_record and company_record.is_valid:
                            company_records.append(company_record)
                            seen_companies.add(record.symbol)
                            
                    except Exception as e:
                        error_msg = f"Error extracting company metadata for {record.symbol}: {e}"
                        parsing_errors.append(error_msg)
                        self.logger.warning(error_msg)
            
            result = ParsedStagingData(
                records=company_records,
                total_records=len(company_records),
                valid_records=len(company_records),
                invalid_records=0,
                parsing_errors=parsing_errors
            )
            
            self.logger.info(f"Extracted {len(company_records)} unique company metadata records")
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing company metadata: {e}")
            return ParsedStagingData([], 0, 0, 0, [str(e)])
    
    def _parse_stock_price_record(self, record_content: Any, record_metadata: Any, ingestion_timestamp: datetime) -> StagingRecord:
        """Parse individual stock price record from VARIANT data"""
        # Convert VARIANT to dict if needed
        if isinstance(record_content, str):
            data = json.loads(record_content)
        else:
            data = dict(record_content) if record_content else {}
        
        # Validate and extract required fields
        validation_errors = []
        
        # Check required fields
        for field in self.required_fields['stock_prices']:
            if field not in data or data[field] is None:
                validation_errors.append(f"Missing required field: {field}")
        
        # Parse timestamp
        try:
            if 'timestamp' in data:
                if isinstance(data['timestamp'], str):
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                else:
                    timestamp = data['timestamp']
            else:
                timestamp = ingestion_timestamp
        except Exception as e:
            validation_errors.append(f"Invalid timestamp format: {e}")
            timestamp = ingestion_timestamp
        
        # Validate numeric fields
        numeric_fields = ['open', 'high', 'low', 'close', 'volume']
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    float(data[field])
                except (ValueError, TypeError):
                    validation_errors.append(f"Invalid numeric value for {field}: {data[field]}")
        
        # Add optional fields with defaults
        for field, default_value in self.optional_fields['stock_prices'].items():
            if field not in data:
                data[field] = default_value
        
        return StagingRecord(
            symbol=data.get('symbol', ''),
            timestamp=timestamp,
            data=data,
            ingestion_timestamp=ingestion_timestamp,
            record_source='stock_prices_staging',
            is_valid=len(validation_errors) == 0,
            validation_errors=validation_errors
        )
    
    def _parse_trading_volume_record(self, record_content: Any, record_metadata: Any, ingestion_timestamp: datetime) -> StagingRecord:
        """Parse individual trading volume record from VARIANT data"""
        # Convert VARIANT to dict if needed
        if isinstance(record_content, str):
            data = json.loads(record_content)
        else:
            data = dict(record_content) if record_content else {}
        
        # Validate and extract required fields
        validation_errors = []
        
        # Check required fields
        for field in self.required_fields['trading_volume']:
            if field not in data or data[field] is None:
                validation_errors.append(f"Missing required field: {field}")
        
        # Parse timestamp
        try:
            if 'timestamp' in data:
                if isinstance(data['timestamp'], str):
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                else:
                    timestamp = data['timestamp']
            else:
                timestamp = ingestion_timestamp
        except Exception as e:
            validation_errors.append(f"Invalid timestamp format: {e}")
            timestamp = ingestion_timestamp
        
        # Validate volume field
        if 'volume' in data and data['volume'] is not None:
            try:
                int(data['volume'])
            except (ValueError, TypeError):
                validation_errors.append(f"Invalid volume value: {data['volume']}")
        
        # Add optional fields with defaults
        for field, default_value in self.optional_fields['trading_volume'].items():
            if field not in data:
                data[field] = default_value
        
        return StagingRecord(
            symbol=data.get('symbol', ''),
            timestamp=timestamp,
            data=data,
            ingestion_timestamp=ingestion_timestamp,
            record_source='trading_volume_staging',
            is_valid=len(validation_errors) == 0,
            validation_errors=validation_errors
        )
    
    def _parse_technical_indicators_record(self, record_content: Any, record_metadata: Any, ingestion_timestamp: datetime) -> StagingRecord:
        """Parse individual technical indicators record from VARIANT data"""
        # Convert VARIANT to dict if needed
        if isinstance(record_content, str):
            data = json.loads(record_content)
        else:
            data = dict(record_content) if record_content else {}
        
        # Validate and extract required fields
        validation_errors = []
        
        # Check required fields
        for field in self.required_fields['technical_indicators']:
            if field not in data or data[field] is None:
                validation_errors.append(f"Missing required field: {field}")
        
        # Parse timestamp
        try:
            if 'timestamp' in data:
                if isinstance(data['timestamp'], str):
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                else:
                    timestamp = data['timestamp']
            else:
                timestamp = ingestion_timestamp
        except Exception as e:
            validation_errors.append(f"Invalid timestamp format: {e}")
            timestamp = ingestion_timestamp
        
        # Validate numeric indicator fields (optional, so just warn if invalid)
        numeric_fields = ['sma_20', 'sma_50', 'ema_12', 'ema_26', 'rsi_14', 'macd', 'macd_signal', 'volume_ratio']
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    float(data[field])
                except (ValueError, TypeError):
                    # Don't mark as invalid, just set to None
                    data[field] = None
        
        # Add optional fields with defaults
        for field, default_value in self.optional_fields['technical_indicators'].items():
            if field not in data:
                data[field] = default_value
        
        return StagingRecord(
            symbol=data.get('symbol', ''),
            timestamp=timestamp,
            data=data,
            ingestion_timestamp=ingestion_timestamp,
            record_source='technical_indicators_staging',
            is_valid=len(validation_errors) == 0,
            validation_errors=validation_errors
        )
    
    def _extract_company_metadata(self, record: StagingRecord) -> Optional[StagingRecord]:
        """Extract company metadata from a staging record"""
        if not record.is_valid or not record.data:
            return None
        
        # Extract company-related fields
        company_data = {
            'symbol': record.symbol,
            'company_name': record.data.get('company_name'),
            'sector': record.data.get('sector'),
            'industry': record.data.get('industry'),
            'exchange': record.data.get('exchange'),
            'currency': record.data.get('currency', 'USD'),
            'country': record.data.get('country')
        }
        
        # Validate company metadata
        validation_errors = []
        
        # Check required fields
        for field in self.required_fields['company_metadata']:
            if field not in company_data or not company_data[field]:
                validation_errors.append(f"Missing required company field: {field}")
        
        # Only create company record if we have meaningful metadata beyond just symbol
        has_metadata = any(
            company_data.get(field) is not None 
            for field in ['company_name', 'sector', 'industry', 'exchange']
        )
        
        if not has_metadata:
            return None
        
        return StagingRecord(
            symbol=record.symbol,
            timestamp=record.timestamp,
            data=company_data,
            ingestion_timestamp=record.ingestion_timestamp,
            record_source='company_metadata',
            is_valid=len(validation_errors) == 0,
            validation_errors=validation_errors
        )
    
    def validate_staging_data(self, parsed_data: ParsedStagingData, data_type: str) -> Dict[str, Any]:
        """
        Validate parsed staging data and return validation report.
        
        Args:
            parsed_data: Parsed staging data to validate
            data_type: Type of data being validated
            
        Returns:
            Dictionary with validation results and recommendations
        """
        self.logger.info(f"Validating {data_type} staging data")
        
        validation_report = {
            'data_type': data_type,
            'total_records': parsed_data.total_records,
            'valid_records': parsed_data.valid_records,
            'invalid_records': parsed_data.invalid_records,
            'success_rate': parsed_data.success_rate,
            'parsing_errors': parsed_data.parsing_errors,
            'validation_passed': True,
            'warnings': [],
            'recommendations': []
        }
        
        # Check success rate thresholds
        if parsed_data.success_rate < 0.95:
            validation_report['warnings'].append(f"Low success rate: {parsed_data.success_rate:.2%}")
            if parsed_data.success_rate < 0.8:
                validation_report['validation_passed'] = False
                validation_report['recommendations'].append("Review data quality issues - success rate below 80%")
        
        # Check for common validation errors
        error_patterns = {}
        for record in parsed_data.records:
            if not record.is_valid:
                for error in record.validation_errors:
                    error_patterns[error] = error_patterns.get(error, 0) + 1
        
        if error_patterns:
            validation_report['common_errors'] = error_patterns
            
            # Add specific recommendations based on error patterns
            if any('timestamp' in error for error in error_patterns):
                validation_report['recommendations'].append("Review timestamp format consistency")
            
            if any('Missing required field' in error for error in error_patterns):
                validation_report['recommendations'].append("Check upstream data completeness")
        
        # Data type specific validations
        if data_type == 'stock_prices':
            self._validate_stock_price_data(parsed_data, validation_report)
        elif data_type == 'trading_volume':
            self._validate_trading_volume_data(parsed_data, validation_report)
        elif data_type == 'technical_indicators':
            self._validate_technical_indicators_data(parsed_data, validation_report)
        
        self.logger.info(f"Validation complete for {data_type}: {validation_report['success_rate']:.2%} success rate")
        return validation_report
    
    def _validate_stock_price_data(self, parsed_data: ParsedStagingData, report: Dict[str, Any]) -> None:
        """Add stock price specific validations"""
        valid_records = [r for r in parsed_data.records if r.is_valid]
        
        if not valid_records:
            return
        
        # Check for reasonable price ranges
        price_issues = 0
        for record in valid_records:
            data = record.data
            if data.get('open', 0) <= 0 or data.get('close', 0) <= 0:
                price_issues += 1
        
        if price_issues > 0:
            report['warnings'].append(f"{price_issues} records with invalid price values")
    
    def _validate_trading_volume_data(self, parsed_data: ParsedStagingData, report: Dict[str, Any]) -> None:
        """Add trading volume specific validations"""
        valid_records = [r for r in parsed_data.records if r.is_valid]
        
        if not valid_records:
            return
        
        # Check for reasonable volume ranges
        volume_issues = 0
        for record in valid_records:
            if record.data.get('volume', 0) <= 0:
                volume_issues += 1
        
        if volume_issues > 0:
            report['warnings'].append(f"{volume_issues} records with invalid volume values")
    
    def _validate_technical_indicators_data(self, parsed_data: ParsedStagingData, report: Dict[str, Any]) -> None:
        """Add technical indicators specific validations"""
        valid_records = [r for r in parsed_data.records if r.is_valid]
        
        if not valid_records:
            return
        
        # Check indicator coverage
        indicator_fields = ['sma_20', 'sma_50', 'ema_12', 'ema_26', 'rsi_14', 'macd']
        coverage = {}
        
        for field in indicator_fields:
            coverage[field] = sum(1 for r in valid_records if r.data.get(field) is not None)
        
        low_coverage = [field for field, count in coverage.items() if count < len(valid_records) * 0.5]
        if low_coverage:
            report['warnings'].append(f"Low coverage for indicators: {', '.join(low_coverage)}")
    
    def to_dataframe(self, parsed_data: ParsedStagingData, include_invalid: bool = False) -> pd.DataFrame:
        """
        Convert parsed staging data to pandas DataFrame for ETL processing.
        
        Args:
            parsed_data: Parsed staging data
            include_invalid: Whether to include invalid records
            
        Returns:
            DataFrame with staging data
        """
        if not parsed_data.records:
            return pd.DataFrame()
        
        # Filter records based on validity
        records_to_convert = parsed_data.records
        if not include_invalid:
            records_to_convert = [r for r in records_to_convert if r.is_valid]
        
        if not records_to_convert:
            return pd.DataFrame()
        
        # Convert to DataFrame
        data_rows = []
        for record in records_to_convert:
            row = record.data.copy()
            row['symbol'] = record.symbol
            row['timestamp'] = record.timestamp
            row['ingestion_timestamp'] = record.ingestion_timestamp
            row['record_source'] = record.record_source
            row['is_valid'] = record.is_valid
            
            if record.validation_errors:
                row['validation_errors'] = '; '.join(record.validation_errors)
            
            data_rows.append(row)
        
        return pd.DataFrame(data_rows)
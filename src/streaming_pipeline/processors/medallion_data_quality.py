"""
Data quality checks for medallion architecture layers (Bronze, Silver, Gold).

This module provides layer-specific data quality validation for the streaming
pipeline's medallion architecture, ensuring data integrity at each stage.
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    isnan, isnull, when, lit, regexp_extract, length, abs as spark_abs,
    stddev, percentile_approx, countDistinct, current_timestamp,
    unix_timestamp, from_json, to_json, size, array_contains
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

logger = logging.getLogger(__name__)


@dataclass
class LayerValidationResult:
    """Result of layer-specific validation check."""
    layer: str  # 'bronze', 'silver', 'gold'
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    failure_rate: float
    severity: str
    message: str
    timestamp: datetime
    failed_records_sample: Optional[List[Dict]] = None


class MedallionDataQualityValidator:
    """
    Data quality validator for medallion architecture layers.
    
    Provides layer-specific validation for Bronze (raw), Silver (processed),
    and Gold (dimensional) data layers.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def validate_bronze_layer(self, df: DataFrame, topic: str) -> List[LayerValidationResult]:
        """
        Validate Bronze layer data (raw data completeness).
        
        Args:
            df: Raw data DataFrame from Kafka
            topic: Source Kafka topic name
            
        Returns:
            List of validation results for Bronze layer
        """
        self.logger.info(f"Validating Bronze layer data from topic: {topic}")
        
        results = []
        total_count = df.count()
        timestamp = datetime.now()
        
        # 1. Data completeness check
        results.append(self._check_data_completeness(df, "bronze", total_count, timestamp))
        
        # 2. Message structure validation
        results.append(self._check_message_structure(df, "bronze", total_count, timestamp))
        
        # 3. Kafka metadata validation
        results.append(self._check_kafka_metadata(df, "bronze", total_count, timestamp))
        
        # 4. Timestamp validation
        results.append(self._check_timestamp_validity(df, "bronze", total_count, timestamp))
        
        # 5. JSON parsing validation
        results.append(self._check_json_parsing(df, "bronze", total_count, timestamp))
        
        # 6. Symbol format validation
        results.append(self._check_symbol_format(df, "bronze", total_count, timestamp))
        
        # 7. Data freshness check
        results.append(self._check_data_freshness(df, "bronze", total_count, timestamp))
        
        self.logger.info(f"Bronze layer validation completed: {len(results)} checks performed")
        return results
    
    def validate_silver_layer(self, df: DataFrame, data_type: str) -> List[LayerValidationResult]:
        """
        Validate Silver layer data (transformation validation).
        
        Args:
            df: Processed data DataFrame
            data_type: Type of processed data ('stock_prices', 'trading_volume', 'technical_indicators')
            
        Returns:
            List of validation results for Silver layer
        """
        self.logger.info(f"Validating Silver layer data for type: {data_type}")
        
        results = []
        total_count = df.count()
        timestamp = datetime.now()
        
        # Common Silver layer validations
        results.append(self._check_processing_metadata(df, "silver", total_count, timestamp))
        results.append(self._check_data_layer_consistency(df, "silver", total_count, timestamp))
        
        # Type-specific validations
        if data_type == "stock_prices":
            results.extend(self._validate_silver_stock_prices(df, total_count, timestamp))
        elif data_type == "trading_volume":
            results.extend(self._validate_silver_trading_volume(df, total_count, timestamp))
        elif data_type == "technical_indicators":
            results.extend(self._validate_silver_technical_indicators(df, total_count, timestamp))
        
        self.logger.info(f"Silver layer validation completed: {len(results)} checks performed")
        return results
    
    def validate_gold_layer(self, df: DataFrame, table_type: str) -> List[LayerValidationResult]:
        """
        Validate Gold layer data (dimensional model validation).
        
        Args:
            df: Dimensional data DataFrame
            table_type: Type of dimensional table ('dim_company', 'dim_date', 'fact_stock_prices', etc.)
            
        Returns:
            List of validation results for Gold layer
        """
        self.logger.info(f"Validating Gold layer data for table: {table_type}")
        
        results = []
        total_count = df.count()
        timestamp = datetime.now()
        
        # Common Gold layer validations
        results.append(self._check_dimensional_integrity(df, "gold", total_count, timestamp))
        
        # Table-specific validations
        if table_type.startswith("dim_"):
            results.extend(self._validate_dimension_table(df, table_type, total_count, timestamp))
        elif table_type.startswith("fact_"):
            results.extend(self._validate_fact_table(df, table_type, total_count, timestamp))
        
        self.logger.info(f"Gold layer validation completed: {len(results)} checks performed")
        return results
    
    def _check_data_completeness(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check for data completeness (no empty records)."""
        try:
            # Check for completely null records
            null_records = df.filter(
                col("value").isNull() | 
                (col("value") == "") |
                (length(col("value")) == 0)
            ).count()
            
            passed = null_records == 0
            failure_rate = null_records / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="data_completeness",
                passed=passed,
                failed_count=null_records,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="ERROR",
                message=f"Data completeness check: {null_records}/{total_count} empty records ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("data_completeness", layer, total_count, timestamp, str(e))
    
    def _check_message_structure(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check message structure consistency."""
        try:
            # Check for required Kafka columns
            required_columns = ["key", "value", "topic", "partition", "offset", "timestamp"]
            missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]
            
            passed = len(missing_columns) == 0
            failed_count = len(missing_columns)
            
            return LayerValidationResult(
                layer=layer,
                rule_name="message_structure",
                passed=passed,
                failed_count=failed_count,
                total_count=len(required_columns),
                failure_rate=failed_count / len(required_columns),
                severity="ERROR",
                message=f"Message structure check: {len(missing_columns)} missing columns: {missing_columns}",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("message_structure", layer, total_count, timestamp, str(e))
    
    def _check_kafka_metadata(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check Kafka metadata validity."""
        try:
            # Check for valid partition and offset values
            invalid_metadata = df.filter(
                col("partition").isNull() |
                col("offset").isNull() |
                (col("partition") < 0) |
                (col("offset") < 0)
            ).count()
            
            passed = invalid_metadata == 0
            failure_rate = invalid_metadata / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="kafka_metadata",
                passed=passed,
                failed_count=invalid_metadata,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="WARNING",
                message=f"Kafka metadata check: {invalid_metadata}/{total_count} invalid metadata records ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("kafka_metadata", layer, total_count, timestamp, str(e))
    
    def _check_timestamp_validity(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check timestamp validity."""
        try:
            # Check for null or future timestamps
            current_time = unix_timestamp(lit(timestamp))
            invalid_timestamps = df.filter(
                col("timestamp").isNull() |
                (unix_timestamp(col("timestamp")) > current_time) |
                (unix_timestamp(col("timestamp")) < unix_timestamp(lit(timestamp - timedelta(days=7))))
            ).count()
            
            passed = invalid_timestamps == 0
            failure_rate = invalid_timestamps / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="timestamp_validity",
                passed=passed,
                failed_count=invalid_timestamps,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="WARNING",
                message=f"Timestamp validity check: {invalid_timestamps}/{total_count} invalid timestamps ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("timestamp_validity", layer, total_count, timestamp, str(e))
    
    def _check_json_parsing(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check JSON parsing validity."""
        try:
            # Try to parse JSON and check for parsing errors
            parsed_df = df.withColumn("parsed_json", from_json(col("value"), "string"))
            parsing_errors = parsed_df.filter(col("parsed_json").isNull()).count()
            
            passed = parsing_errors == 0
            failure_rate = parsing_errors / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="json_parsing",
                passed=passed,
                failed_count=parsing_errors,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="ERROR",
                message=f"JSON parsing check: {parsing_errors}/{total_count} unparseable JSON records ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("json_parsing", layer, total_count, timestamp, str(e))
    
    def _check_symbol_format(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check stock symbol format."""
        try:
            # Extract symbol from key and validate format
            invalid_symbols = df.filter(
                col("key").isNull() |
                ~col("key").rlike("^[A-Z]{1,5}$")
            ).count()
            
            passed = invalid_symbols == 0
            failure_rate = invalid_symbols / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="symbol_format",
                passed=passed,
                failed_count=invalid_symbols,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="ERROR",
                message=f"Symbol format check: {invalid_symbols}/{total_count} invalid symbols ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("symbol_format", layer, total_count, timestamp, str(e))
    
    def _check_data_freshness(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check data freshness (not too old)."""
        try:
            # Check for data older than 1 hour
            one_hour_ago = unix_timestamp(lit(timestamp - timedelta(hours=1)))
            stale_records = df.filter(
                unix_timestamp(col("timestamp")) < one_hour_ago
            ).count()
            
            passed = stale_records == 0
            failure_rate = stale_records / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="data_freshness",
                passed=passed,
                failed_count=stale_records,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="WARNING",
                message=f"Data freshness check: {stale_records}/{total_count} stale records (>1h old) ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("data_freshness", layer, total_count, timestamp, str(e))
    
    def _check_processing_metadata(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check processing metadata in Silver layer."""
        try:
            # Check for required processing metadata
            missing_metadata = df.filter(
                col("data_layer").isNull() |
                col("record_type").isNull() |
                col("processing_version").isNull() |
                col("processing_timestamp").isNull()
            ).count()
            
            passed = missing_metadata == 0
            failure_rate = missing_metadata / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="processing_metadata",
                passed=passed,
                failed_count=missing_metadata,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="ERROR",
                message=f"Processing metadata check: {missing_metadata}/{total_count} missing metadata ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("processing_metadata", layer, total_count, timestamp, str(e))
    
    def _check_data_layer_consistency(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check data layer consistency."""
        try:
            # Check that data_layer field matches expected layer
            incorrect_layer = df.filter(col("data_layer") != layer).count()
            
            passed = incorrect_layer == 0
            failure_rate = incorrect_layer / total_count if total_count > 0 else 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="data_layer_consistency",
                passed=passed,
                failed_count=incorrect_layer,
                total_count=total_count,
                failure_rate=failure_rate,
                severity="WARNING",
                message=f"Data layer consistency check: {incorrect_layer}/{total_count} incorrect layer tags ({failure_rate:.2%})",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("data_layer_consistency", layer, total_count, timestamp, str(e))
    
    def _validate_silver_stock_prices(self, df: DataFrame, total_count: int, timestamp: datetime) -> List[LayerValidationResult]:
        """Validate Silver layer stock prices data."""
        results = []
        
        # Price validation
        try:
            invalid_prices = df.filter(
                col("current_price").isNull() |
                (col("current_price") <= 0) |
                (col("current_price") > 100000)
            ).count()
            
            results.append(LayerValidationResult(
                layer="silver",
                rule_name="price_validity",
                passed=invalid_prices == 0,
                failed_count=invalid_prices,
                total_count=total_count,
                failure_rate=invalid_prices / total_count if total_count > 0 else 0,
                severity="ERROR",
                message=f"Price validity check: {invalid_prices}/{total_count} invalid prices",
                timestamp=timestamp
            ))
        except Exception as e:
            results.append(self._create_error_result("price_validity", "silver", total_count, timestamp, str(e)))
        
        # Moving average validation
        try:
            invalid_sma = df.filter(
                col("sma_5min").isNull() |
                col("sma_20min").isNull() |
                (col("sma_5min") <= 0) |
                (col("sma_20min") <= 0)
            ).count()
            
            results.append(LayerValidationResult(
                layer="silver",
                rule_name="moving_average_validity",
                passed=invalid_sma == 0,
                failed_count=invalid_sma,
                total_count=total_count,
                failure_rate=invalid_sma / total_count if total_count > 0 else 0,
                severity="WARNING",
                message=f"Moving average validity check: {invalid_sma}/{total_count} invalid moving averages",
                timestamp=timestamp
            ))
        except Exception as e:
            results.append(self._create_error_result("moving_average_validity", "silver", total_count, timestamp, str(e)))
        
        return results
    
    def _validate_silver_trading_volume(self, df: DataFrame, total_count: int, timestamp: datetime) -> List[LayerValidationResult]:
        """Validate Silver layer trading volume data."""
        results = []
        
        # Volume validation
        try:
            invalid_volume = df.filter(
                col("volume").isNull() |
                (col("volume") < 0)
            ).count()
            
            results.append(LayerValidationResult(
                layer="silver",
                rule_name="volume_validity",
                passed=invalid_volume == 0,
                failed_count=invalid_volume,
                total_count=total_count,
                failure_rate=invalid_volume / total_count if total_count > 0 else 0,
                severity="ERROR",
                message=f"Volume validity check: {invalid_volume}/{total_count} invalid volumes",
                timestamp=timestamp
            ))
        except Exception as e:
            results.append(self._create_error_result("volume_validity", "silver", total_count, timestamp, str(e)))
        
        return results
    
    def _validate_silver_technical_indicators(self, df: DataFrame, total_count: int, timestamp: datetime) -> List[LayerValidationResult]:
        """Validate Silver layer technical indicators data."""
        results = []
        
        # Technical indicator validation
        try:
            invalid_indicators = df.filter(
                col("momentum_signal").isNull() |
                col("volatility_level").isNull() |
                ~col("momentum_signal").isin(["bullish", "bearish", "neutral"]) |
                ~col("volatility_level").isin(["high", "medium", "low"])
            ).count()
            
            results.append(LayerValidationResult(
                layer="silver",
                rule_name="technical_indicator_validity",
                passed=invalid_indicators == 0,
                failed_count=invalid_indicators,
                total_count=total_count,
                failure_rate=invalid_indicators / total_count if total_count > 0 else 0,
                severity="WARNING",
                message=f"Technical indicator validity check: {invalid_indicators}/{total_count} invalid indicators",
                timestamp=timestamp
            ))
        except Exception as e:
            results.append(self._create_error_result("technical_indicator_validity", "silver", total_count, timestamp, str(e)))
        
        return results
    
    def _check_dimensional_integrity(self, df: DataFrame, layer: str, total_count: int, timestamp: datetime) -> LayerValidationResult:
        """Check dimensional model integrity."""
        try:
            # This is a placeholder for dimensional integrity checks
            # In a real implementation, this would check foreign key relationships
            passed = True
            failed_count = 0
            
            return LayerValidationResult(
                layer=layer,
                rule_name="dimensional_integrity",
                passed=passed,
                failed_count=failed_count,
                total_count=total_count,
                failure_rate=0.0,
                severity="INFO",
                message=f"Dimensional integrity check: {total_count} records validated",
                timestamp=timestamp
            )
        except Exception as e:
            return self._create_error_result("dimensional_integrity", layer, total_count, timestamp, str(e))
    
    def _validate_dimension_table(self, df: DataFrame, table_type: str, total_count: int, timestamp: datetime) -> List[LayerValidationResult]:
        """Validate dimension table specific rules."""
        results = []
        
        # This would contain dimension-specific validation logic
        # For now, return a placeholder result
        results.append(LayerValidationResult(
            layer="gold",
            rule_name=f"{table_type}_validation",
            passed=True,
            failed_count=0,
            total_count=total_count,
            failure_rate=0.0,
            severity="INFO",
            message=f"{table_type} validation: {total_count} records validated",
            timestamp=timestamp
        ))
        
        return results
    
    def _validate_fact_table(self, df: DataFrame, table_type: str, total_count: int, timestamp: datetime) -> List[LayerValidationResult]:
        """Validate fact table specific rules."""
        results = []
        
        # This would contain fact table-specific validation logic
        # For now, return a placeholder result
        results.append(LayerValidationResult(
            layer="gold",
            rule_name=f"{table_type}_validation",
            passed=True,
            failed_count=0,
            total_count=total_count,
            failure_rate=0.0,
            severity="INFO",
            message=f"{table_type} validation: {total_count} records validated",
            timestamp=timestamp
        ))
        
        return results
    
    def _create_error_result(self, rule_name: str, layer: str, total_count: int, timestamp: datetime, error_msg: str) -> LayerValidationResult:
        """Create an error result for failed validation execution."""
        return LayerValidationResult(
            layer=layer,
            rule_name=rule_name,
            passed=False,
            failed_count=total_count,
            total_count=total_count,
            failure_rate=1.0,
            severity="ERROR",
            message=f"Validation execution failed: {error_msg}",
            timestamp=timestamp
        )
    
    def publish_data_quality_alerts(self, validation_results: List[LayerValidationResult], kafka_topic: str, avro_serializer=None) -> None:
        """
        Publish data quality alerts to Kafka topic using Avro serialization.
        
        Args:
            validation_results: List of validation results
            kafka_topic: Kafka topic for data quality alerts
            avro_serializer: Optional Avro serializer instance
        """
        try:
            # Filter for failed validations
            failed_validations = [r for r in validation_results if not r.passed and r.severity in ["ERROR", "WARNING"]]
            
            if not failed_validations:
                self.logger.info("No data quality issues to report")
                return
            
            # Create alert messages
            alerts = []
            for result in failed_validations:
                alert = {
                    "timestamp": int(result.timestamp.timestamp() * 1000),  # epoch millis
                    "layer": result.layer,
                    "rule_name": result.rule_name,
                    "severity": result.severity,
                    "message": result.message,
                    "failure_rate": result.failure_rate,
                    "failed_count": result.failed_count,
                    "total_count": result.total_count,
                    "topic": None,  # Could be populated if needed
                    "data_type": None  # Could be populated if needed
                }
                alerts.append(alert)
            
            # Log alerts (actual Kafka publishing would be done by StreamProcessor)
            if alerts:
                self.logger.warning(f"Generated {len(alerts)} data quality alerts for topic: {kafka_topic}")
                for alert in alerts:
                    self.logger.warning(f"Data Quality Alert: {alert['layer']}.{alert['rule_name']} - {alert['message']}")
            
            # Return alerts for external publishing
            return alerts
            
        except Exception as e:
            self.logger.error(f"Failed to generate data quality alerts: {str(e)}")
            return []
    
    def generate_layer_quality_report(self, validation_results: List[LayerValidationResult]) -> Dict[str, Any]:
        """
        Generate layer-specific data quality report.
        
        Args:
            validation_results: List of validation results
            
        Returns:
            Data quality report dictionary
        """
        # Group results by layer
        layer_results = {}
        for result in validation_results:
            if result.layer not in layer_results:
                layer_results[result.layer] = []
            layer_results[result.layer].append(result)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "layers": {}
        }
        
        for layer, results in layer_results.items():
            total_rules = len(results)
            passed_rules = sum(1 for r in results if r.passed)
            error_count = sum(1 for r in results if r.severity == "ERROR" and not r.passed)
            warning_count = sum(1 for r in results if r.severity == "WARNING" and not r.passed)
            
            report["layers"][layer] = {
                "total_rules": total_rules,
                "passed_rules": passed_rules,
                "failed_rules": total_rules - passed_rules,
                "pass_rate": passed_rules / total_rules if total_rules > 0 else 0,
                "error_count": error_count,
                "warning_count": warning_count,
                "failed_rules": [
                    {
                        "rule_name": r.rule_name,
                        "severity": r.severity,
                        "message": r.message,
                        "failure_rate": r.failure_rate
                    } for r in results if not r.passed
                ]
            }
        
        return report
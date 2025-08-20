"""
Data quality validation and checks for dimensional model.

This module provides comprehensive data quality validation for fact and
dimension tables, including completeness, accuracy, consistency, and
business rule validation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    isnan, isnull, when, lit, regexp_extract, length, abs as spark_abs,
    stddev, percentile_approx, countDistinct, lag
)
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationRule:
    """Data validation rule definition."""
    name: str
    description: str
    column: str
    rule_type: str  # 'not_null', 'range', 'format', 'uniqueness', 'referential'
    parameters: Dict[str, Any]
    severity: str = 'ERROR'  # 'ERROR', 'WARNING', 'INFO'


@dataclass
class ValidationResult:
    """Result of a validation check."""
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    failure_rate: float
    severity: str
    message: str
    failed_records: Optional[DataFrame] = None


class DataQualityValidator:
    """Comprehensive data quality validator for dimensional model."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def validate_dim_company(self, df: DataFrame) -> List[ValidationResult]:
        """
        Validate dim_company dimension table.
        
        Args:
            df: dim_company DataFrame
            
        Returns:
            List of validation results
        """
        rules = [
            ValidationRule(
                name="company_symbol_not_null",
                description="Company symbol must not be null",
                column="symbol",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="company_symbol_format",
                description="Company symbol must be 1-5 uppercase letters",
                column="symbol",
                rule_type="format",
                parameters={"pattern": "^[A-Z]{1,5}$"},
                severity="ERROR"
            ),
            ValidationRule(
                name="company_key_uniqueness",
                description="Company key must be unique",
                column="company_key",
                rule_type="uniqueness",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="effective_date_not_null",
                description="Effective date must not be null",
                column="effective_date",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="current_record_consistency",
                description="Only one current record per symbol",
                column="symbol",
                rule_type="business_rule",
                parameters={"rule": "current_record_per_symbol"},
                severity="ERROR"
            )
        ]
        
        return self._execute_validation_rules(df, rules)
    
    def validate_dim_date(self, df: DataFrame) -> List[ValidationResult]:
        """
        Validate dim_date dimension table.
        
        Args:
            df: dim_date DataFrame
            
        Returns:
            List of validation results
        """
        rules = [
            ValidationRule(
                name="date_key_not_null",
                description="Date key must not be null",
                column="date_key",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="date_key_format",
                description="Date key must be in YYYYMMDD format",
                column="date_key",
                rule_type="range",
                parameters={"min_value": 19000101, "max_value": 21001231},
                severity="ERROR"
            ),
            ValidationRule(
                name="date_value_not_null",
                description="Date value must not be null",
                column="date_value",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="year_range",
                description="Year must be reasonable range",
                column="year",
                rule_type="range",
                parameters={"min_value": 1900, "max_value": 2100},
                severity="WARNING"
            ),
            ValidationRule(
                name="month_range",
                description="Month must be 1-12",
                column="month",
                rule_type="range",
                parameters={"min_value": 1, "max_value": 12},
                severity="ERROR"
            ),
            ValidationRule(
                name="day_of_month_range",
                description="Day of month must be 1-31",
                column="day_of_month",
                rule_type="range",
                parameters={"min_value": 1, "max_value": 31},
                severity="ERROR"
            )
        ]
        
        return self._execute_validation_rules(df, rules)
    
    def validate_dim_time(self, df: DataFrame) -> List[ValidationResult]:
        """
        Validate dim_time dimension table.
        
        Args:
            df: dim_time DataFrame
            
        Returns:
            List of validation results
        """
        rules = [
            ValidationRule(
                name="time_key_not_null",
                description="Time key must not be null",
                column="time_key",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="hour_range",
                description="Hour must be 0-23",
                column="hour",
                rule_type="range",
                parameters={"min_value": 0, "max_value": 23},
                severity="ERROR"
            ),
            ValidationRule(
                name="minute_range",
                description="Minute must be 0-59",
                column="minute",
                rule_type="range",
                parameters={"min_value": 0, "max_value": 59},
                severity="ERROR"
            ),
            ValidationRule(
                name="market_session_values",
                description="Market session must be valid value",
                column="market_session",
                rule_type="format",
                parameters={"allowed_values": ["PRE_MARKET", "REGULAR", "AFTER_HOURS", "CLOSED"]},
                severity="WARNING"
            )
        ]
        
        return self._execute_validation_rules(df, rules)
    
    def validate_fact_stock_prices(self, df: DataFrame) -> List[ValidationResult]:
        """
        Validate fact_stock_prices table.
        
        Args:
            df: fact_stock_prices DataFrame
            
        Returns:
            List of validation results
        """
        rules = [
            ValidationRule(
                name="price_key_not_null",
                description="Price key must not be null",
                column="price_key",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="company_key_not_null",
                description="Company key must not be null",
                column="company_key",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="positive_prices",
                description="Stock prices must be positive",
                column="close_price",
                rule_type="range",
                parameters={"min_value": 0.01, "max_value": 100000},
                severity="ERROR"
            ),
            ValidationRule(
                name="volume_non_negative",
                description="Volume must be non-negative",
                column="volume",
                rule_type="range",
                parameters={"min_value": 0, "max_value": None},
                severity="ERROR"
            ),
            ValidationRule(
                name="price_consistency",
                description="High >= Low, High >= Open, High >= Close",
                column="high_price",
                rule_type="business_rule",
                parameters={"rule": "price_consistency"},
                severity="ERROR"
            ),
            ValidationRule(
                name="reasonable_price_change",
                description="Price changes should be reasonable (< 50% in one period)",
                column="close_price",
                rule_type="business_rule",
                parameters={"rule": "reasonable_price_change", "threshold": 0.5},
                severity="WARNING"
            )
        ]
        
        return self._execute_validation_rules(df, rules)
    
    def validate_fact_trading_volume(self, df: DataFrame) -> List[ValidationResult]:
        """
        Validate fact_trading_volume table.
        
        Args:
            df: fact_trading_volume DataFrame
            
        Returns:
            List of validation results
        """
        rules = [
            ValidationRule(
                name="volume_key_not_null",
                description="Volume key must not be null",
                column="volume_key",
                rule_type="not_null",
                parameters={},
                severity="ERROR"
            ),
            ValidationRule(
                name="volume_positive",
                description="Volume must be positive",
                column="volume",
                rule_type="range",
                parameters={"min_value": 1, "max_value": None},
                severity="ERROR"
            ),
            ValidationRule(
                name="volume_weighted_price_positive",
                description="Volume weighted price must be positive",
                column="volume_weighted_price",
                rule_type="range",
                parameters={"min_value": 0.01, "max_value": 100000},
                severity="ERROR"
            )
        ]
        
        return self._execute_validation_rules(df, rules)
    
    def _execute_validation_rules(self, df: DataFrame, rules: List[ValidationRule]) -> List[ValidationResult]:
        """
        Execute validation rules against DataFrame.
        
        Args:
            df: DataFrame to validate
            rules: List of validation rules
            
        Returns:
            List of validation results
        """
        results = []
        total_count = df.count()
        
        for rule in rules:
            try:
                result = self._execute_single_rule(df, rule, total_count)
                results.append(result)
                
                if result.severity == 'ERROR' and not result.passed:
                    self.logger.error(f"Validation failed: {result.message}")
                elif result.severity == 'WARNING' and not result.passed:
                    self.logger.warning(f"Validation warning: {result.message}")
                    
            except Exception as e:
                self.logger.error(f"Error executing validation rule {rule.name}: {str(e)}")
                results.append(ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    failed_count=total_count,
                    total_count=total_count,
                    failure_rate=1.0,
                    severity=rule.severity,
                    message=f"Validation rule execution failed: {str(e)}"
                ))
        
        return results
    
    def _execute_single_rule(self, df: DataFrame, rule: ValidationRule, total_count: int) -> ValidationResult:
        """Execute a single validation rule."""
        
        if rule.rule_type == "not_null":
            failed_df = df.filter(col(rule.column).isNull())
            failed_count = failed_df.count()
            
        elif rule.rule_type == "range":
            min_val = rule.parameters.get("min_value")
            max_val = rule.parameters.get("max_value")
            
            condition = lit(True)
            if min_val is not None:
                condition = condition & (col(rule.column) >= min_val)
            if max_val is not None:
                condition = condition & (col(rule.column) <= max_val)
                
            failed_df = df.filter(~condition | col(rule.column).isNull())
            failed_count = failed_df.count()
            
        elif rule.rule_type == "format":
            if "pattern" in rule.parameters:
                pattern = rule.parameters["pattern"]
                failed_df = df.filter(
                    ~col(rule.column).rlike(pattern) | col(rule.column).isNull()
                )
            elif "allowed_values" in rule.parameters:
                allowed_values = rule.parameters["allowed_values"]
                failed_df = df.filter(
                    ~col(rule.column).isin(allowed_values) | col(rule.column).isNull()
                )
            else:
                raise ValueError(f"Invalid format rule parameters: {rule.parameters}")
            
            failed_count = failed_df.count()
            
        elif rule.rule_type == "uniqueness":
            duplicate_df = df.groupBy(rule.column).count().filter(col("count") > 1)
            failed_count = duplicate_df.count()
            failed_df = df.join(duplicate_df, rule.column, "inner")
            
        elif rule.rule_type == "business_rule":
            failed_df, failed_count = self._execute_business_rule(df, rule)
            
        else:
            raise ValueError(f"Unknown rule type: {rule.rule_type}")
        
        failure_rate = failed_count / total_count if total_count > 0 else 0
        passed = failed_count == 0
        
        message = f"{rule.description}: {failed_count}/{total_count} records failed ({failure_rate:.2%})"
        
        return ValidationResult(
            rule_name=rule.name,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            severity=rule.severity,
            message=message,
            failed_records=failed_df if failed_count > 0 else None
        )
    
    def _execute_business_rule(self, df: DataFrame, rule: ValidationRule) -> Tuple[DataFrame, int]:
        """Execute business rule validation."""
        
        rule_name = rule.parameters.get("rule")
        
        if rule_name == "current_record_per_symbol":
            # Check that each symbol has only one current record
            current_records = df.filter(col("is_current") == True)
            duplicate_symbols = current_records.groupBy("symbol").count().filter(col("count") > 1)
            failed_df = current_records.join(duplicate_symbols, "symbol", "inner")
            failed_count = failed_df.count()
            
        elif rule_name == "price_consistency":
            # High >= Low, High >= Open, High >= Close
            failed_df = df.filter(
                (col("high_price") < col("low_price")) |
                (col("high_price") < col("open_price")) |
                (col("high_price") < col("close_price")) |
                (col("low_price") > col("open_price")) |
                (col("low_price") > col("close_price"))
            )
            failed_count = failed_df.count()
            
        elif rule_name == "reasonable_price_change":
            # Check for unreasonable price changes
            threshold = rule.parameters.get("threshold", 0.5)
            window_spec = Window.partitionBy("company_key").orderBy("date_key", "time_key")
            
            df_with_prev = df.withColumn("prev_close", lag("close_price").over(window_spec))
            failed_df = df_with_prev.filter(
                spark_abs((col("close_price") - col("prev_close")) / col("prev_close")) > threshold
            )
            failed_count = failed_df.count()
            
        else:
            raise ValueError(f"Unknown business rule: {rule_name}")
        
        return failed_df, failed_count
    
    def generate_data_quality_report(self, validation_results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Generate comprehensive data quality report.
        
        Args:
            validation_results: List of validation results
            
        Returns:
            Data quality report dictionary
        """
        total_rules = len(validation_results)
        passed_rules = sum(1 for r in validation_results if r.passed)
        failed_rules = total_rules - passed_rules
        
        error_rules = [r for r in validation_results if r.severity == 'ERROR' and not r.passed]
        warning_rules = [r for r in validation_results if r.severity == 'WARNING' and not r.passed]
        
        report = {
            "summary": {
                "total_rules": total_rules,
                "passed_rules": passed_rules,
                "failed_rules": failed_rules,
                "pass_rate": passed_rules / total_rules if total_rules > 0 else 0,
                "error_count": len(error_rules),
                "warning_count": len(warning_rules)
            },
            "failed_rules": {
                "errors": [
                    {
                        "rule_name": r.rule_name,
                        "message": r.message,
                        "failure_rate": r.failure_rate
                    } for r in error_rules
                ],
                "warnings": [
                    {
                        "rule_name": r.rule_name,
                        "message": r.message,
                        "failure_rate": r.failure_rate
                    } for r in warning_rules
                ]
            },
            "recommendations": self._generate_recommendations(validation_results)
        }
        
        return report
    
    def _generate_recommendations(self, validation_results: List[ValidationResult]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        error_rules = [r for r in validation_results if r.severity == 'ERROR' and not r.passed]
        if error_rules:
            recommendations.append("Address all ERROR-level validation failures before proceeding to production")
        
        high_failure_rate_rules = [r for r in validation_results if r.failure_rate > 0.1 and not r.passed]
        if high_failure_rate_rules:
            recommendations.append("Investigate rules with high failure rates (>10%) for data source issues")
        
        price_consistency_failures = [r for r in validation_results 
                                    if "price_consistency" in r.rule_name and not r.passed]
        if price_consistency_failures:
            recommendations.append("Review data extraction logic for price consistency issues")
        
        return recommendations
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from datetime import datetime

class DataQualityChecker:
    """Class for performing data quality checks on batch data"""
    
    def __init__(self, spark):
        self.spark = spark
        self.quality_checks = []
        
    def add_check(self, check_name: str, check_func, description: str = None):
        """Add a custom data quality check"""
        self.quality_checks.append({
            'name': check_name,
            'func': check_func,
            'description': description or check_name
        })
        return self
    
    def run_checks(self, df: DataFrame, batch_id: str) -> DataFrame:
        """Run all registered data quality checks"""
        results = []
        
        # Run built-in checks
        results.extend(self._run_builtin_checks(df, batch_id))
        
        # Run custom checks
        for check in self.quality_checks:
            try:
                result = check['func'](df)
                results.append({
                    'check_name': check['name'],
                    'status': 'PASSED' if result['passed'] else 'FAILED',
                    'description': check['description'],
                    'records_processed': result.get('records_processed'),
                    'records_failed': result.get('records_failed')
                })
            except Exception as e:
                results.append({
                    'check_name': check['name'],
                    'status': 'ERROR',
                    'description': f"Check failed with error: {str(e)}",
                    'records_processed': 0,
                    'records_failed': 0
                })
        
        # Convert results to DataFrame
        return self._create_results_df(results, batch_id)
    
    def _run_builtin_checks(self, df: DataFrame, batch_id: str) -> list:
        """Run built-in data quality checks"""
        results = []
        
        # Check for null values in required columns
        null_checks = [
            'stock_id', 'trading_date', 'open_price', 
            'high_price', 'low_price', 'close_price'
        ]
        
        for col_name in null_checks:
            null_count = df.filter(F.col(col_name).isNull()).count()
            results.append({
                'check_name': f'null_check_{col_name}',
                'status': 'PASSED' if null_count == 0 else 'FAILED',
                'description': f'Check for null values in {col_name}',
                'records_processed': df.count(),
                'records_failed': null_count
            })
        
        # Check for negative prices
        price_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adjusted_close']
        for col in price_cols:
            neg_count = df.filter(F.col(col) < 0).count()
            results.append({
                'check_name': f'negative_price_check_{col}',
                'status': 'PASSED' if neg_count == 0 else 'FAILED',
                'description': f'Check for negative values in {col}',
                'records_processed': df.count(),
                'records_failed': neg_count
            })
            
        # Check for volume consistency
        vol_check = df.filter((F.col('volume').isNull()) | (F.col('volume') < 0)).count()
        results.append({
            'check_name': 'volume_consistency_check',
            'status': 'PASSED' if vol_check == 0 else 'FAILED',
            'description': 'Check for null or negative volume values',
            'records_processed': df.count(),
            'records_failed': vol_check
        })
        
        return results
    
    def _create_results_df(self, results: list, batch_id: str) -> DataFrame:
        """Convert check results to DataFrame"""
        # Add timestamp and batch_id to each result
        timestamp = datetime.utcnow()
        for result in results:
            result.update({
                'check_timestamp': timestamp,
                'batch_id': batch_id
            })
        
        # Create DataFrame
        return self.spark.createDataFrame(results)

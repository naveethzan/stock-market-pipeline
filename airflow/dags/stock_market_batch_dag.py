"""
Stock Market Batch Processing DAG

This DAG orchestrates the end-to-end batch processing pipeline:
1. Raw extract to S3 (Parquet)
2. Execute PySpark batch processing (transformation/curated)
3. Load processed data to Snowflake
4. Run data quality checks

"""
from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
import pendulum

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'snowflake_conn_id': 'snowflake_conn',
    'on_failure_callback': None,
    'execution_timeout': timedelta(hours=2),
}

# Get environment variables
PROJECT_HOME = Variable.get('PROJECT_HOME', default_var=os.getcwd())
ENVIRONMENT = Variable.get('ENVIRONMENT', default_var='dev')
AWS_S3_BUCKET = Variable.get('aws_s3_bucket', default_var=os.environ.get('AWS_S3_BUCKET', ''))
RAW_BASE_URI = Variable.get('RAW_BASE_URI', default_var=os.environ.get('RAW_BASE_URI', 'raw-data'))

# Create DAG
with DAG(
    'stock_market_batch_processing',
    default_args=default_args,
    schedule='0 5 * * 1-5',  # 05:00 UTC on weekdays
    description='Orchestrates the stock market batch processing pipeline',
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    tags=['stock_market', 'batch', 'etl'],
) as dag:

    # Task to get processing date as T-1
    @task(task_id='set_processing_date')
    def get_processing_date():
        ctx = get_current_context()
        # Use T-1 relative to the end of the data interval for this DAG run
        pd = (ctx['data_interval_end'] - timedelta(days=1)).date()
        while pd.weekday() >= 5:  # 5=Sat, 6=Sun
            pd -= timedelta(days=1)
        return pd.strftime('%Y-%m-%d')

    processing_date = get_processing_date()

    # Short-circuit early if data already processed for PROCESSING_DATE
    @task.short_circuit(task_id='check_already_processed')
    def check_already_processed(processing_date: str):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from snowflake.connector.errors import ProgrammingError
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        try:
            # First check if the table exists
            table_check = """
            SELECT COUNT(*) as table_exists 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'PUBLIC' 
            AND TABLE_NAME = 'DAILY_STOCKS'
            """
            table_exists = snowflake_hook.get_first(table_check)
            
            if not table_exists or table_exists[0] == 0:
                logger.info("DAILY_STOCKS table not found. Treating as first run; continuing.")
                return True
                
            # If table exists, check for existing data for the processing date
            query = f"""
            SELECT COUNT(*) as count 
            FROM STOCK_MARKET.PUBLIC.DAILY_STOCKS 
            WHERE PROCESS_DATE = TO_DATE('{processing_date}')
            """
            result = snowflake_hook.get_first(query)
            
            if result and result[0] > 0:
                logger.info(f"Data already exists for {processing_date}. Short-circuiting downstream tasks.")
                return False
            
            logger.info(f"No data found for {processing_date}. Proceeding with processing.")
            return True
            
        except ProgrammingError as e:
            # If there's an error (like table doesn't exist), treat as first run
            logger.warning(f"Error checking for existing data: {str(e)}. Proceeding with processing.")
            return True

    proceed = check_already_processed(processing_date)

    # Raw extraction group: Extract from yfinance -> validate S3 partition
    with TaskGroup(group_id='raw') as raw:
        @task(task_id='extract_raw_parquet')
        def extract_raw_parquet(processing_date: str):
            import sys
            import os
            from airflow.exceptions import AirflowException
            
            # Ensure project and src are importable
            if PROJECT_HOME and PROJECT_HOME not in sys.path:
                sys.path.insert(0, PROJECT_HOME)
            src_dir = os.path.join(PROJECT_HOME, 'src')
            if os.path.isdir(src_dir) and src_dir not in sys.path:
                sys.path.insert(0, src_dir)

            try:
                from batch.extractors.yfinance_extractor import extract_to_s3_parquet
            except Exception as e:
                raise AirflowException(f"Failed to import extractor: {e}")

            bucket = AWS_S3_BUCKET
            if not bucket:
                raise AirflowException("AWS_S3_BUCKET not configured")

            from airflow.models import Variable as _V
            raw_base_uri = _V.get('RAW_BASE_URI', default_var=os.environ.get('RAW_BASE_URI', 'raw-data'))
            symbols_csv = _V.get('BATCH_SYMBOLS', default_var=os.environ.get('BATCH_SYMBOLS', 'AAPL,MSFT,GOOGL,AMZN,META,TSLA,NVDA,INTC,JPM,V'))
            symbols = [s.strip() for s in symbols_csv.split(',') if s.strip()]

            result = extract_to_s3_parquet(
                process_date=processing_date,
                bucket=bucket,
                raw_base_uri=raw_base_uri,
                symbols=symbols,
            )
            return result

        @task(task_id='validate_raw_partition')
        def validate_raw_partition(processing_date: str, extract_result: dict):
            import boto3
            from airflow.exceptions import AirflowException
            import datetime as _dt
            
            bucket = AWS_S3_BUCKET
            if not bucket:
                raise AirflowException("AWS_S3_BUCKET not configured")

            prefix = (extract_result or {}).get('prefix')
            if not prefix:
                # Reconstruct prefix if xcom missing
                from airflow.models import Variable as _V
                raw_base_uri = _V.get('RAW_BASE_URI', default_var=os.environ.get('RAW_BASE_URI', 'raw-data'))
                try:
                    dt = _dt.datetime.strptime(processing_date, "%Y-%m-%d")
                except Exception:
                    raise AirflowException(f"Invalid processing_date: {processing_date}")
                y, m, d = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
                prefix = f"{raw_base_uri}/batch/year={y}/month={m}/day={d}/"

            s3 = boto3.client('s3')
            try:
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                contents = resp.get('Contents', [])
                if not contents:
                    raise AirflowException(f"No objects found under s3://{bucket}/{prefix}")
                parquet_files = [o for o in contents if o['Key'].endswith('.parquet')]
                success_markers = [o for o in contents if o['Key'].endswith('/_SUCCESS') or o['Key'].endswith('_SUCCESS')]
                if not parquet_files:
                    raise AirflowException(f"No parquet files found under s3://{bucket}/{prefix}")
                if not success_markers:
                    raise AirflowException(f"_SUCCESS marker not found under s3://{bucket}/{prefix}")
            except Exception as e:
                raise AirflowException(f"Error validating raw partition: {e}")
            return True

        extract_task = extract_raw_parquet(processing_date)
        validate_task = validate_raw_partition(processing_date, extract_task)
        extract_task >> validate_task

    # ETL group: Spark ETL -> assert transformed exists
    with TaskGroup(group_id='etl') as etl:
        spark_etl = BashOperator(
            task_id='run_spark_etl',
            bash_command=(
                'docker exec spark-client '
                'spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 '
                '--conf spark.driver.memory=2g '
                '--conf spark.driver.maxResultSize=512m '
                '--conf spark.default.parallelism=2 '
                '--conf spark.sql.shuffle.partitions=2 '
                f'--conf spark.s3.bucket="{AWS_S3_BUCKET}" '
                f'--conf spark.s3.raw_zone="{RAW_BASE_URI}/batch" '
                '--conf spark.s3.transformed_zone="transformed/batch" '
                '/opt/spark/batch_jobs/daily_etl.py --date "$PROCESSING_DATE"'
            ),
            env={
                **os.environ
                ,
                'PYTHONUNBUFFERED': '1',
                'AWS_S3_BUCKET': AWS_S3_BUCKET,
                'PROCESSING_DATE': "{{ ti.xcom_pull(task_ids='set_processing_date') }}",
            },
            retries=2,
            retry_delay=timedelta(minutes=5),
            execution_timeout=timedelta(hours=1),
        )

        # Assertion: transformed data must exist for PROCESSING_DATE in S3
        @task(task_id='assert_transformed_exists')
        def assert_transformed_exists(processing_date: str):
            import os
            from airflow.exceptions import AirflowException
            try:
                import boto3
            except Exception as e:
                logger.error(f"boto3 not available to check S3: {e}")
                raise AirflowException("Dependency error: boto3 not available to check S3 for transformed data")
            
            if not processing_date:
                logger.error("processing_date missing; cannot validate transformed data existence")
                raise AirflowException("processing_date not provided")

            bucket = os.environ.get('AWS_S3_BUCKET') or Variable.get('aws_s3_bucket', default_var=None)
            if not bucket:
                logger.error("AWS_S3_BUCKET not configured; cannot validate transformed data existence")
                raise AirflowException("AWS_S3_BUCKET not configured")

            prefix = f"transformed/batch/date={processing_date}/"
            try:
                s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
                parquet_files = []
                if 'Contents' in resp:
                    parquet_files = [
                        obj for obj in resp['Contents']
                        if obj.get('Key', '').endswith('.parquet') and obj.get('Size', 0) > 0
                    ]
                if parquet_files:
                    logger.info(f"Found {len(parquet_files)} parquet files under s3://{bucket}/{prefix}")
                    return
                else:
                    logger.error(f"No parquet data files found for {processing_date} at s3://{bucket}/{prefix}")
                    raise AirflowException(f"No parquet data files found for {processing_date} at s3://{bucket}/{prefix}")
            except Exception as e:
                logger.error(f"Error checking S3 for transformed data: {e}")
                raise AirflowException(f"Error checking S3 for transformed data: {e}")

        assert_transformed = assert_transformed_exists(processing_date)

        spark_etl >> assert_transformed

    aws_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', '')

    stage_url = f"s3://{AWS_S3_BUCKET}/transformed/batch/"
    # Escape single quotes to avoid SQL parsing issues
    _aws_key_escaped = aws_key.replace("'", "''")
    _aws_secret_escaped = aws_secret.replace("'", "''")
    credentials_sql = (
        f"AWS_KEY_ID='{_aws_key_escaped}'\n            AWS_SECRET_KEY='{_aws_secret_escaped}'"
    )

    sql_stmt = f'''
CREATE OR REPLACE STAGE STOCK_MARKET.PUBLIC.BATCH_STAGE
URL='{stage_url}'
CREDENTIALS=(
    {credentials_sql}
)
FILE_FORMAT=(TYPE=PARQUET);

CREATE OR REPLACE TEMP TABLE DAILY_STOCKS_RAW (V VARIANT);

-- Create a fresh temp staging table for this session
CREATE OR REPLACE TEMP TABLE DAILY_STOCKS_STG LIKE STOCK_MARKET.PUBLIC.DAILY_STOCKS;

-- Load parquet rows for the processing date into RAW (filters out 0-byte files via part- pattern)
COPY INTO DAILY_STOCKS_RAW
FROM @STOCK_MARKET.PUBLIC.BATCH_STAGE
FILE_FORMAT=(TYPE=PARQUET)
PATTERN='.*date={{{{ ti.xcom_pull(task_ids=\'set_processing_date\') }}}}/part-.*\\.parquet$'
ON_ERROR='SKIP_FILE';

-- Map from VARIANT to typed staging with explicit casts
INSERT INTO DAILY_STOCKS_STG (
    STOCK_ID, TRADING_DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE,
    VOLUME, ADJUSTED_CLOSE, DAILY_RETURN, SMA_5, SMA_20, RSI_14,
    INGESTION_TIMESTAMP, BATCH_ID, PROCESS_DATE
)
SELECT
    V:stock_id::STRING,
    V:trading_date::DATE,
    V:open_price::FLOAT,
    V:high_price::FLOAT,
    V:low_price::FLOAT,
    V:close_price::FLOAT,
    V:volume::NUMBER,
    V:adjusted_close::FLOAT,
    V:daily_return::FLOAT,
    V:sma_5::FLOAT,
    V:sma_20::FLOAT,
    V:rsi_14::FLOAT,
    V:ingestion_timestamp::TIMESTAMP_NTZ,
    V:batch_id::STRING,
    V:process_date::DATE
FROM DAILY_STOCKS_RAW;

-- Idempotent upsert into target table
MERGE INTO STOCK_MARKET.PUBLIC.DAILY_STOCKS AS T
USING DAILY_STOCKS_STG AS S
ON T.STOCK_ID = S.STOCK_ID
   AND T.TRADING_DATE = S.TRADING_DATE
   AND T.PROCESS_DATE = S.PROCESS_DATE
WHEN MATCHED THEN UPDATE SET
    T.OPEN_PRICE = S.OPEN_PRICE,
    T.HIGH_PRICE = S.HIGH_PRICE,
    T.LOW_PRICE = S.LOW_PRICE,
    T.CLOSE_PRICE = S.CLOSE_PRICE,
    T.VOLUME = S.VOLUME,
    T.ADJUSTED_CLOSE = S.ADJUSTED_CLOSE,
    T.DAILY_RETURN = S.DAILY_RETURN,
    T.SMA_5 = S.SMA_5,
    T.SMA_20 = S.SMA_20,
    T.RSI_14 = S.RSI_14,
    T.INGESTION_TIMESTAMP = S.INGESTION_TIMESTAMP,
    T.BATCH_ID = S.BATCH_ID
WHEN NOT MATCHED THEN INSERT (
    STOCK_ID, TRADING_DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE,
    VOLUME, ADJUSTED_CLOSE, DAILY_RETURN, SMA_5, SMA_20, RSI_14,
    INGESTION_TIMESTAMP, BATCH_ID, PROCESS_DATE
) VALUES (
    S.STOCK_ID, S.TRADING_DATE, S.OPEN_PRICE, S.HIGH_PRICE, S.LOW_PRICE, S.CLOSE_PRICE,
    S.VOLUME, S.ADJUSTED_CLOSE, S.DAILY_RETURN, S.SMA_5, S.SMA_20, S.RSI_14,
    S.INGESTION_TIMESTAMP, S.BATCH_ID, S.PROCESS_DATE
);
    '''

    # Warehouse group: load -> data quality checks
    with TaskGroup(group_id='warehouse') as warehouse:
        load_to_snowflake = SnowflakeOperator(
            task_id='load_to_snowflake',
            sql=sql_stmt,
            autocommit=True,
            snowflake_conn_id='snowflake_conn',
            warehouse=Variable.get('SNOWFLAKE_WAREHOUSE', default_var='COMPUTE_WH'),
            database='STOCK_MARKET',
            schema='PUBLIC',
            role='ACCOUNTADMIN',
        )

        data_quality_check = SnowflakeOperator(
            task_id='data_quality_check',
            sql='''
        EXECUTE IMMEDIATE $$
        DECLARE
            v_date DATE;
            v_cnt NUMBER;
        BEGIN
            v_date := TO_DATE('{{ ti.xcom_pull(task_ids=\'set_processing_date\') }}');

            -- 1) Require rows for the processing date
            SELECT COUNT(*) INTO :v_cnt
            FROM STOCK_MARKET.PUBLIC.DAILY_STOCKS
            WHERE PROCESS_DATE = :v_date;
            IF (v_cnt = 0) THEN
                CALL SYSTEM$ABORT_SESSION('DQ: No rows found for PROCESS_DATE=' || TO_VARCHAR(:v_date));
            END IF;

            -- 2) No NULLs in critical columns
            SELECT COUNT(*) INTO :v_cnt
            FROM STOCK_MARKET.PUBLIC.DAILY_STOCKS
            WHERE PROCESS_DATE = :v_date
              AND (STOCK_ID IS NULL OR TRADING_DATE IS NULL OR CLOSE_PRICE IS NULL);
            IF (v_cnt > 0) THEN
                CALL SYSTEM$ABORT_SESSION('DQ: NULL values detected in critical columns for PROCESS_DATE=' || TO_VARCHAR(:v_date));
            END IF;

            -- 3) No duplicates for business key (STOCK_ID, TRADING_DATE)
            SELECT COUNT(*) INTO :v_cnt
            FROM (
                SELECT STOCK_ID, TRADING_DATE, COUNT(*) AS c
                FROM STOCK_MARKET.PUBLIC.DAILY_STOCKS
                WHERE PROCESS_DATE = :v_date
                GROUP BY STOCK_ID, TRADING_DATE
                HAVING COUNT(*) > 1
            );
            IF (v_cnt > 0) THEN
                CALL SYSTEM$ABORT_SESSION('DQ: Duplicate STOCK_ID/TRADING_DATE rows found for PROCESS_DATE=' || TO_VARCHAR(:v_date));
            END IF;
        END;
        $$;
        ''',
            autocommit=True,
            snowflake_conn_id='snowflake_conn',
            split_statements=False,
            warehouse=Variable.get('SNOWFLAKE_WAREHOUSE', default_var='COMPUTE_WH'),
            database='STOCK_MARKET',
            schema='PUBLIC',
            role='ACCOUNTADMIN',
        )

        load_to_snowflake >> data_quality_check

    # Post-warehouse validation: compare raw vs Snowflake row counts
    @task(task_id='validate_row_counts')
    def validate_row_counts(processing_date: str):
        from airflow.exceptions import AirflowException
        try:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        except Exception as e:
            raise AirflowException(f"Snowflake provider missing: {e}")

        # Pull raw row_count from extractor XCom
        from airflow.operators.python import get_current_context as _get_ctx
        ctx = _get_ctx()
        extract_result = ctx['ti'].xcom_pull(task_ids='raw.extract_raw_parquet') or {}
        raw_count = int(extract_result.get('row_count') or 0)

        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        row = hook.get_first(
            f"SELECT COUNT(*) FROM STOCK_MARKET.PUBLIC.DAILY_STOCKS WHERE PROCESS_DATE = TO_DATE('{processing_date}')"
        )
        sf_count = int(row[0]) if row else 0

        logger.info(f"Row count comparison for {processing_date}: raw={raw_count}, snowflake={sf_count}")
        if raw_count != sf_count:
            raise AirflowException(
                f"Row count mismatch for {processing_date}: raw={raw_count}, snowflake={sf_count}"
            )
        return True

    validate_counts = validate_row_counts(processing_date)

    # Task to mark run as successful
    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    
    proceed >> raw >> etl >> warehouse >> validate_counts >> end_pipeline

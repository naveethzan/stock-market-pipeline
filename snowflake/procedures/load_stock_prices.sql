-- Stored procedure to load data from staging to fact table
CREATE OR REPLACE PROCEDURE LOAD_STOCK_PRICES(BATCH_ID STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Insert new records and update existing ones
    var merge_sql = `
        MERGE INTO FACT_STOCK_PRICES t
        USING (
            SELECT 
                s.stock_id || '_' || TO_CHAR(TO_DATE(s.trading_date), 'YYYYMMDD') as stock_price_id,
                s.stock_id as company_id,
                TO_DATE(s.trading_date) as date_id,
                s.open_price,
                s.high_price,
                s.low_price,
                s.close_price,
                s.volume,
                s.adjusted_close,
                s.daily_return,
                s.sma_5,
                s.sma_20,
                s.rsi_14,
                s.batch_id
            FROM STAGING_STOCK_PRICES s
            WHERE s.batch_id = '${BATCH_ID}'
        ) s
        ON t.company_id = s.company_id AND t.date_id = s.date_id
        WHEN MATCHED THEN
            UPDATE SET 
                t.open_price = s.open_price,
                t.high_price = s.high_price,
                t.low_price = s.low_price,
                t.close_price = s.close_price,
                t.volume = s.volume,
                t.adjusted_close = s.adjusted_close,
                t.daily_return = s.daily_return,
                t.sma_5 = s.sma_5,
                t.sma_20 = s.sma_20,
                t.rsi_14 = s.rsi_14,
                t.batch_id = s.batch_id,
                t.dwh_updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                stock_price_id, company_id, date_id, open_price, high_price, 
                low_price, close_price, volume, adjusted_close, daily_return,
                sma_5, sma_20, rsi_14, batch_id
            ) VALUES (
                s.stock_price_id, s.company_id, s.date_id, s.open_price, s.high_price, 
                s.low_price, s.close_price, s.volume, s.adjusted_close, s.daily_return,
                s.sma_5, s.sma_20, s.rsi_14, s.batch_id
            )`;
    
    // Execute the merge
    var merge_result = snowflake.execute({sqlText: merge_sql});
    
    // Get the number of rows affected
    var row_count = merge_result.getRowCount();
    
    // Log the operation
    var log_sql = `
        INSERT INTO DWH_LOAD_LOGS (
            batch_id, 
            process_name, 
            status, 
            records_processed, 
            error_message
        ) VALUES (
            '${BATCH_ID}',
            'LOAD_STOCK_PRICES',
            'SUCCESS',
            ${row_count},
            NULL
        )`;
    
    snowflake.execute({sqlText: log_sql});
    
    return `Successfully processed ${row_count} records for batch ${BATCH_ID}`;
$$;

-- Stored procedure to handle the entire ETL process
CREATE OR REPLACE PROCEDURE RUN_DAILY_ETL(BATCH_ID STRING, PROCESS_DATE DATE)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        // Step 1: Create staging table if not exists
        var create_stage_sql = `
            CREATE TABLE IF NOT EXISTS STAGING_STOCK_PRICES (
                stock_id STRING,
                trading_date STRING,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume INTEGER,
                adjusted_close FLOAT,
                daily_return FLOAT,
                sma_5 FLOAT,
                sma_20 FLOAT,
                rsi_14 FLOAT,
                batch_id STRING,
                process_date DATE,
                ingestion_timestamp TIMESTAMP_NTZ,
                dwh_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )`;
        
        snowflake.execute({sqlText: create_stage_sql});
        
        // Step 2: Truncate staging table for the current batch
        var truncate_sql = `TRUNCATE TABLE STAGING_STOCK_PRICES`;
        snowflake.execute({sqlText: truncate_sql});
        
        // Step 3: Copy data from S3 to staging
        var copy_sql = `
            COPY INTO STAGING_STOCK_PRICES
            FROM @STOCK_MARKET_STAGE/batch/date=${PROCESS_DATE}
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`;
        
        var copy_result = snowflake.execute({sqlText: copy_sql});
        
        // Step 4: Load data to dimension and fact tables
        var load_result = snowflake.execute({
            sqlText: `CALL LOAD_STOCK_PRICES('${BATCH_ID}')`
        });
        
        // Step 5: Update materialized views
        var refresh_mv = `ALTER MATERIALIZED VIEW MV_DAILY_STOCK_METRICS REFRESH`;
        snowflake.execute({sqlText: refresh_mv});
        
        return `Successfully completed ETL for batch ${BATCH_ID}`;
        
    } catch (err) {
        // Log the error
        var error_sql = `
            INSERT INTO DWH_LOAD_LOGS (
                batch_id, 
                process_name, 
                status, 
                records_processed, 
                error_message
            ) VALUES (
                '${BATCH_ID}',
                'RUN_DAILY_ETL',
                'FAILED',
                0,
                '${err.message}'
            )`;
        
        snowflake.execute({sqlText: error_sql});
        
        // Re-throw the error
        throw err;
    }
$$;

-- Dimension table for dates
CREATE OR REPLACE TABLE DIM_DATE (
    date_id DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_trading_day BOOLEAN,
    dwh_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_date PRIMARY KEY (date_id)
);

-- Create a stored procedure to populate the date dimension
CREATE OR REPLACE PROCEDURE POPULATE_DATE_DIMENSION(START_DATE DATE, END_DATE DATE)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Create date dimension records between start and end dates
    var current_date = new Date(START_DATE);
    var end_date = new Date(END_DATE);
    var row_count = 0;
    
    while (current_date <= end_date) {
        var date_id = current_date.toISOString().split('T')[0];
        var day_of_week = current_date.getDay() + 1; // 1-7 (Sunday=1)
        var day_name = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][current_date.getDay()];
        var day_of_month = current_date.getDate();
        var month = current_date.getMonth() + 1;
        var month_name = ['January','February','March','April','May','June','July','August','September','October','November','December'][current_date.getMonth()];
        var year = current_date.getFullYear();
        
        // Calculate day of year (1-366)
        var start = new Date(year, 0, 1);
        var diff = current_date - start;
        var day_of_year = Math.floor(diff / (1000 * 60 * 60 * 24)) + 1;
        
        // Calculate week of year (1-53)
        var first_day = new Date(year, 0, 1);
        var week_of_year = Math.ceil((((current_date - first_day) / 86400000) + first_day.getDay() + 1) / 7);
        
        // Calculate quarter
        var quarter = Math.ceil(month / 3);
        
        // Check if weekend
        var is_weekend = (day_of_week = 1 OR day_of_week = 7);
        
        // Assume trading day if not weekend (simplified)
        var is_trading_day = NOT is_weekend;
        
        // Insert or update the date dimension
        var sql = `
            MERGE INTO DIM_DATE t
            USING (
                SELECT 
                    '${date_id}'::DATE as date_id,
                    ${day_of_week} as day_of_week,
                    '${day_name}' as day_name,
                    ${day_of_month} as day_of_month,
                    ${day_of_year} as day_of_year,
                    ${week_of_year} as week_of_year,
                    ${month} as month_number,
                    '${month_name}' as month_name,
                    ${quarter} as quarter,
                    ${year} as year,
                    ${is_weekend} as is_weekend,
                    ${is_trading_day} as is_trading_day
            ) s
            ON t.date_id = s.date_id
            WHEN MATCHED THEN
                UPDATE SET 
                    day_of_week = s.day_of_week,
                    day_name = s.day_name,
                    day_of_month = s.day_of_month,
                    day_of_year = s.day_of_year,
                    week_of_year = s.week_of_year,
                    month_number = s.month_number,
                    month_name = s.month_name,
                    quarter = s.quarter,
                    year = s.year,
                    is_weekend = s.is_weekend,
                    is_trading_day = s.is_trading_day,
                    dwh_updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    date_id, day_of_week, day_name, day_of_month, day_of_year,
                    week_of_year, month_number, month_name, quarter, year,
                    is_weekend, is_trading_day
                ) VALUES (
                    s.date_id, s.day_of_week, s.day_name, s.day_of_month, s.day_of_year,
                    s.week_of_year, s.month_number, s.month_name, s.quarter, s.year,
                    s.is_weekend, s.is_trading_day
                )`;
        
        snowflake.execute({sqlText: sql});
        row_count++;
        
        // Move to next day
        current_date.setDate(current_date.getDate() + 1);
    }
    
    return `Inserted/updated ${row_count} date records`;
$$;

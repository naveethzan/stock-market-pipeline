{{
    config(
        materialized='table',
        dist='date_key',
        sort=['date_actual']
    )
}}

WITH dates AS (
    -- Generate dates from 2020 to 2030
    SELECT 
        DATEADD(day, seq, '2020-01-01'::DATE) AS date_actual
    FROM 
        (SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS seq 
         FROM stl_scan 
         LIMIT 4018)  -- ~11 years
),

date_dimension AS (
    SELECT
        TO_CHAR(date_actual, 'YYYYMMDD')::INTEGER AS date_key,
        date_actual,
        EXTRACT(YEAR FROM date_actual) AS year,
        EXTRACT(QUARTER FROM date_actual) AS quarter,
        EXTRACT(MONTH FROM date_actual) AS month,
        EXTRACT(DAY FROM date_actual) AS day,
        EXTRACT(DOW FROM date_actual) AS day_of_week,
        TO_CHAR(date_actual, 'Month') AS month_name,
        TO_CHAR(date_actual, 'Day') AS day_name,
        CASE 
            WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
            ELSE TRUE
        END AS is_weekday,
        CASE 
            WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN FALSE
            ELSE TRUE
        END AS is_trading_day  -- Simplified: same as weekday, complex logic handled in Spark
    FROM dates
)

SELECT * FROM date_dimension

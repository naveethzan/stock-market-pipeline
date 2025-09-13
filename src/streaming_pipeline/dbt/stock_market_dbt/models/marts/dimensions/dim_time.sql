{{
    config(
        materialized='table',
        dist='time_key',
        sort=['hour', 'minute']
    )
}}

WITH minutes AS (
    -- Generate 1440 minutes (24 hours * 60 minutes)
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 1) - 1 AS minute_of_day
    FROM stl_scan
    LIMIT 1440
),

time_dimension AS (
    SELECT
        minute_of_day AS time_key,
        minute_of_day / 60 AS hour,
        MOD(minute_of_day, 60) AS minute,
        LPAD((minute_of_day / 60)::VARCHAR, 2, '0') || ':' || 
        LPAD(MOD(minute_of_day, 60)::VARCHAR, 2, '0') || ':00' AS time_string,
        CASE
            WHEN minute_of_day >= 570 AND minute_of_day < 960 THEN TRUE  -- 9:30 AM - 4:00 PM
            ELSE FALSE
        END AS is_trading_hours,
        CASE
            WHEN minute_of_day / 60 < 12 THEN 'AM'
            ELSE 'PM'
        END AS am_pm
    FROM minutes
)

SELECT * FROM time_dimension

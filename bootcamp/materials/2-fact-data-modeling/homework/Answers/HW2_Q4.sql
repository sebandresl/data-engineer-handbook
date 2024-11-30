-- Q4 | datelist_int generation
WITH 
users_devices as (
    SELECT 
        *
    from user_devices_cumulated
    WHERE date = DATE('2023-01-31')
),
series AS (
    SELECT
        *
    FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
),
place_holder_int AS (
    SELECT 
        CASE
            WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 31 - (date - DATE(series_date))) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users_devices
    CROSS JOIN series
)
SELECT 
    user_id,
    browser_type,
    CAST(CAST(sum(placeholder_int_value) AS BIGINT) AS BIT(32))
FROM place_holder_int
GROUP BY user_id, browser_type
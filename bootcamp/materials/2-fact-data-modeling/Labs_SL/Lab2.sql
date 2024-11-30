-- DROP TABLE users_cumulated
CREATE TABLE users_cumulated (
    user_id TEXT,
    -- The list of dates in the past where the user was active
    dates_active DATE[],
    -- The current data for the user
    date DATE,
    PRIMARY KEY (user_id, date)
)

INSERT INTO users_cumulated
WITH 
today_date as (
    SELECT DATE('2023-01-31') AS today
),
yesterday as (
    select 
        *
    from users_cumulated
    -- where date = DATE('2023-01-01')
    where date = (select today from today_date) -1
),
today as (
    select 
        CAST(user_id as TEXT) as user_id,
        date(cast(event_time as timestamp)) as date_active
    from events
    where 
        -- date(cast(event_time as timestamp)) = DATE('2023-01-02')
        date(cast(event_time as timestamp)) = (select today from today_date)
        and user_id IS NOT NULL
    GROUP BY user_id, date(cast(event_time as timestamp))
)
SELECT 
    COALESCE(t.user_id, y.user_id),
    CASE 
        WHEN y.dates_active IS NULL THEN ARRAY[t.date_Active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active]  || y.dates_active       
    END AS dates_active,
    COALESCE(t.date_active, y.date + Interval '1 day') AS date
FROM today t 
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id;

select * from users_cumulated;


WITH 
users AS (
    SELECT 
        *
    from users_cumulated
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
            WHEN dates_active @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users
    CROSS JOIN series
    -- WHERE user_id = '439578290726747300'
)
-- select * from place_holder_int

SELECT 
    user_id,
    CAST(CAST(sum(placeholder_int_value) AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(sum(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0as dim_is_monthly_active,
    BIT_COUNT(CAST('111111100000000000000000000000' AS BIT(32)) & 
        CAST(CAST(sum(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active
FROM place_holder_int
GROUP BY user_id

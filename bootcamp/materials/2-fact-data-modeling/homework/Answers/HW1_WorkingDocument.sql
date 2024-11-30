-- Q1 | deduplicate game_details
SELECT
    *,
    ROW_NUMBER() OVER(partition by gd.game_id, gd.team_id, gd.player_id) as row_num
FROM game_details gd
WHERE row_num = 1

-- Q2 | DDL for user_devices_cumulated
-- DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT, 
    device_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

-- Q3 | cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH 
today_date as (
    SELECT DATE('2023-01-31') AS today
),
devices_prepped AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY device_id) as row_num
    FROM devices
),
devices_deduped AS (
    SELECT
        *
    FROM devices_prepped
    WHERE row_num = 1
),
yesterday as (
    select 
        *
    from user_devices_cumulated
    where date = (select today from today_date) - 1
),
today as (
    select 
        CAST(e.user_id AS TEXT) as user_id,
        d.browser_type as browser_type,
        date(cast(e.event_time as timestamp)) AS dates_active
    from events e
    LEFT JOIN devices_deduped d
    ON e.device_id = d.device_id
    WHERE 
        DATE(CAST(event_time AS timestamp)) = (SELECT today FROM today_date)
        AND user_id IS NOT NULL
        AND e.device_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE(CAST(event_time AS timestamp))
)
select
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) as browser_type,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.dates_active]
        WHEN t.dates_active IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.dates_active] || y.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(t.dates_active, y.date + Interval '1 day') AS date
FROM today t 
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id AND t.browser_type = y.browser_type;

-- select * from user_devices_cumulated WHERE user_id = '5439580346344408000';
-- select * from events where user_id = '5439580346344408000';
-- select * from devices where device_id IN ('5244719735692742000', '17408258267649300000');

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
    -- BIT_COUNT(CAST(CAST(sum(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_monthly_active,
    -- BIT_COUNT(CAST('111111100000000000000000000000' AS BIT(32)) & 
    --     CAST(CAST(sum(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active
FROM place_holder_int
GROUP BY user_id, browser_type


-- Q5 | DDL for hosts_cumulated
DROP TABLE hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT, 
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);

-- Q6 | incremental query to generate host_activity_datelist
WITH 
today_date AS (
    SELECT DATE('2023-01-01') AS today
),
current_data AS (
    SELECT 
        *
    FROM events
    WHERE DATE(event_time) = (select today from today_date)
),
historic_data AS (
    SELECT
        *
    FROM events
    WHERE DATE(event_time) < (select today from today_date)
)



-- Q7 | DDL for monthly reduced fact table host_activity_reduced
-- DROP TABLE host_activity_reduced;
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (month, host)
)

-- Q8 | incremental query loading host_activity_reduced
-- DELETE FROM host_activity_reduced;
INSERT INTO host_activity_reduced
WITH
today_date as (
    SELECT DATE('2023-01-04') AS today
),
daily_aggregate AS (
    select
        host,
        DATE(event_time) AS date,
        count(1) as num_site_hits,
        COUNT(DISTINCT user_id) AS num_unique_visitors
    FROM events
    WHERE
        DATE(event_time) = (select today from today_date)
        AND host IS NOT NULL
    GROUP BY host, DATE(event_time)
),
yesterday_array as (
    SELECT 
        *
    FROM host_activity_reduced 
    WHERE month = DATE_TRUNC('month', (select today from today_date))
)
select
    COALESCE(ya.month, DATE_TRUNC('month', da.date)) AS month,
    COALESCE(da.host, ya.host) AS host,
    CASE 
        WHEN ya.hit_array IS NOT NULL THEN ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.hit_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)))]) || ARRAY[COALESCE(da.num_site_hits, 0)]
    END as hit_array,
    CASE
        WHEN ya.unique_visitors IS NOT NULL THEN ya.unique_visitors || ARRAY[COALESCE(da.num_unique_visitors, 0)]
        WHEN ya.unique_visitors IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)))]) || ARRAY[COALESCE(da.num_unique_visitors, 0)]
    END AS unique_visitors
from daily_aggregate da
FULL OUTER JOIN yesterday_array ya 
ON da.host = ya.host
ON CONFLICT (host, month)
DO
    UPDATE SET hit_array = EXCLUDED.hit_array, unique_visitors = EXCLUDED.unique_visitors;

SELECT * FROM host_activity_reduced;
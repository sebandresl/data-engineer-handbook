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
-- Q6 | incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH 
today_date AS (
    SELECT DATE('2023-01-02') AS today
),
yesterday AS (
    SELECT 
        *
    FROM hosts_cumulated
    WHERE date = (select today from today_date) -1
),
today AS (
    SELECT
        host as host,
        date(cast(event_time as timestamp)) AS dates_active
    FROM events
    WHERE 
        DATE(CAST(event_time AS timestamp)) = (SELECT today FROM today_date)
        AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS timestamp))
)
SELECT
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.dates_active]
        WHEN t.dates_active IS NOT NULL THEN ARRAY[t.dates_active] || y.host_activity_datelist
        ELSE y.host_activity_datelist
    END AS host_activity_datelist,
    (select today from today_date) as date
    FROM today t
    FULL OUTER JOIN yesterday y 
    ON t.host = y.host;
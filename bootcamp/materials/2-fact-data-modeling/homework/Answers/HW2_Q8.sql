-- Q8 | incremental query loading host_activity_reduced
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
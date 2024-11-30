CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
)

DELETE FROM array_metrics;

INSERT INTO array_metrics
WITH
daily_aggregate AS (
    select
        user_id,
        DATE(event_time) AS date,
        count(1) as num_site_hits
    FROM events
    WHERE
        DATE(event_time) = DATE('2023-01-03')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array as (
    SELECT 
        *
    FROM array_metrics 
    WHERE month_start = DATE('2023-01-01')
)

select
    COALESCE(da.user_id, ya.user_id) AS user_id,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    'site_hits' as metric_name,
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)))]) || ARRAY[COALESCE(da.num_site_hits, 0)]
    END as metric_array
from daily_aggregate da
FULL OUTER JOIN yesterday_array ya 
ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO
    UPDATE SET metric_array= EXCLUDED.metric_array;


SELECT 
    cardinality(metric_array),
    count(1)
 FROM array_metrics
 GROUP BY 1;

 select * FROM array_metrics

WITH
agg AS (
    SELECT
        metric_name,
        month_start,
        ARRAY[
            sum(metric_array[1]),
            sum(metric_array[2]),
            sum(metric_array[3])
        ] as summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
SELECT 
    metric_name,
    month_start + CAST(CAST(index - 1 as TEXT) || ' day' AS INTERVAL),
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ordinality AS a(elem, index)
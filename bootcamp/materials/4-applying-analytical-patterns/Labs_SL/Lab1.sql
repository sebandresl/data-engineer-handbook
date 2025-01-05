CREATE TABLE users_growth_accounting (
    user_id TEXT,
    first_active_date DATE,
    last_active_date DATE,
    daily_active_state TEXT,
    weekly_active_state TEXT,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (user_id, date)
);

SELECT
    weekly_active_state,
    count(1)
FROM users_growth_accounting
WHERE date = DATE('2023-01-09')
GROUP BY weekly_active_state



INSERT INTO users_growth_accounting
WITH 
current_day AS (
    SELECT DATE('2023-01-09') AS day
),
yesterday AS (
    SELECT 
        *
    FROM users_growth_accounting
    WHERE date = (SELECT day from current_day) -1
),
today AS (
    SELECT
        CAST(user_id AS TEXT),
        DATE_TRUNC('day', event_time::timestamp) as today_date,
        COUNT(1)
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = (SELECT day from current_day)
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
)
SELECT
    COALESCE(t.user_id, y.user_id) as user_id,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
        WHEN  y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
        WHEN  y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END AS daily_active_state,
    CASE
        WHEN y.user_id IS NULL THEN 'New'
        WHEN  y.last_active_date >= y.date - Interval '7 day' THEN 'Retained'
        WHEN  y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
        WHEN  t.today_date IS NULL AND y.last_active_date = y.date - Interval '7 day' THEN 'Churned'
        ELSE 'Stale' 
    END AS weekly_active_state,
    COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
        CASE
            WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
            ELSE ARRAY[]::DATE[]
        END AS date_list,
    COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id

-- Growth accounting
SELECT 
    date, 
    daily_active_state,
    COUNT(1) 
FROM users_growth_accounting
GROUP BY date, daily_active_state


-- First_active_state for retention analysis rate for cohorts
SELECT 
    extract(dow from first_active_date) as dow,
    date - first_active_date AS days_since_first_active,
    CAST(COUNT(CASE WHEN daily_active_state IN ('Retained', 'Ressurected', 'New') THEN 1 END) AS REAL)/COUNT(1) AS perc_active,
    COUNT(1)
FROM users_growth_accounting
GROUP BY extract(dow from first_active_date), date - first_active_date
ORDER BY extract(dow from first_active_date), date - first_active_date ASC
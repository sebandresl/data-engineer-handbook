-- Week1 Q1 | DDL for actors table
CREATE TYPE films as (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class as ENUM('star', 'good', 'average', 'bad');

-- DROP TABLE actors;
CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
)

-- Week1 Q2 | Cumulative table generation
INSERT INTO actors
WITH 
    current_year AS (
        SELECT 2021 AS year
    ),
    yesterday AS (
        SELECT * 
        FROM actors 
        WHERE current_year = (select year from current_year) -1),
    today AS (
        select actor, 
            actorid, 
            array_agg(ROW(film, votes, rating, filmid)::films) as films,
            max(year) as latest_year,
            avg(rating) as avg_rating
        from actor_films
        where year = (select year from current_year)
        group by actor, actorid)
SELECT 
    COALESCE(t.actor, y.actor) as actor,    
    COALESCE(t.actorid, y.actorid) as actorid,
    CASE 
        WHEN y.films IS NULL
            THEN t.films
        WHEN y.films IS NOT NULL 
            THEN y.films || t.films::films[]
        ELSE y.films
    END AS films,
    CASE 
        WHEN t.avg_rating IS NULL THEN y.quality_class
        WHEN t.avg_rating > 8 THEN 'star'
        WHEN t.avg_rating > 7 THEN 'good'
        WHEN t.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    CASE 
        WHEN t.actor IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    (select year from current_year) as current_year
FROM
    today t FULL OUTER JOIN yesterday y ON t.actorid = y.actorid;


-- Week1 Q3 | DDL for actors_history_scd
DROP TABLE actors_history_scd;
CREATE TABLE actors_history_scd (
    actor TEXT,
    actorid TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER, 
    end_year INTEGER,
    current_year INTEGER,
    PRIMARY KEY (actorid, start_year)
)

-- Week 1 Q4 | Backfill query for actors_history_scd
INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT 
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_is_active
    FROM actors
    WHERE current_year <= 2020
),
with_indicators AS (
    SELECT *,
        CASE
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *, 
        SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM with_indicators
)
SELECT 
    actor,
    actorid,
    quality_class,
    is_active,
    MIN(current_year) as start_year,
    MAX(current_year) as end_year,
    2020 as current_year
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, is_active, quality_class
ORDER BY actor, actorid;

select * from actors_history_scd

-- Week1 Q5 | Incremental query for actors_history_scd
-- DROP TYPE scd_type;
CREATE TYPE scd_type as (
    quality_class quality_class,
    is_active boolean,
    start_year INTEGER,
    end_year INTEGER
);

WITH last_year_scd AS (
    select * from actors_history_scd
    WHERE current_year = 2020
    AND end_year = 2020
    ),
    historical_scd AS (
        select 
            actor,
            actorid,
            quality_class,
            is_active,
            start_year,
            end_year
        from actors_history_scd
        WHERE current_year = 2020
        AND end_year < 2020
    ),
    this_year_data as (
        select * from actors
        where current_year = 2021
    ),
    unchanged_records as(
        select 
            ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ls.start_year,
            ts.current_year as end_year
        from this_year_data ts
            JOIN last_year_scd ls
            ON ts.actorid = ls.actorid
        WHERE ts.quality_class = ls.quality_class
        AND ts.is_active = ls.is_active
    ),
    changed_records as (
        select 
            ts.actor,
            ts.actorid,
            UNNEST(ARRAY[
                ROW(
                    ls.quality_class,
                    ls.is_active,
                    ls.start_year,
                    ls.end_year
                )::scd_type,
                ROW(
                    ts.quality_class,
                    ts.is_active,
                    ts.current_year,
                    ts.current_year
                )::scd_type
            ]) as records
        from this_year_data ts
            LEFT JOIN last_year_scd ls
            ON ls.actorid = ts.actorid
        WHERE (ts.quality_class <> ls.quality_class
        OR ts.is_active <> ls.is_active)
    ),
    unnested_changed_records as (
        select 
            actor,
            actorid,
            (records::scd_type).quality_class,
            (records::scd_type).is_active,
            (records::scd_type).start_year,
            (records::scd_type).end_year
        FROM changed_records
    ),
    new_records as (
        select 
            ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ts.current_year as start_year,
            ts.current_year as end_year
        from this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
        WHERE ls.actorid IS NULL
    )

SELECT * from historical_scd

UNION ALL

select * from unchanged_records

UNION ALL

select * from unnested_changed_records

UNION ALL

select * from new_records;
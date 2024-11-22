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
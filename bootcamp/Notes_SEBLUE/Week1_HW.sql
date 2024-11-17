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
    current_year INTEGER
)


-- Week1 Q2 | Cumulative table generation
INSERT INTO actors
WITH 
    current_year AS (
        SELECT 2017 AS year
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
        where year = (select year from current_year) --2017 and actor = '50 Cent'
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


-- Testing query
-- select * from actors where actor = '50 Cent'



-- Week1 Q3 | DDL for actors_history_scd
CREATE TABLE actors_history_scd (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER
    start_date 
    end_date
)
-- Week1 Q2 | Cumulative table generation
INSERT INTO actors
WITH 
    current_year AS (
        SELECT 2020 AS year
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
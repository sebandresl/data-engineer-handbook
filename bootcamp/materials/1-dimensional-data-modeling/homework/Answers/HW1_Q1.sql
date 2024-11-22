-- Week1 Q1 | DDL for actors table
-- DROP TYPE films CASCADE;
CREATE TYPE films as (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- DROP TYPE quality_class CASCADE;
CREATE TYPE quality_class as ENUM('star', 'good', 'average', 'bad');

-- DROP TABLE actors CASCADE;
CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
)
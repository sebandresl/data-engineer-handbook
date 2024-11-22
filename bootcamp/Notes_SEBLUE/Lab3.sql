CREATE TYPE vertex_type as ENUM(
    'player', 'team', 'game'
);

CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type as ENUM(
    'plays_against', 
    'shares_team', 
    'plays_in',
    'plays_on'
);

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (
        subject_identifier,
        subject_type,
        object_identifier,
        object_type,
        edge_type
    )
);

-- Create 'game' in vertices table
insert into vertices
select 
    game_id as identifier,
    'game'::vertex_type as type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning team' , CASE WHEN home_team_id = 1 THEN home_team_id ELSE visitor_team_id END 
    ) as properties
FROM games;


INSERT INTO vertices 
with players_agg as (
    SELECT
        player_id as identifier,
        max(player_name) as player_name,
        COUNT(1) as number_of_games,
        sum(pts) as total_points,
        ARRAY_AGG(distinct team_id) as teams
    FROM game_details
    GROUP BY player_id
)
select 
    identifier, 
    'player'::vertex_type as type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    )
FROM players_agg;



INSERT INTO vertices
WITH teams_deduped as (
    select 
        *,
        ROW_NUMBER() over(partition by team_id) as row_num
    FROM teams
)
SELECT
    team_id AS identifier,
    'team'::vertex_type as type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded

    ) 
FROM teams_deduped
WHERE row_num = 1


INSERT INTO edges
with deduped as (
    select
        *,
        ROW_NUMBER() OVER(partition by player_id, game_id) as row_num
    FROM game_details
)
SELECT
    player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    game_id as object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) as properties
FROM deduped
WHERE row_num = 1;


SELECT 
    v.properties ->>'player_name',
    max(cast(e.properties->>'pts' as INTEGER))
from vertices v JOIN edges e
    on e.subject_identifier = v.identifier
    AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC


INSERT INTO edges
with 
deduped as (
    select
        *,
        ROW_NUMBER() OVER(partition by player_id, game_id) as row_num
    FROM game_details
),
filtered as (
    select * from deduped
    where row_num = 1
),
aggregated as (
    select 
        f1.player_id AS subject_player_id,
        f2.player_id as object_player_id,
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END as edge_type,
        max(f1.player_name) as subject_player_name,
        max(f2.player_name) as object_player_name,
        COUNT(1) as num_games,
        sum(f1.pts) as subject_points,
        sum(f2.pts) as object_points
    from filtered f1 
        JOIN filtered f2
        ON f1.game_id = f2.game_id
        AND f1.player_name <> f2.player_name
    WHERE f1.player_name > f2.player_name
    GROUP BY 
        f1.player_id,
        f2.player_id,
        CASE 
            WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
        END
)
select
    subject_player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id as object_identifier,
    'player'::vertex_type as object_type,
    edge_type as edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    )
from aggregated



SELECT 
    v.properties->>'player_name',
    e.object_identifier,
    CAST(v.properties->>'number_of_games' as REAL)/
    CASE 
        WHEN CAST(v.properties->>'total_points' as REAL) = 0 THEN 1 
        ELSE CAST(v.properties->>'total_points' as REAL)
    END,
    e.properties->>'subject_points',
    e.properties->>'num_games'
from vertices v 
JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type
AND v.properties->>'player_name' = 'Nik Stauskas'
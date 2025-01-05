-- Q1 | State change tracking for players
CREATE TYPE season_state as ENUM(
    'New', 
    'Retired', 
    'Continued Playing', 
    'Returned from Retirement',
    'Stayed Retired');

-- DROP TABLE players_state_tracking;
CREATE TABLE players_state_tracking (
    player_name text,
    first_active_season integer,
    last_active_season integer,
    season_state season_state,
    current_season integer,
    primary key (player_name, current_season)
 );

INSERT INTO players_state_tracking
WITH 
season_indicator AS (
    SELECT 1996 AS season
),
last_season AS (
    SELECT 
        *
    FROM players_state_tracking
    WHERE current_season = (SELECT season from season_indicator) -1
),
current_season AS (
    SELECT
        CAST(player_name AS TEXT),
        current_season as current_season,
        COUNT(1)
    FROM players
    WHERE current_season = (SELECT season from season_indicator)
    AND player_name IS NOT NULL
    GROUP BY player_name, current_season
)
SELECT
    COALESCE(t.player_name, y.player_name) as player_name,
    COALESCE(y.first_active_season, t.current_season) AS first_active_season,
    COALESCE(t.current_season, y.last_active_season) AS last_active_season,
    CASE
        WHEN y.player_name IS NULL AND t.player_name IS NOT NULL THEN 'New'
        WHEN y.last_active_season = t.current_season - 1 THEN 'Contiuned Playing'
        WHEN y.last_active_season < t.current_season - 1 THEN 'Returned from Retirement'
        WHEN t.current_season IS NULL AND y.last_active_season = y.current_season THEN 'Retired'
        ELSE 'Stayed Retired'
    END::season_state AS current_season_state,
    COALESCE(t.current_season, y.current_season + 1) as current_season
FROM current_season t
FULL OUTER JOIN last_season y
ON t.player_name = y.player_name


-- Q2 | Grouping sets for efficient aggregation of game_details
WITH
winner_team AS (
    SELECT
        CASE
            WHEN home_team_wins = 1 THEN home_team_id
            WHEN home_team_wins = 0 THEN visitor_team_id
        END AS team_id_winner
    FROM games
    GROUP BY game_id
),
winner_team_count AS (
    SELECT
        team_id_winner,
        count(1) AS team_wins
    FROM winner_team
    GROUP BY team_id_winner
    ORDER BY team_wins DESC
),
raw_analytics_table AS (
    SELECT 
        CASE
            WHEN GROUPING(player_id) = 0 AND GROUPING(team_id) = 0 THEN 'player_id__team_id'
            WHEN GROUPING(player_id) = 0 AND GROUPING(season) = 0 THEN 'player_id__season'
            WHEN GROUPING(player_id) = 0 THEN 'player_id'
            WHEN GROUPING(team_id) = 0 THEN 'team_id'
        END AS aggregation_level,
        team_id,
        player_id,
        player_name,
        SUM(pts) AS scored_points,
        MAX(team_wins) as team_wins_total,
        MAX(g.season) as season
    FROM game_details gd
    INNER JOIN winner_team_count wtc
    ON gd.team_id = wtc.team_id_winner
    INNER JOIN games g
    ON gd.game_id = g.game_id
    GROUP BY GROUPING SETS (
        (player_id, player_name, team_id),
        (player_id, player_name, season),
        (team_id)
    )
    HAVING SUM(pts) IS NOT NULL
    ORDER BY SUM(pts) DESC
)
SELECT 
    rat.aggregation_level,
    rat.team_id,
    rat.player_id,
    rat.player_name,
    rat.scored_points,
    rat.team_wins_total,
    rat.season
FROM raw_analytics_table rat
-- INNER JOIN game_details gd
-- ON rat.player_id = gd.player_id

-- Q3 | Window functions

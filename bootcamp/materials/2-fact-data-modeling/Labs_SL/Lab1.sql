DROP TABLE fct_game_details;
CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_is_playing_at_home BOOLEAN,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);

INSERT INTO fct_game_details
WITH
deduped as (
    select
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER(partition by gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) as row_num
    FROM game_details gd
    JOIN games g
    ON gd.game_id = g.game_id
    -- WHERE g.game_date_est = '2016-10-04'
)
select 
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    team_id = home_team_id AS dim_is_playing_at_home,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    COALESCE(POSITION('DNP' in comment), 0) > 0 as dim_did_not_play,
    COALESCE(POSITION('DND' in comment), 0) > 0 as dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment), 0) > 0 as dim_not_with_team,
    CAST(split_part(min, ':', 1) as real) + CAST(split_part(min, ':', 2) as real)/60 AS m_minutes ,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    fg3a as m_fg3a,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
from deduped
WHERE row_num = 1;


select 
    dim_player_name,
    COUNT(1) as num_games,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) as bailed_num,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) as REAL) / COUNT(1) as bailed_perc
FROM fct_game_details
GROUP BY 1
ORDER BY 4 DESC




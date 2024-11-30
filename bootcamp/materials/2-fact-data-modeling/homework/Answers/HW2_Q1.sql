-- Q1 | deduplicate game_details
SELECT
    *,
    ROW_NUMBER() OVER(partition by gd.game_id, gd.team_id, gd.player_id) as row_num
FROM game_details gd
WHERE row_num = 1
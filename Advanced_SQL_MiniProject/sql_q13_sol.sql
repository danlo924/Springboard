SELECT *
FROM player_mast pm
WHERE posi_to_play = 'DF'
	AND EXISTS
    (
		SELECT 1
        FROM goal_details gd
        WHERE gd.player_id = pm.player_id
    )
ORDER BY player_name;
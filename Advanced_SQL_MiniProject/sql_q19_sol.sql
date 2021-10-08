SELECT COUNT(*)
FROM player_mast pm
WHERE posi_to_play = 'GK'
	AND EXISTS
    (
		SELECT *
        FROM match_captain mc
        WHERE mc.player_captain = pm.player_id
    );
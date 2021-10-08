SELECT pm.player_name
FROM player_in_out pio
	INNER JOIN player_mast pm ON pio.player_id = pm.player_id
WHERE play_half = 1
	AND play_schedule = 'NT'
    AND in_out = 'I';
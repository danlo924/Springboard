SELECT 
	mm.play_stage
    ,COUNT(*) / 2 AS Substituions
FROM match_mast mm
	INNER JOIN player_in_out subs ON mm.match_no = subs.match_no
GROUP BY mm.play_stage;
    
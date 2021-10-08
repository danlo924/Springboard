SELECT 
	rm.referee_name
    ,sv.venue_name
    ,COUNT(*) AS num_matches
FROM referee_mast rm
	INNER JOIN match_mast mm ON rm.referee_id = mm.referee_id
    INNER JOIN soccer_venue sv ON mm.venue_id = sv.venue_id
GROUP BY 
	rm.referee_name
    ,sv.venue_name;
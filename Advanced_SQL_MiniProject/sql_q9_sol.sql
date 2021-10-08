SELECT distinct
	p.player_name
	,p.jersey_no
FROM match_mast mm
	INNER JOIN match_details md on mm.match_no = md.match_no
    INNER JOIN soccer_country sc on md.team_id = sc.country_id
    INNER JOIN player_mast p ON md.player_gk = p.player_id
WHERE mm.play_stage = 'G'
	AND sc.country_name = 'Germany'
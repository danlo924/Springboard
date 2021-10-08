SELECT pm.player_name
	,pm.jersey_no
    ,pm.playing_club
FROM player_mast pm
WHERE EXISTS
(
	SELECT *
    FROM match_details md 
		INNER JOIN soccer_country sc ON md.team_id = sc.country_id
    WHERE sc.country_name = 'England'
		AND md.player_gk = pm.player_id
)
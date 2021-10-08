SELECT pm.*
FROM player_mast pm
	INNER JOIN soccer_country sc on pm.team_id = sc.country_id
WHERE playing_club LIKE 'Liverpool%'
	AND sc.country_name = 'England';
SELECT 
	sc.country_name
    ,pp.position_id
    ,pp.position_desc
    ,COUNT(*) AS num_goals
FROM goal_details gd
	INNER JOIN soccer_country sc on gd.team_id = sc.country_id
    INNER JOIN player_mast pm on gd.player_id = pm.player_id
    INNER JOIN playing_position pp on pm.posi_to_play = pp.position_id
GROUP BY 
	sc.country_name
    ,pp.position_id
    ,pp.position_desc
ORDER BY country_name
	,position_id;

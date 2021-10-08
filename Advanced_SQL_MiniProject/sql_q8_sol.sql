SELECT m.match_no
	,c.country_name
FROM match_details m
    INNER JOIN soccer_country c ON m.team_id = c.country_id
WHERE m.match_no = 
(
	SELECT p.match_no
	FROM penalty_shootout p
	ORDER BY kick_no DESC
	LIMIT 1
);
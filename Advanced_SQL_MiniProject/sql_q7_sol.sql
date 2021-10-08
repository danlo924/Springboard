SELECT DISTINCT(v.venue_name) AS Venue
FROM match_mast m
	INNER JOIN soccer_venue v on m.venue_id = v.venue_id
WHERE decided_by = 'P';
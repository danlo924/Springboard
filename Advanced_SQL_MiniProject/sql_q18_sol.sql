SELECT match_no
	,COUNT(*) as foul_cards
FROM player_booked
GROUP by match_no
ORDER BY foul_cards DESC
LIMIT 1;
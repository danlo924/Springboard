SELECT b.referee_id
	,b.referee_name
    ,b.Bookings
FROM
(
	SELECT rm.referee_id
		,rm.referee_name
		,COUNT(*) Bookings
        ,RANK() OVER (ORDER BY COUNT(*) DESC) AS BookingRank
	FROM referee_mast rm
		LEFT OUTER JOIN match_mast mm ON rm.referee_id = mm.referee_id
		LEFT OUTER JOIN player_booked pb ON pb.match_no = mm.match_no
	GROUP BY rm.referee_id
		,rm.referee_name
) b
WHERE b.BookingRank = 1;

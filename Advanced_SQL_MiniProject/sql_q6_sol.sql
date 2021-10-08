SELECT COUNT(*)
FROM match_details m1
	INNER JOIN match_details m2 ON m1.match_no = m2.match_no
WHERE m1.win_lose = 'W'
	AND m1.decided_by = 'N'
	AND m2.win_lose = 'L'
    AND m1.goal_score - m2.goal_score = 1;
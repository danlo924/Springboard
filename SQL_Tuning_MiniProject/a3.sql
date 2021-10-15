-- EXPLAIN PLAN BEFORE OPTIMIZATION:
-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 	1	SIMPLE	<subquery2>		ALL						100.00	Using where
-- 	1	SIMPLE	Student		eq_ref	PRIMARY	PRIMARY	4	<subquery2>.studId	1	100.00	
-- 	2	MATERIALIZED	Transcript		ALL					100	10.00	Using where

-- THERE IS A FULL TABLE SCAN ON THE transcript TABLE
-- ADDING AN INDEX ON THIS TABLE FOR THE FILTER CLAUSE (crsCode) AND studId SHOULD PROVIDE THE BEST PERFORMANCE

CREATE INDEX ix_crsCode_studID ON transcript (crsCode, studID);

-- EXPLAIN PLAN AFTER ADDING THE INDEX:
-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 	1	SIMPLE	Transcript		ref	ix_crsCode_studID	ix_crsCode_studID	1023	const	2	100.00	Using where; Using index; LooseScan
-- 	1	SIMPLE	Student		eq_ref	PRIMARY	PRIMARY	4	springboardopt.Transcript.studId	1	100.00	

-- THE QUERY PLAN THEN USES 2 SEEKS INSTEAD OF FULL TABLE SCANS
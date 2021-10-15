-- THE PK CLUSTERED INDEX THAT WAS CREATED FOR q1 IS STILL THE BEST SOLUTION FOR THIS QUERY:
	-- ALTER TABLE student ADD CONSTRAINT pk_student PRIMARY KEY CLUSTERED (id)
-- SINCE THE QUERY IS LOOKING FOR A RANGE OF VALUES, THE RANGE SCAN IS THE BEST WE CAN HOPE FOR
-- EXECUTION PLAN SHOWS THE FOLLOWING:
	-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
	-- 	1	SIMPLE	Student		range	PRIMARY	PRIMARY	4		278	100.00	Using where
-- THERE IS NOT REALLY ANY OTHER WAY TO IMPROVE THIS QUERY OUTSIDE OF THE PK CLUSTERED THAT HAS ALREADY BEEN CREATED
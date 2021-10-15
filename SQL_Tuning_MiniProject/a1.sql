-- 	EXPLAIN PLAN BEFORE: 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 							1	SIMPLE	Student		ALL					400	10.00	Using where

-- EXPLAIN ANALYZE BEFORE: -> Filter: (student.id = <cache>((@v1)))  (cost=41.00 rows=40) (actual time=0.216..0.931 rows=1 loops=1)
--      -> Table scan on Student  (cost=41.00 rows=400) (actual time=0.034..0.838 rows=400 loops=1)

-- THERE IS A FULL TABLE SCAN SINCE THERE ISN'T ANY PK OR CLUSTERED INDEX ON THE student TABLE
-- SINCE id IS UNIQUE IN THE TABLE IT WOULD BE A GOOD CHOICE FOR THE PK / CLUSTERED INDEX FOR THE student TABLE

ALTER TABLE student ADD CONSTRAINT pk_student PRIMARY KEY CLUSTERED (id); 
-- ALTER TABLE student DROP PRIMARY KEY;

-- 	EXPLAIN PLAN AFTER: 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 							1	SIMPLE	Student		const	PRIMARY	PRIMARY	4	const	1	100.00	

-- EXPLAIN ANAYZE AFTER: -> Rows fetched before execution  (cost=0.00..0.00 rows=1) (actual time=0.000..0.000 rows=1 loops=1)

-- AFTER THE PK CLUSTERED INDEX IS APPLIED TO THE TABLE ON (id), THE EXECUTION PLAN SHOWS type=CONST, which is a SEEK OPERATION
-- THE COSE BECOMES PRACTICALLY NEGLIGBLE
 
SELECT * 
FROM INFORMATION_SCHEMA.STATISTICS
WHERE table_schema = 'springboardopt';
-- INITIALL THERE ARE FULL TABLE SCANS ON teacher AND professor TABLES
-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 	1	SIMPLE	Teaching		ALL					100	100.00	Using where
-- 	1	SIMPLE	Transcript		ref	ix_crsCode_studID	ix_crsCode_studID	1023	springboardopt.Teaching.crsCode	1	10.00	Using index condition; Using where
-- 	1	SIMPLE	Student		eq_ref	PRIMARY	PRIMARY	4	springboardopt.Transcript.studId	1	100.00	
-- 	1	SIMPLE	Professor		ALL					400	1.00	Using where; Using join buffer (hash join)

-- BOTH teaching AND professor TABLES NEED PK CLUSTERED INDEXES:
-- ALTER TABLE professor ADD CONSTRAINT pk_professor PRIMARY KEY CLUSTERED (id); 
-- ALTER TABLE teaching ADD CONSTRAINT pk_teaching PRIMARY KEY CLUSTERED (crsCode, semester); 

-- ALSO, SINCE THE MAIN FILTER IS ON professor.name, A NONCLUSTERED INDEX ON THAT COLUMN MAKES SENSE:
-- CREATE INDEX ix_prof_name ON professor (name);

-- AND SINCE THE JOIN FROM professor TO teaching TABLE IS ON teach.profID, WE NEED AN INDEX ON THAT COLUMN:
-- CREATE INDEX ix_teaching_profID ON teaching (profID);

-- IT WOULD ALSO BE BETTER TO REWRITE THE QUERY AS A SERIES OF INNER JOINS INSTEAD OF SUB-QUERIES:
SET @v5 = 'Amber Hill';
EXPLAIN
SELECT s.name
FROM student s 
	INNER JOIN transcript t on s.id = t.studID
    INNER JOIN teaching tch on t.crsCode = tch.crsCode
		AND t.semester = tch.semester
	INNER JOIN professor p ON tch.profId = p.id
WHERE p.name = @v5;

-- AFTER THE INDEXES ARE CREATED, THE EXPLAIN PLAN CONSTAINS INDEX SEEKS ONLY:
-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 	1	SIMPLE	p		ref	PRIMARY,ix_prof_name	ix_prof_name	1023	const	1	100.00	Using index
-- 	1	SIMPLE	tch		ref	PRIMARY,ix_teaching_profID	ix_teaching_profID	5	springboardopt.p.id	1	100.00	Using index
-- 	1	SIMPLE	t		ref	ix_crsCode_studID	ix_crsCode_studID	1023	springboardopt.tch.crsCode	1	10.00	Using index condition; Using where
-- 	1	SIMPLE	s		eq_ref	PRIMARY	PRIMARY	4	springboardopt.t.studId	1	100.00	
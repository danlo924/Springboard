SET @v6 = 'MGT';
SET @v7 = 'EE';			  

-- 5. List the names of students who have taken a course from department v6 (deptId), but not v7.
-- EXPLAIN 
-- SELECT * FROM Student, 
-- 	(SELECT studId FROM Transcript, Course WHERE deptId = @v6 AND Course.crsCode = Transcript.crsCode
-- 	AND studId NOT IN
-- 	(SELECT studId FROM Transcript, Course WHERE deptId = @v7 AND Course.crsCode = Transcript.crsCode)) as alias
-- WHERE Student.id = alias.studId
-- order by student.id;

-- BEFORE OPTIMIZATION EXPLAIN PLAN:
-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 	1	PRIMARY	Course		ALL					100	10.00	Using where; Using temporary; Using filesort
-- 	1	PRIMARY	Transcript		ref	ix_crsCode_studID	ix_crsCode_studID	1023	springboardopt.Course.crsCode	1	100.00	Using where; Using index
-- 	1	PRIMARY	Student		eq_ref	PRIMARY	PRIMARY	4	springboardopt.Transcript.studId	1	100.00	Using where
-- 	3	DEPENDENT SUBQUERY	Course		ALL					100	10.00	Using where
-- 	3	DEPENDENT SUBQUERY	Transcript		ref_or_null	ix_crsCode_studID	ix_crsCode_studID	1028	springboardopt.Course.crsCode,func	2	100.00	Using where; Using index; Full scan on NULL key

-- THERE ARE A COUPLE ISSUES WITH THE INITIAL QUERY: 
	-- THE QUERY RETURNS DUPLICATE ROWS SINCE THE LIST OF studID FROM transcript TABLE CAN CONTAIN THE SAME STUDENT MORE THAN ONCE
    -- THERE IS A FULL TABLE SCAN ON course TABLE WHEN FILTERING BY deptID
    
-- TO RESOLVE THE ISSUE, I SUGGEST USING THE FOLLOWING CORRELATED QUERY TO GET A DISTINCT LIST OF students:
EXPLAIN
SELECT *
FROM student s
WHERE EXISTS(SELECT 1 FROM transcript t INNER JOIN course c ON t.crsCode = c.crsCode WHERE c.deptId = @v6 AND t.studId = s.id)
	AND NOT EXISTS(SELECT 1 FROM transcript t INNER JOIN course c ON t.crsCode = c.crsCode WHERE c.deptId = @v7 AND t.studId = s.id);
    
-- AND ADD AN INDEX ON course.deptID, crsCode FOR QUICK FILTERING AND THEN JOINING TO transcript TABLE:
-- CREATE INDEX ix_course_deptID_crsCode ON course (deptID, crsCode);

-- AFTERWARD, THE EXPLAIN PLAN LOOKS MUCH BETTER:
-- 	id	select_type	table	partitions	type	possible_keys	key	key_len	ref	rows	filtered	Extra
-- 	1	SIMPLE	<subquery2>		ALL						100.00	Using where
-- 	1	SIMPLE	s		eq_ref	PRIMARY	PRIMARY	4	<subquery2>.studId	1	100.00	
-- 	1	SIMPLE	c		ref	ix_course_deptID_crsCode	ix_course_deptID_crsCode	1023	const	25	100.00	Using where; Not exists; Using index
-- 	1	SIMPLE	t		ref	ix_crsCode_studID	ix_crsCode_studID	1028	springboardopt.c.crsCode,<subquery2>.studId	1	100.00	Using where; Using index
-- 	2	MATERIALIZED	c		ref	ix_course_deptID_crsCode	ix_course_deptID_crsCode	1023	const	26	100.00	Using where; Using index
-- 	2	MATERIALIZED	t		ref	ix_crsCode_studID	ix_crsCode_studID	1023	springboardopt.c.crsCode	1	100.00	Using index
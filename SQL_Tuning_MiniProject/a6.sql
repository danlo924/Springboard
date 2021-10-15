SET @v8 = 'MAT';

-- 6. List the names of students who have taken all courses offered by department v8 (deptId).
SELECT name FROM Student,
	(SELECT studId
	FROM Transcript
		WHERE crsCode IN
		(SELECT crsCode FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))
		GROUP BY studId
		HAVING COUNT(*) = 
			(SELECT COUNT(*) FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))) as alias
WHERE id = alias.studId;

-- THE QUERY COULD BE BETTER REWRITTEN BY FIRST FINDING THE COUNT OF COURSE IN THE DEPT, THEN FILTERING FROM THERE:
SET @dptCnt = (SELECT COUNT(*) FROM course WHERE deptID = @v8);
SELECT name, COUNT(*) AS total
FROM student s
	INNER JOIN transcript t ON s.id = t.studId
    INNER JOIN course c on t.crsCode = c.crsCode
WHERE c.deptId = @v8
GROUP BY s.name
HAVING COUNT(*) = @dptCnt;


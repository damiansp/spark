-- errs if ansi.enabled=True
SELECT CAST('a' AS INT); 
SELECT CAST(2147483648L AS INT);
SELECT CAST('2020-01-01' AS INT);

-- if ansi.enabled=False:
SELECT CAST('a' AS INT);          -- NULL
SELECT CAST(2147483648L AS INT);  -- -2147483648 (overflow)
SELECT CAST('2020-01-01' AS INT); -- NULL


CREATE TABLE t (v INT);
INSERT INTO t VALUES('1');  -- err if ansi.enabled else 1

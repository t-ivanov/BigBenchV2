--
-- Find the average number of sessions per registered user per month.
-- Display the top ten users.
--

-- 10 min timeout
set session_time=600; 
set TEMP_TABLE1=sessions;

-- set the database
use BigBenchV2;

DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE VIEW ${hiveconf:TEMP_TABLE1} AS
SELECT
	uid,
	tstamp,
	CONCAT(sessionize.uid, 
       CONCAT('_', 
        SUM(new_session) OVER (PARTITION BY sessionize.uid ORDER BY sessionize.tstamp)
       )
      ) AS session_id
FROM (
    SELECT
      lg.wl_customer_id 			AS uid,
      unix_timestamp(lg.wl_timestamp)	AS tstamp
	  ,CASE
        WHEN (unix_timestamp(lg.wl_timestamp) - lag (unix_timestamp(lg.wl_timestamp))
                 OVER (PARTITION BY lg.wl_customer_id ORDER BY lg.wl_timestamp)) >= ${hiveconf:session_time}
		  THEN 1
          ELSE 0
       END AS new_session 
    FROM 
		spark_logs lg
    WHERE 
		lg.wl_customer_id IS NOT NULL
    CLUSTER BY uid	
) sessionize
CLUSTER BY session_id, uid, tstamp
;

SELECT 
	c_customer_id,
	c_name, 
	count(*)/24	as cnt	-- Two years of data
FROM 
	${hiveconf:TEMP_TABLE1} s, 
	customers c
WHERE 
	s.uid = c.c_customer_id
GROUP BY c.c_customer_id, c.c_name
ORDER BY cnt desc
LIMIT 10;

--cleanup --------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};

-- hive  SF1 clicks.json
--9210    customer#00009210       24.791666666666668
--11719   customer#00011719       23.0
--11123   customer#00011123       22.875
--665     customer#00000665       22.75
--941     customer#00000941       22.666666666666668
--5254    customer#00005254       22.625
--18499   customer#00018499       22.583333333333332
--16140   customer#00016140       22.541666666666668
--1536    customer#00001536       22.333333333333332
--8423    customer#00008423       22.291666666666668
--Time taken: 519.97 seconds, Fetched: 10 row(s)

-- saprk-sql
-- 9210    customer#00009210       24.791666666666668
-- 665     customer#00000665       22.75
-- 941     customer#00000941       22.666666666666668
-- 5254    customer#00005254       22.625
-- 1536    customer#00001536       22.333333333333332
-- 8423    customer#00008423       22.291666666666668
-- 1622    customer#00001622       22.208333333333332
-- 1642    customer#00001642       22.125
-- 6957    customer#00006957       21.833333333333332
-- 10529   customer#00010529       21.75
-- Time taken: 217.044 seconds, Fetched 10 row(s)


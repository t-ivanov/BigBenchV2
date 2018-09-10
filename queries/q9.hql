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
      logs.wl_customer_id 			AS uid,
      unix_timestamp(logs.wl_ts)	AS tstamp
	  ,CASE
        WHEN (unix_timestamp(logs.wl_ts) - lag (unix_timestamp(logs.wl_ts))
                 OVER (PARTITION BY logs.wl_customer_id ORDER BY logs.wl_ts)) >= ${hiveconf:session_time}
		  THEN 1
          ELSE 0
       END AS new_session 
    FROM 
		web_logs
			lateral view 
			json_tuple(
			web_logs.line,
			'wl_customer_id',
			'wl_item_id',
			'wl_timestamp'
		) logs as 
			wl_customer_id,
			wl_item_id,
			wl_ts
    WHERE 
		logs.wl_customer_id IS NOT NULL
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
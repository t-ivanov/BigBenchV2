--
-- Find the average time amount of time a user spends on the retailer website.
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
  session_id,
  min(tstamp) as startTime,
  max(tstamp) as endTime
FROM
	(
	select
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
	 (
	   select
		   js.wl_customer_id,
		   js.wl_item_id,
		   js.wl_ts
	   from web_logs
			lateral view 
			json_tuple(
			web_logs.line,
			'wl_customer_id',
			'wl_item_id',
			'wl_timestamp'
		) js as 
			wl_customer_id,
			wl_item_id,
			wl_ts
	    WHERE 
		  js.wl_customer_id is not NULL
		  and js.wl_item_id is not NULL
	 ) logs	
    CLUSTER BY uid
  ) sessionize
  CLUSTER BY uid, session_id
) temp
GROUP BY uid, session_id
;

select 
	avg(s.endTime - s.startTime)
from
	${hiveconf:TEMP_TABLE1} s;


-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 42 minutes 44 seconds 670 msec
--OK
--625.9108834897341
--Time taken: 451.565 seconds, Fetched: 1 row(s)

-- hive current result with lag
--1.402544081798601E7
--Time taken: 335.893 seconds, Fetched: 1 row(s)


--- spartk result with lag
-- 650.0588537274027
--Time taken: 196.54 seconds, Fetched 1 row(s)

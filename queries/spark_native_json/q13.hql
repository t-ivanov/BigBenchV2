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
	where 
	lg.wl_customer_id is not NULL
	and lg.wl_item_id is not NULL
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


--- spark result with lag
-- 650.0588537274027
--Time taken: 196.54 seconds, Fetched 1 row(s)

--- spark result with lag
-- 1844.3899734105214
-- Time taken: 210.787 seconds, Fetched 1 row(s)

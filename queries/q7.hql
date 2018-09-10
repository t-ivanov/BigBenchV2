--
-- List users with more than 10 sessions.
-- A session is defined as a 10-minute window of clicks by a 
-- registered user.
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
    item,
    wptype,
    tstamp,
	CONCAT(sessionize.uid, 
		CONCAT('_', 
			SUM(new_session) OVER (PARTITION BY sessionize.uid ORDER BY sessionize.tstamp)
       )
      ) AS session_id
FROM (
    SELECT
      logs.wl_customer_id 			AS uid,
      logs.wl_item_id 				AS item,
      w.w_web_page_type     		AS wptype,
      unix_timestamp(logs.wl_ts)	AS tstamp
	  , CASE
        WHEN (unix_timestamp(logs.wl_ts) - lag (unix_timestamp(logs.wl_ts))
             OVER (PARTITION BY logs.wl_customer_id ORDER BY logs.wl_ts)) >= ${hiveconf:session_time}
		THEN 1
        ELSE 0
      END AS new_session   
    FROM 
		web_pages w,
		(
		select 
			js.wl_customer_id,
			js.wl_item_id,
			js.wl_webpage_name,
			js.wl_ts
		from web_logs
			lateral view 
				json_tuple(
				web_logs.line,
				'wl_customer_id',
				'wl_item_id',
				'wl_webpage_name',
				'wl_timestamp'
			) js as 
			wl_customer_id,
			wl_item_id,
			wl_webpage_name,
			wl_ts
		where
			js.wl_customer_id is not NULL 
			and js.wl_item_id is not NULL
	) logs
    WHERE 
		logs.wl_webpage_name = w.w_web_page_name
    CLUSTER BY uid
) sessionize
CLUSTER BY session_id, uid, tstamp
;

SELECT 
	c.c_customer_id,
	c.c_name,
	count(*) as cnt_se
FROM
	${hiveconf:TEMP_TABLE1} s,
	customers c
WHERE 
	c.c_customer_id = s.uid
GROUP BY c_customer_id, c_name
HAVING cnt_se > 10
ORDER BY cnt_se desc
LIMIT 50;

--cleanup --------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
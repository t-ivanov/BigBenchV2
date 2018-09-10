--Shopping cart abandonment analysis: For users who added products in
--their shopping carts but did not check out in the online store, find the average
--number of pages they visited during their sessions.

-- set the database
use BigBenchV2;

-- Query parameters
set RESULT_TABLE=q4_results;
set TEMP_TABLE1=sessions;
set TEMP_TABLE2=cart_abandon;
set q04_timeout=600;

-- Part 1: sessionazing -----------
-- more info: https://www.dataiku.com/learn/guide/code/reshaping_data/sessionization.html
DROP View IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE View ${hiveconf:TEMP_TABLE1} AS
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
		logs.wl_customer_id 		AS uid,
		logs.wl_item_id 			AS item,
		w.w_web_page_type     		AS wptype,
		unix_timestamp(logs.wl_ts)	AS tstamp
        , CASE
            WHEN (unix_timestamp(logs.wl_ts) - lag (unix_timestamp(logs.wl_ts))
                 OVER (PARTITION BY logs.wl_customer_id ORDER BY logs.wl_ts)) >= ${hiveconf:q04_timeout}
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


-- Part 2: Abandoned shopping carts ----------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE VIEW ${hiveconf:TEMP_TABLE2} AS
SELECT
	sid,
	uid,
	times[0] as start_s,
	times[size_times - 1] as end_s,
	count(*) as pages
FROM matchpath
(
 on ${hiveconf:TEMP_TABLE1} 
	partition by uid, session_id
	order by tstamp
	arg1('A*.B.C*'),
	arg2('A'),arg3( wptype not in ('add to cart')),
	arg4('B'),arg5( wptype in ('add to cart')),  -- add to cart
	arg6('C'),arg7( wptype not in ('checkout')), -- checkout pages
	arg8('tpath[0].session_id as sid, 
			tpath[0].uid as uid,
			tpath.tstamp as times,
			size(tpath.tstamp) as size_times')
)
WHERE 
	sid is not NULL 
	AND uid is not NULL
GROUP BY sid, uid, times[0], times[size_times - 1]
;

--select * from  ${hiveconf:TEMP_TABLE2} limit 100;

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  c_customer_id 	BIGINT,
  c_name     		STRING,
  s_pages 			BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT 
	c.c_customer_id,
	c.c_name,
	avg(ca.pages) AS s_pages
FROM 
	${hiveconf:TEMP_TABLE2} ca,
	customers c
WHERE 
	ca.uid = c.c_customer_id
GROUP BY c.c_customer_id, c.c_name
ORDER BY s_pages desc, c.c_customer_id
limit 50;

--cleanup --------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};

-- hive SF1 run
--Total MapReduce CPU Time Spent: 0 days 2 hours 24 minutes 12 seconds 450 msec
--OK
--Time taken: 915.207 seconds

--15568   customer#00015568       1
--4961    customer#00004961       1
--6048    customer#00006048       1
--3154    customer#00003154       1
--5237    customer#00005237       1

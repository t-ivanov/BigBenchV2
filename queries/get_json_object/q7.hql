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
			get_json_object(web_logs.line, '$.wl_customer_id') as wl_customer_id,
			get_json_object(web_logs.line, '$.wl_item_id') as wl_item_id,
			get_json_object(web_logs.line, '$.wl_webpage_name') as wl_webpage_name,
			get_json_object(web_logs.line, '$.wl_timestamp') as wl_ts
		from web_logs
	) logs
    WHERE logs.wl_customer_id is not NULL 
		and logs.wl_item_id is not NULL
		and logs.wl_webpage_name = w.w_web_page_name
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

-- hive SF1 clicks.json
--9210    customer#00009210       530
--941     customer#00000941       498
--665     customer#00000665       495
--5254    customer#00005254       488
--1536    customer#00001536       477
--1622    customer#00001622       476
--8423    customer#00008423       476
--8552    customer#00008552       475
--1642    customer#00001642       473
--6957    customer#00006957       471
--5469    customer#00005469       468
--5702    customer#00005702       464
--1768    customer#00001768       463
--8871    customer#00008871       461
--7170    customer#00007170       459
--771     customer#00000771       459
--7064    customer#00007064       458
--2514    customer#00002514       456
--537     customer#00000537       452
--5575    customer#00005575       450
--9316    customer#00009316       449
--5639    customer#00005639       449
--1853    customer#00001853       449
--5128    customer#00005128       447
--6785    customer#00006785       445
--725     customer#00000725       444
--7169    customer#00007169       443
--8318    customer#00008318       442
--599     customer#00000599       440
--7552    customer#00007552       440
--2871    customer#00002871       439
--3681    customer#00003681       439
--9549    customer#00009549       439
--6681    customer#00006681       438
--2025    customer#00002025       437
--1872    customer#00001872       435
--5789    customer#00005789       434
--1874    customer#00001874       434
--4552    customer#00004552       434
--6850    customer#00006850       434
--5937    customer#00005937       433
--4534    customer#00004534       433
--5300    customer#00005300       432
--8974    customer#00008974       432
--2982    customer#00002982       432
--5915    customer#00005915       432
--1154    customer#00001154       431
--9592    customer#00009592       431
--5043    customer#00005043       431
--156     customer#00000156       431
--Time taken: 434.061 seconds, Fetched: 50 row(s)




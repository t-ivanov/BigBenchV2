--Find all customers who viewed items of a given category on the web
--in a given month and year that was followed by an in-store purchase of an item from the same category in the three
--consecutive months.

-- set the database
use BigBenchV2;
-- Resources
set RESULT_TABLE=q12_results;
set q12_startDate=2014-09-02;
set q12_endDate1=2014-10-02;
set q12_endDate2=2014-12-02;
set q12_i_category_IN='cat#03', 'cat#11';

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  c_date	string,
  s_date	string,
  i_id    	BIGINT,
  u_id    	BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
-- the real query part
SELECT 
	DISTINCT wcsView.wl_timestamp, 
			storeView.ss_ts,
            wcsView.wl_item_id,
            wcsView.wl_customer_id
FROM( 
  SELECT 
	logs.wl_item_id,
    logs.wl_customer_id,
	logs.wl_timestamp,
	i.i_category_name
  FROM 
	(
	select 
			js.wl_customer_id,
			js.wl_item_id,
			js.wl_webpage_name,
			js.wl_timestamp 
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
			wl_timestamp
		WHERE
		js.wl_customer_id IS NOT NULL
	) logs
-- filter given category 
  JOIN items i ON (logs.wl_item_id = i.i_item_id AND i.i_category_name IN (${hiveconf:q12_i_category_IN}) )
  WHERE 
	logs.wl_customer_id IS NOT NULL
	-- filter given month and year 
	AND to_date(logs.wl_timestamp) >= '${hiveconf:q12_startDate}' 
	AND to_date(logs.wl_timestamp) <= '${hiveconf:q12_endDate1}' 
) wcsView
JOIN( 
  SELECT 
	ss.ss_item_id,
    ss.ss_customer_id,
    ss.ss_ts,
	i.i_category_name
  FROM store_sales ss
-- filter given category 
  JOIN items i ON (ss.ss_item_id = i.i_item_id  AND i.i_category_name IN (${hiveconf:q12_i_category_IN})) 
  WHERE 
	ss.ss_customer_id IS NOT NULL
	-- filter given month and year + 3 consecutive months
	AND to_date(ss.ss_ts) >= '${hiveconf:q12_startDate}' 
	AND to_date(ss.ss_ts) <= '${hiveconf:q12_endDate2}'
) storeView
ON (wl_customer_id = ss_customer_id)
-- filter 3 consecutive months: buy AFTER view on website
WHERE 
	wl_timestamp < ss_ts
	AND wcsView.i_category_name = storeView.i_category_name
CLUSTER BY wl_timestamp,
           wl_item_id,
		   wl_customer_id 
;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 45 minutes 36 seconds 540 msec
--Time taken: 244.217 seconds
--Total MapReduce CPU Time Spent: 45 minutes 31 seconds 220 msec
--Time taken: 245.848 seconds

--...
--2014-10-02 14:07:11     2014-10-27 03:10:16     1083    2389
--2014-10-02 14:08:48     2014-10-27 03:10:16     741     2389
--2014-10-02 14:09:03     2014-10-27 03:10:16     741     2389
--2014-10-02 14:09:15     2014-10-24 18:39:43     186     13984
--2014-10-02 17:14:12     2014-11-10 06:59:32     1045    12838
--2014-10-02 18:44:40     2014-11-19 04:17:42     125     6823
--2014-10-02 18:49:36     2014-11-19 04:17:42     11      6823
--2014-10-02 20:02:02     2014-11-09 06:49:13     589     5381
--...



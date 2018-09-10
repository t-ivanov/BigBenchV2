--For online sales, compare the total sales in which customers checked
--online reviews before making the purchase and that of sales in which customers
--did not read reviews. Consider only online sales for a specific category in a given
--year.

-- set the database
use BigBenchV2;

set TEMP_TABLE1=q08_temp1;
set TEMP_TABLE2=q08_temp2;
set RESULT_TABLE=q08_results;
--set q08_category="product look up";
-- web logs date range + 1year
set q08_startDate=2012-09-02;
set q08_endDate=2013-09-02;

--PART 1 - sales that users have viewed the review pages--------------------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE VIEW IF not exists ${hiveconf:TEMP_TABLE1} AS
SELECT 
	DISTINCT s_sk,
	c_date
FROM  matchpath
(
  on (
		SELECT 
			logs.wl_customer_id    			AS uid,
			logs.wl_timestamp 				AS c_date,
		--	substr(logs.wl_timestamp, 12) 	AS c_time,
			logs.wl_transaction_id      	AS sales_sk, -- wcs_sales_sk
			w.w_web_page_type      			AS wpt
		FROM(
			SELECT
				js.wl_transaction_id,
				js.wl_item_id,
				js.wl_webpage_name,
				js.wl_customer_id,
				js.wl_timestamp
			from web_logs
				lateral view 
				json_tuple(
				web_logs.line,
				'wl_id',
				'wl_item_id',
				'wl_webpage_name',
				'wl_customer_id',
				'wl_timestamp'
			) js as 
				wl_transaction_id,
				wl_item_id,
				wl_webpage_name,
				wl_customer_id,
				wl_timestamp
			WHERE
				js.wl_customer_id IS NOT NULL
				AND	to_date(js.wl_timestamp) >= '${hiveconf:q08_startDate}'
				AND to_date(js.wl_timestamp) <= '${hiveconf:q08_endDate}'
			) logs		
		INNER JOIN web_pages w 
		ON logs.wl_webpage_name = w.w_web_page_name
		CLUSTER BY uid
	)joined_tables
	partition by uid
	order by c_date
	arg1('A+.C*'),
	arg2('A'),arg3( wpt in ('product look up')),
	arg4('C'),arg5( wpt not in ('product look up')),
	--arg6('B'),arg7( sales_sk is not NULL),
	arg7('tpath.sales_sk[0] as s_sk, tpath.c_date[0] as c_date')
) npath	
;


--PART 2 - helper table: sales within one year starting 2012-09-02  ---------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE VIEW IF NOT EXISTS ${hiveconf:TEMP_TABLE2} AS
SELECT 
	(ws.ws_quantity * i.i_price) as totalprice,
	ws_transaction_id
FROM 
	web_sales ws,
	items i
WHERE
	to_date(ws.ws_ts ) >= '${hiveconf:q08_startDate}'
	AND to_date(ws.ws_ts ) <= '${hiveconf:q08_endDate}'
	AND ws.ws_item_id = i.i_item_id
;

--select * from ${hiveconf:TEMP_TABLE2} limit 100;

--PART 3 - for sales in given year, compute sales in which customers checked online reviews (product look up) vs. sales in which customers did not read reviews.
--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
	q08_review_sales_amount    DOUBLE,
	no_q08_review_sales_amount DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query part----------------------------------------------------------------------´
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
  q08_review_sales.amount AS q08_review_sales_amount,
  q08_all_sales.amount - q08_review_sales.amount AS no_q08_review_sales_amount
FROM (
  SELECT 
	1 AS id,
	SUM(totalprice) as amount
  FROM ${hiveconf:TEMP_TABLE2} ws
  INNER JOIN ${hiveconf:TEMP_TABLE1} sr 
  ON ws.ws_transaction_id = sr.s_sk
) q08_review_sales
JOIN (
  SELECT 
	1 AS id,
	SUM(totalprice) as amount
  FROM ${hiveconf:TEMP_TABLE2} ws
)  q08_all_sales
ON q08_review_sales.id = q08_all_sales.id
;


--cleanup-------------------------------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};

-- result hive
--171776.57       5285057.18
--Time taken: 0.812 seconds, Fetched: 1 row(s)

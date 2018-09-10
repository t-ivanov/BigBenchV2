--Customer segmentation analysis: Customers are separated along the
--following key shopping dimensions: recency of last visit, frequency of visits and
--monetary amount. Use the store and online purchase data during a given year
--to compute.

-- set the database
use BigBenchV2;
-- Query parameters
-- store_sales and web_sales date
set q25_date=2013-01-02;
set TEMP_RESULT_TABLE=q25_results;
set TEMP_TABLE=q25_temp;

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE TABLE ${hiveconf:TEMP_TABLE} (
  cid     BIGINT,
  oid     BIGINT,
  dateid  BIGINT,
  amount  DOUBLE
);

INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  ss_customer_id AS cid,
  ss_transaction_id  AS oid,
  unix_timestamp(ss_ts) AS dateid,
  SUM(ss.ss_quantity * i.i_price) AS amount
FROM 
	store_sales ss,
	items i
WHERE 
	to_date(ss.ss_ts) > '${hiveconf:q25_date}'
	AND ss_customer_id IS NOT NULL
	AND ss.ss_item_id = i.i_item_id
GROUP BY
  ss_customer_id,
  ss_transaction_id,
  ss_ts
;


INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  ws_customer_id AS cid,
  ws_transaction_id AS oid,
  ws_ts AS dateid,
  sum(ws.ws_quantity * i.i_price) AS amount
FROM 
	web_sales ws,
	items i
WHERE 
	to_date(ws.ws_ts) > '${hiveconf:q25_date}'
	AND ws_customer_id IS NOT NULL
	AND ws.ws_item_id = i.i_item_id
GROUP BY
  ws_customer_id,
  ws_transaction_id,
  ws_ts
;

------ create input table for mahout --------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:TEMP_RESULT_TABLE};
CREATE TABLE ${hiveconf:TEMP_RESULT_TABLE} (
  cid        INT,
  recency    INT,
  frequency  INT,
  totalspend INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE ${hiveconf:TEMP_RESULT_TABLE}
SELECT
  cid AS id,
  CASE WHEN 37621 - max(dateid) < 60 THEN 1.0 ELSE 0.0 END -- 37621 == 2003-01-02
    AS recency,
  count(oid) AS frequency,
  SUM(amount) AS totalspend
FROM 
	${hiveconf:TEMP_TABLE}
GROUP BY cid;


--- CLEANUP--------------------------------------------
DROP TABLE ${hiveconf:TEMP_TABLE};
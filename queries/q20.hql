---
--- Do a customer segmentation based on their preferred shopping method (online vs in store).
---

-- set the database
use BigBenchV2;

set RESULT_TABLE=q20_results;
set TEMP_TABLE1=tmp1;
set TEMP_TABLE2=tmp2;

--- calculate total revenue of online sales segment
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE VIEW ${hiveconf:TEMP_TABLE1} AS
SELECT 
	c.c_customer_id as o_customer,
	SUM(ws.ws_quantity * i.i_price) as online_revenue
FROM 
	web_sales ws,
	customers c,
	items i
WHERE ws.ws_customer_id = c.c_customer_id
	  AND ws.ws_customer_id is NOT NULL
	  AND ws.ws_item_id = i.i_item_id
GROUP BY c.c_customer_id
ORDER BY c.c_customer_id ASC
;

--SELECT * FROM  ${hiveconf:TEMP_TABLE1} limit 10;
		
--- calculate total revenue of in-store sales segment
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE VIEW ${hiveconf:TEMP_TABLE2} AS
SELECT 
	c.c_customer_id as i_customer,
	SUM(ss.ss_quantity * i.i_price) as instore_revenue
FROM 
	store_sales ss,
	customers c,
	items i
WHERE ss.ss_customer_id = c.c_customer_id
		AND ss.ss_customer_id is NOT NULL
		AND ss.ss_item_id = i.i_item_id
GROUP BY c.c_customer_id
ORDER BY c.c_customer_id ASC
;

--SELECT * FROM  ${hiveconf:TEMP_TABLE2} limit 10;


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  online_segment		BIGINT,
  instore_segment		BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


--- calculate total revenue of both online and in-store sales segment
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT 
	SUM(CASE WHEN t1.online_revenue >= t2.instore_revenue 
THEN 1 
ELSE 0 END) as online_revenue,
	SUM(CASE WHEN t1.online_revenue < t2.instore_revenue	
THEN 1 
ELSE 0 END) as instore_revenue
FROM customers c
JOIN ${hiveconf:TEMP_TABLE1} t1 ON c.c_customer_id = t1.o_customer
JOIN ${hiveconf:TEMP_TABLE2} t2 ON c.c_customer_id = t2.i_customer
;
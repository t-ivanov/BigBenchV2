--Cluster customers into book buddies/club groups based on their in
--store book purchasing histories.

-- set the database
use BigBenchV2;
-- Query parameters
set q26_i_category_IN='cat#13';
set q26_count_ss_item_id=5;
set TEMP_RESULT_TABLE=q26_results;
--set TEMP_TABLE=q26_temp;

------ create input table for mahout --------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:TEMP_RESULT_TABLE};
CREATE TABLE ${hiveconf:TEMP_RESULT_TABLE} (
  cid  INT,
  id1  INT,
  id3  INT,
  id5  INT,
  id7  INT,
  id9  INT,
  id11 INT,
  id13 INT,
  id15 INT,
  id2  INT,
  id4  INT,
  id6  INT,
  id8  INT,
  id10 INT,
  id14 INT,
  id16 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE ${hiveconf:TEMP_RESULT_TABLE}
SELECT
  ss.ss_customer_id AS cid,
  count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
  count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
  count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
  count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
  count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
  count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
  count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
  count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15,
  count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
  count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
  count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
  count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
  count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
  count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
  count(CASE WHEN i.i_class_id=16 THEN 1 ELSE NULL END) AS id16
FROM 
	store_sales ss
INNER JOIN items i ON ss.ss_item_id = i.i_item_id
WHERE 
	i.i_category_name IN (${hiveconf:q26_i_category_IN})
	AND ss.ss_customer_id IS NOT NULL
GROUP BY ss.ss_customer_id
HAVING count(ss.ss_item_id) > ${hiveconf:q26_count_ss_item_id}
;
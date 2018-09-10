--Perform category affinity analysis for products viewed together.

-- Resources
ADD FILE /home/user1/semi_bench/queries/resources/q30_reducer.py;

-- set the database
use BigBenchV2;
-- Resources
set RESULT_TABLE=q30_results;
--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  category_id        STRING,
  affine_category_id STRING,
  category           STRING,
  affine_category    STRING,
  frequency          BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Begin: the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
  ro.category_id AS category_id,
  ro.affine_category_id AS affine_category_id,
  ro.category AS category,
  ro.affine_category AS affine_category,
  count(*) as frequency
FROM (
  FROM (
    SELECT
      logs.wl_customer_id AS combined_key,
      i.i_category_id AS category_id,
      i.i_category_name AS category
    FROM 
	(
	select
		l.wl_customer_id,
		l.wl_item_id
	from web_logs
		lateral view 
		json_tuple(
		web_logs.line,
		'wl_customer_id',
		'wl_item_id'
	) l as 
		wl_customer_id,
		wl_item_id
	where 
		wl_customer_id IS NOT NULL
		AND wl_item_id IS NOT NULL
	) logs,
		items i
	WHERE	
		logs.wl_item_id = i.i_item_id 
		AND i.i_category_id IS NOT NULL
    CLUSTER BY combined_key
  ) mo
  REDUCE
    mo.combined_key,
    mo.category_id,
    mo.category
  USING 'python q30_reducer.py'
  AS (
    category_id,
    category,
    affine_category_id,
    affine_category )
) ro
GROUP BY ro.category_id, ro.affine_category_id, ro.category ,ro.affine_category
ORDER BY frequency
;

select * from ${hiveconf:RESULT_TABLE};

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 49 minutes 35 seconds 670 msec
--Time taken: 477.404 seconds
--11      17      cat#11  cat#17  17804
--17      11      cat#17  cat#11  17804
--13      11      cat#13  cat#11  17805
--11      13      cat#11  cat#13  17805
--11      15      cat#11  cat#15  17816
--15      11      cat#15  cat#11  17816
--17      13      cat#17  cat#13  17826
--13      17      cat#13  cat#17  17826
--21      15      cat#21  cat#15  17828
--15      21      cat#15  cat#21  17828
--21      11      cat#21  cat#11  17828
--11      21      cat#11  cat#21  17828
--5       11      cat#05  cat#11  17830
--11      2       cat#11  cat#02  17830
--2       11      cat#02  cat#11  17830
--11      5       cat#11  cat#05  17830
--17      21      cat#17  cat#21  17832
--21      17      cat#21  cat#17  17832
--23      11      cat#23  cat#11  17837
--11      23      cat#11  cat#23  17837
--21      13      cat#21  cat#13  17840
--...

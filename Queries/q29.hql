--Perform category affinity analysis for products purchased online together.


ADD FILE /home/user1/semi_bench/queries/resources/q29_reducer.py; 

-- set the database
use BigBenchV2;
-- Resources
set RESULT_TABLE=q29_results;

--Result  --------------------------------------------------------------------
--keep result human readable
--set hive.exec.compress.output=false;
--set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  category_id        BIGINT,
  affine_category_id BIGINT,
  category           STRING,
  affine_category    STRING,
  frequency          BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Begin: the real query part
--INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
  ro.category_id AS category_id,
  ro.affine_category_id AS affine_category_id,
  ro.category AS category,
  ro.affine_category AS affine_category,
  count(*) as frequency
FROM (
  
  FROM (
    SELECT
      logs.wl_customer_id AS customer_id,
      i.i_category_id AS category_id,
      i.i_category_name AS category
    FROM 
	(
	select
		l.wl_customer_id,
		l.wl_item_id,
		l.wl_webpage_name
	from web_logs
		lateral view 
		json_tuple(
		web_logs.line,
		'wl_customer_id',
		'wl_item_id',
		'wl_webpage_name'
	) l as 
		wl_customer_id,
		wl_item_id,
		wl_webpage_name
	where 
		wl_customer_id is not null
	) logs,
		web_pages wp,
		items i
    WHERE
		logs.wl_item_id = i.i_item_id
		AND logs.wl_webpage_name = wp.w_web_page_name
		AND i.i_category_id IS NOT NULL
		AND wp.w_web_page_type = 'add to cart'
    CLUSTER BY customer_id
	
  ) mo
  REDUCE
    mo.customer_id,
    mo.category_id,
    mo.category
  USING 'python q29_reducer.py'
  AS (
    category_id,
    category,
    affine_category_id,
    affine_category
  )
  
) ro
GROUP BY ro.category_id, ro.affine_category_id, ro.category, ro.affine_category
ORDER BY frequency
;

--select * from ${hiveconf:RESULT_TABLE};

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 32 minutes 56 seconds 40 msec
--Time taken: 453.007 seconds
--21      11      cat#21  cat#11  8003
--11      21      cat#11  cat#21  8003
--7       21      cat#07  cat#21  8078
--21      7       cat#21  cat#07  8078
--19      21      cat#19  cat#21  8094
--21      19      cat#21  cat#19  8094
--21      15      cat#21  cat#15  8099
--15      21      cat#15  cat#21  8099
--7       11      cat#07  cat#11  8105
--11      7       cat#11  cat#07  8105
--17      11      cat#17  cat#11  8107
--11      17      cat#11  cat#17  8107
--11      15      cat#11  cat#15  8116
--15      11      cat#15  cat#11  8116
--2       11      cat#02  cat#11  8119
--11      2       cat#11  cat#02  8119
--21      2       cat#21  cat#02  8120
--2       21      cat#02  cat#21  8120
--21      17      cat#21  cat#17  8126
--17      21      cat#17  cat#21  8126
--5       21      cat#05  cat#21  8134
--...

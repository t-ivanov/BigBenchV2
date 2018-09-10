--Perform category affinity analysis for products purchased online together.


ADD FILE /bigbenchv2/resources/q29_reducer.py; 

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
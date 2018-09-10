--For a given product, measure the correlation of sentiments, including
--the number of reviews and average review ratings, on product monthly revenues.

-- set the database
use BigBenchV2;
-- Resources
set RESULT_TABLE=q11_results;
--web_sales date range
set q11_startDate=2013-11-02;
-- +30days
set q11_endDate=2013-12-02; 

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  correlation DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
-- DOUBLE corr(col1, col2) Returns the Pearson coefficient of correlation of a pair of a numeric columns in the group.
SELECT corr(reviews_count, avg_rating)
FROM (
  SELECT
    p.pr_item_id  AS pid,
    p.r_count     AS reviews_count,
    p.avg_rating  AS avg_rating,
    s.revenue     AS m_revenue
  FROM (
    SELECT
      pr_item_id,
      count(*) AS r_count,
      avg(pr_rating) AS avg_rating
    FROM product_reviews
    WHERE pr_item_id IS NOT null
    GROUP BY pr_item_id
  ) p
  INNER JOIN (
    SELECT 
		ws_item_id,
		SUM(ws.ws_quantity * i.i_price) AS revenue
	FROM 
		web_sales ws,
		items i
	WHERE  
		ws_item_id IS NOT null 
		AND to_date(ws.ws_ts) >= '${hiveconf:q11_startDate}' 
		AND to_date(ws.ws_ts) <= '${hiveconf:q11_endDate}' 
		AND ws.ws_item_id = i.i_item_id 
	GROUP BY ws_item_id
  ) s
  ON p.pr_item_id = s.ws_item_id
) q11_review_stats
;
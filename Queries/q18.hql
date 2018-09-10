--Identify the stores with flat or declining sales in 3 consecutive months,
--check if there are any negative reviews regarding these stores available online.

-- set the database
use BigBenchV2;
-- Resources
set RESULT_TABLE=q18_results;
set TEMP_TABLE=q18_temp;
-- store_sales date range
set q18_startDate=2014-05-02;
--+90days
set q18_endDate=2014-09-02;
ADD JAR /home/user1/semi_bench/queries/resources/opennlp-maxent-3.0.3.jar;
ADD JAR /home/user1/semi_bench/queries/resources/opennlp-tools-1.5.3.jar;
ADD JAR /home/user1/semi_bench/queries/resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_NegSentiment AS 'de.bankmark.bigbench.queries.q18.NegativeSentimentUDF';

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE TABLE ${hiveconf:TEMP_TABLE} (
  s_store_name       STRING,
  pr_review_date     STRING,
  pr_content  		 STRING
);

INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  s_store_name,
  pr_ts,
  pr_content
FROM (
  --select store_name for stores with flat or declining sales in 3 consecutive months.
  SELECT 
	s_store_name
  FROM stores s
  JOIN (
    -- linear regression part
    SELECT
      temp.cat AS cat,
      --SUM(temp.x)as sumX,
      --SUM(temp.y)as sumY,
      --SUM(temp.xy)as sumXY,
      --SUM(temp.xx)as sumXSquared,
      --count(temp.x) as N,
      --N * sumXY - sumX * sumY AS numerator,
      --N * sumXSquared - sumX*sumX AS denom
      --numerator / denom as slope,
      --(sumY - slope * sumX) / N as intercept
      --(count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) AS numerator,
      --(count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) AS denom
      --numerator / denom as slope,
      --(sumY - slope * sumX) / N as intercept
      ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) ) as slope,
      (SUM(temp.y) - ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) ) * SUM(temp.x)) / count(temp.x) as intercept
    FROM (
      SELECT
        s_s.ss_store_id AS cat,
        unix_timestamp(s_s.ss_ts) AS x,
        SUM(s_s.ss_quantity * i.i_price) AS y,
        unix_timestamp(s_s.ss_ts) * SUM(s_s.ss_quantity * i.i_price) AS xy,
        unix_timestamp(s_s.ss_ts)*unix_timestamp(s_s.ss_ts) AS xx
      FROM 
		store_sales s_s,
		items i
      WHERE 
		s_s.ss_store_id <= 18
		-- select date range
		AND to_date(s_s.ss_ts) >= '${hiveconf:q18_startDate}' 
		AND to_date(s_s.ss_ts) <= '${hiveconf:q18_endDate}'
		AND s_s.ss_item_id = i.i_item_id
      GROUP BY s_s.ss_store_id, unix_timestamp(s_s.ss_ts)
    ) temp
    GROUP BY temp.cat
  ) c on s.s_store_id = c.cat
  WHERE c.slope < 0
) tmp
JOIN  product_reviews pr on (true)
WHERE instr(pr.pr_content, tmp.s_store_name) > 0
;


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--Prepare result storage
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  s_store_name    STRING,
  review_date     STRING,
  review_sentence STRING,
  sentiment       STRING,
  sentiment_word  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT extract_NegSentiment( s_store_name, pr_review_date, pr_content) AS ( s_store_name, review_date, review_sentence, sentiment, sentiment_word )
--select product_reviews containing the name of a store. Consider only stores with flat or declining sales in 3 consecutive months.
FROM ${hiveconf:TEMP_TABLE}
;


-- Cleanup ----------------------------------------
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};

--Total MapReduce CPU Time Spent: 1 seconds 540 msec
--OK
--Time taken: 14.611 seconds


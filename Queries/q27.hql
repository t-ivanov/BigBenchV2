--Extract competitor product names and model names (if any) from
--online product reviews for a given product.

-- Resources
ADD JAR /home/user1/semi_bench/queries/resources/opennlp-maxent-3.0.3.jar;
ADD JAR /home/user1/semi_bench/queries/resources/opennlp-tools-1.5.3.jar;
ADD JAR /home/user1/semi_bench/queries/resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION find_company AS 'de.bankmark.bigbench.queries.q27.CompanyUDF';

-- set the database
use BigBenchV2;
-- Query parameters
set RESULT_TABLE=q27_results;
set q27_pr_item_id=25;

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  pr_review_id    BIGINT,
  pr_item_id      BIGINT,
  company_name    STRING,
  review_sentence STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT find_company(pr_review_id, pr_item_id, pr_content) AS (pr_review_id, pr_item_id, company_name, review_sentence)
FROM (
  SELECT 
	pr_review_id,
	pr_item_id,
	pr_content
  FROM 
	product_reviews
  WHERE pr_item_id = ${hiveconf:q27_pr_item_id}
) subtable
;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 4 seconds 640 msec
--Time taken: 20.03 seconds



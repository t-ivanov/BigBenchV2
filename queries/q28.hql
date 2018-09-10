--Build text classifier for online review sentiment classification (Positive,
--Negative, Neutral), using 60% of available reviews for training and the remaining
--40% for testing. Display classifier accuracy on testing data.

-- set the database
use BigBenchV2;
-- Query parameters
set TEMP_TABLE1=q28_temp1_training;
set TEMP_TABLE2=q28_temp2_testing;

--Result 1 Training table for mahout--------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE TABLE ${hiveconf:TEMP_TABLE1} (
  pr_review_id      BIGINT,
  pr_r_rating       STRING,
  pr_content 		STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE ${hiveconf:TEMP_TABLE1}
SELECT
  pr_review_id,
  CASE pr_rating
    WHEN 1 THEN 'NEG'
    WHEN 2 THEN 'NEG'
    WHEN 3 THEN 'NEU'
    WHEN 4 THEN 'POS'
    WHEN 5 THEN 'POS'
    END AS pr_r_rating,
  pr_content
FROM 
	product_reviews
WHERE pmod(pr_review_id, 5) IN (1,2,3)
--limit 10000
;


--Result 2 Testing table for mahout --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE TABLE ${hiveconf:TEMP_TABLE2} (
  pr_review_id      BIGINT,
  pr_r_rating       STRING,
  pr_content 		STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE ${hiveconf:TEMP_TABLE2}
SELECT
  pr_review_id,
  CASE pr_rating
    WHEN 1 THEN 'NEG'
    WHEN 2 THEN 'NEG'
    WHEN 3 THEN 'NEU'
    WHEN 4 THEN 'POS'
    WHEN 5 THEN 'POS'
    END AS pr_r_rating,
  pr_content
FROM 
	product_reviews
WHERE pmod(pr_review_id, 5) in (0,4)
--limit 10000
;
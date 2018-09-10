--For all products, extract sentences from its product reviews that con-
--tain Positive or Negative sentiment and display the sentiment polarity of the
--extracted sentences.

-- Resources
ADD JAR /home/user1/semi_bench/queries/resources/opennlp-maxent-3.0.3.jar;
ADD JAR /home/user1/semi_bench/queries/resources/opennlp-tools-1.5.3.jar;
ADD JAR /home/user1/semi_bench/queries/resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_sentiment AS 'de.bankmark.bigbench.queries.q10.SentimentUDF';
-- set the database
use BigBenchV2;

-- Query parameters
set RESULT_TABLE=q10_results;
--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  pr_item_id      BIGINT,
  review_sentence STRING,
  sentiment       STRING,
  sentiment_word  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query part
-- you may want to adapt: set hive.exec.reducers.bytes.per.reducer=xxxx;  Default Value: 1,000,000,000 prior to Hive 0.14.0; 256 MB (256,000,000) in Hive 0.14.0 and later
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT extract_sentiment(pr_item_id, pr_content) AS (pr_item_id, review_sentence, sentiment, sentiment_word)
FROM (
  SELECT 
	pr_item_id,
	pr_content 
  FROM 
	product_reviews DISTRIBUTE BY length(pr_content)
) pr
;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 32 seconds 420 msec
--Time taken: 49.421 seconds
--483     temptingly brave instructions whithout shall have to hinder outside:    POS     brave
--485     ironically brave multipliers believe thin foxes!        POS     brave
--485     ironically brave multipliers believe thin foxes!        POS     believe
--...

-- spark
-- 16/06/29 22:12:27 INFO scheduler.DAGScheduler: Job 11 finished: processCmd at CliDriver.java:376, took 0.041029 s
--Time taken: 19.613 seconds
--16/06/29 22:12:27 INFO CliDriver: Time taken: 19.613 seconds

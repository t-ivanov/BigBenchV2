--For all products, extract sentences from its product reviews that con-
--tain Positive or Negative sentiment and display the sentiment polarity of the
--extracted sentences.

-- Resources
ADD JAR /bigbenchv2/resources/opennlp-maxent-3.0.3.jar;
ADD JAR /bigbenchv2/resources/opennlp-tools-1.5.3.jar;
ADD JAR /bigbenchv2/resources/bigbenchqueriesmr.jar;
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
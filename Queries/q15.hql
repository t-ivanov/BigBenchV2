--Find the categories with flat or declining sales for in store purchases
--during a given year for a given store.

-- set the database
use BigBenchV2;
-- Resources
set RESULT_TABLE=q15_results;
set q15_startDate=2013-09-02;
--+1year
set q15_endDate=2014-09-02;
set q15_store_id=10;

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  cat       INT,
  slope     DOUBLE,
  intercept DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT *
FROM (
  SELECT
    temp.cat,
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
    (SUM(temp.y) - ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x)*SUM(temp.x)) ) * SUM(temp.x)) / count(temp.x) as intercept
  FROM (
    SELECT
      i.i_category_id AS cat, -- ranges from 1 to 10
      unix_timestamp(s.ss_ts) AS x,
      SUM(s.ss_quantity * i.i_price) AS y,
	  unix_timestamp(s.ss_ts)*SUM(s.ss_quantity * i.i_price) AS xy,
      unix_timestamp(s.ss_ts)*unix_timestamp(s.ss_ts) AS xx
    FROM store_sales s	
    INNER JOIN items i ON s.ss_item_id = i.i_item_id
    WHERE 
		i.i_category_id IS NOT NULL
		AND s.ss_store_id = ${hiveconf:q15_store_id} -- for a given store ranges from 1 to 12
		-- select date range
		AND to_date(s.ss_ts) >= '${hiveconf:q15_startDate}' 
		AND to_date(s.ss_ts) <= '${hiveconf:q15_endDate}'
	GROUP BY i.i_category_id, unix_timestamp(s.ss_ts)
  ) temp
  GROUP BY temp.cat
) regression
WHERE slope < 0
;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 5 seconds 270 msec
--Time taken: 34.027 seconds
--Total MapReduce CPU Time Spent: 5 seconds 160 msec
--Time taken: 33.933 seconds
-- results:
--0       -1.70006E-6     2534.6498466010426
--1       -1.1469E-7      271.7907768203855
--2       -1.09115E-6     1648.3776954500374
--4       -4.04041E-6     5768.38323714674
--5       -2.99723E-6     4282.179698989439
--6       -4.52314E-6     6466.373273403257

-- Spark
-- 16/07/01 13:27:48 INFO scheduler.DAGScheduler: Job 11 finished: processCmd at CliDriver.java:376, took 0.025030 s
--Time taken: 16.522 seconds
--16/07/01 13:27:48 INFO CliDriver: Time taken: 16.522 seconds


--Find products that are sold together frequently in given
--stores. Only products in certain categories sold in specific stores are considered,
--and "sold together frequently" means at least 50 customers bought these products 
--together in a transaction.

-- Resources
add file /bigbenchv2/resources/bigbenchqueriesmr.jar;

-- set the database
use BigBenchV2;

-- Query Parameters
set RESULT_TABLE=q01_results;
set q01_i_category_id_IN=1, 2 ,3;
-- sf1 -> 11 stores, 90k sales in 820k lines
set q01_ss_store_sk_IN=10, 20, 33, 40, 50;
set q01_COUNT_pid1_greater=49;
set q01_NPATH_ITEM_SET_MAX=500;

--Result -------------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  pid1 BIGINT,
  pid2 BIGINT,
  cnt  BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- the real query part
--Find the most frequent ones
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT 
	pid1,
	pid2,
	COUNT (*) AS cnt
FROM (
  --Make items basket
  FROM (
    -- Joining two tables
    SELECT 
		s.ss_transaction_id AS oid,
		s.ss_item_id AS pid
    FROM store_sales s
    INNER JOIN items i ON (s.ss_item_id = i.i_item_id)
    WHERE i.i_category_id in (${hiveconf:q01_i_category_id_IN})
    AND s.ss_store_id in (${hiveconf:q01_ss_store_sk_IN})
    CLUSTER BY oid
  ) q01_map_output
  REDUCE q01_map_output.oid, q01_map_output.pid
  USING 'java  -Xmx1024m  -cp bigbenchqueriesmr.jar de.bankmark.bigbench.queries.q01.Red -ITEM_SET_MAX ${hiveconf:q01_NPATH_ITEM_SET_MAX} '
  AS (pid1, pid2)
) q01_temp_basket
GROUP BY pid1, pid2
HAVING COUNT (pid1) > ${hiveconf:q01_COUNT_pid1_greater}
ORDER BY pid1, cnt, pid2
;
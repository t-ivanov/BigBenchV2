
-- set the database
use BigBenchV2;

set TEMP_TABLE=q02_temp;
set RESULT_TABLE=q02_results;
set q02_pid1_IN=500;
set q02_ITEM_SET_MAX=500;
set q02_limit=10;
-- 10 min timeout
set session_time=600; 


-- SESSIONIZE
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE VIEW ${hiveconf:TEMP_TABLE} AS
SELECT DISTINCT 
   session_id,
   item
FROM
(
 SELECT
    CONCAT(sessionize.uid, 
		CONCAT('_', 
			SUM(sessionize.new_session) OVER (PARTITION BY sessionize.uid ORDER BY sessionize.tstamp)
       )
      ) AS session_id,
	  sessionize.item
FROM
(
  SELECT
      logs.wl_customer_id 			AS uid,
      logs.wl_item_id 				AS item,
      unix_timestamp(logs.wl_ts)	AS tstamp
	  , CASE
        WHEN (unix_timestamp(logs.wl_ts) - lag (unix_timestamp(logs.wl_ts))
             OVER (PARTITION BY logs.wl_customer_id ORDER BY logs.wl_ts)) >= ${hiveconf:session_time}
		THEN 1
        ELSE 0
      END AS new_session   
    FROM 
		(
		select 
			js.wl_customer_id,
			js.wl_item_id,
			js.wl_ts
		from web_logs
			lateral view 
				json_tuple(
				web_logs.line,
				'wl_customer_id',
				'wl_item_id',
				'wl_timestamp'
			) js as 
			wl_customer_id,
			wl_item_id,
			wl_ts
		where
			js.wl_customer_id is not NULL 
			and js.wl_item_id is not NULL
		) logs
    CLUSTER BY uid
) sessionize
CLUSTER BY session_id
) distinct_session
;


DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  item_id_1 BIGINT,
  item_id_2 BIGINT,
  cnt       BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
  item_id_1,
  ${hiveconf:q02_pid1_IN} AS item_id_2,
  COUNT (*) AS cnt
FROM
(
  -- Make item "viewed together" pairs by exploding the itemArray's containing the searched item q02_pid1_IN
  SELECT 
	explode(itemArray) AS item_id_1
  FROM
  (
    SELECT collect_list(item) AS itemArray --(_list= with duplicates, _set = distinct)
    FROM ${hiveconf:TEMP_TABLE}
    GROUP BY session_id
    HAVING array_contains(itemArray, cast(${hiveconf:q02_pid1_IN} AS STRING) ) -- eager filter out groups that don't contain the searched item
  ) collectedList
) pairs
WHERE item_id_1 <> ${hiveconf:q02_pid1_IN}
GROUP BY item_id_1
ORDER BY
  cnt DESC,
  item_id_1
LIMIT ${hiveconf:q02_limit};

-- cleanup
--DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE};

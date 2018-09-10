
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



-- spark sf 1
--370     500     86
--826     500     85
--630     500     81
--874     500     81
--918     500     79
--433     500     78
--890     500     78
--144     500     77
--378     500     77
--425     500     77
--Time taken: 206.551 seconds, Fetched 10 row(s)

--370     500     86
--826     500     85
--630     500     81
--874     500     81
--918     500     79
--433     500     78
--890     500     78
--144     500     77
--378     500     77
--425     500     77
--Time taken: 199.432 seconds, Fetched 10 row(s)


-- hive sf1
--427     500     102
--630     500     102
--701     500     102
--181     500     101
--917     500     99
--682     500     98
--874     500     98
--157     500     97
--455     500     97
--509     500     97
--Time taken: 376.281 seconds, Fetched: 10 row(s)

--918     500     105
--379     500     104
--597     500     103
--777     500     103
--304     500     100
--781     500     100
--282     500     99
--730     500     99
--362     500     98
--901     500     98
--Time taken: 375.246 seconds, Fetched: 10 row(s)

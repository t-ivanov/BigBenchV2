--For a given product, measure the effect of competitor's prices on
--products' in-store and online sales. (Compute the cross-price elasticity of demand
--for a given product)

-- set the database
use BigBenchV2;

-- Resources
set q24_i_item_id_IN=7;
set TEMP_TABLE1=tmp1;
set TEMP_TABLE2=tmp2;
set TEMP_TABLE3=tmp3;
set RESULT_TABLE=q24_results;

-- compute the price change % for the competitor
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE VIEW ${hiveconf:TEMP_TABLE1} AS
SELECT
   i_item_id, 
  (i_comp_price - i_price)/i_price AS price_change,
  logs.wl_ts as start_date  -- imp_start_date
--  ,(imp_end_date - imp_start_date) AS no_days
FROM items i,
(
	select 
		get_json_object(web_logs.line, '$.wl_item_id') as wl_item_id,
		get_json_object(web_logs.line, '$.wl_timestamp') as wl_ts
	from web_logs
	order by wl_ts asc
) logs
where logs.wl_item_id IN (${hiveconf:q24_i_item_id_IN}) 
and i.i_item_id = logs.wl_item_id
and i.i_comp_price < i.i_price
-- order by logs.wl_ts asc
;


DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE VIEW ${hiveconf:TEMP_TABLE2} AS
SELECT
  ws_item_id,
  SUM(
    CASE WHEN ws_ts >= c.start_date
--    AND ws_ts < c.imp_start_date + c.no_days
    THEN ws_quantity
    ELSE 0 END
  ) AS current_ws,
  SUM(
    CASE WHEN  ws_ts < c.start_date
--	AND ws_ts >= c.imp_start_date - c.no_days
    THEN ws_quantity
    ELSE 0 END
  ) AS prev_ws
FROM web_sales ws
JOIN ${hiveconf:TEMP_TABLE1} c ON ws.ws_item_id = c.i_item_id
GROUP BY ws_item_id
;


DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE3};
CREATE VIEW ${hiveconf:TEMP_TABLE3} AS
SELECT
  ss_item_id,
  SUM(
    CASE WHEN ss_ts >= c.start_date
--    AND ss_ts < c.imp_start_date + c.no_days
    THEN ss_quantity
    ELSE 0 END
  ) AS current_ss,
  SUM(
    CASE WHEN ss_ts < c.start_date
--    AND ss_ts >= c.imp_start_date - c.no_days 
    THEN ss_quantity
    ELSE 0 END
  ) AS prev_ss
FROM store_sales ss
JOIN ${hiveconf:TEMP_TABLE1} c ON c.i_item_id = ss.ss_item_id
GROUP BY ss_item_id
;


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  i_item_id               BIGINT,
  cross_price_elasticity  decimal(10,10)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Begin: the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
  distinct i_item_id,
  (current_ss + current_ws - prev_ss - prev_ws) / ((prev_ss + prev_ws) * price_change) AS cross_price_elasticity
FROM ${hiveconf:TEMP_TABLE1} c
JOIN ${hiveconf:TEMP_TABLE2} ws ON c.i_item_id = ws.ws_item_id
JOIN ${hiveconf:TEMP_TABLE3} ss ON c.i_item_id = ss.ss_item_id
;

-- clean up -----------------------------------
DROP VIEW ${hiveconf:TEMP_TABLE2};
DROP VIEW ${hiveconf:TEMP_TABLE3};
DROP VIEW ${hiveconf:TEMP_TABLE1};


-- hive SF1
-- 7       0.7912996816
-- 7	0.3456465832

-- spark SF1
-- 7       0.7912996816
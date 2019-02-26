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
--DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
--CREATE VIEW ${hiveconf:TEMP_TABLE1} AS

-- explain
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
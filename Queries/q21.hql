--
-- Path analysis to a purchase page
--
-- enable hive on spark
set hive.execution.engine=spark;

-- Resources
-- set the database
use BigBenchV2;

--set TEMP_TABLE2=PathToPurchase;

--DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE2};
--CREATE TABLE ${hiveconf:TEMP_TABLE2} as
SELECT
	path_to_purchase,
	count(*) as freq
FROM matchpath
(
 on (
	select 
		js.wl_customer_id,
		js.wl_item_id,
		js.wl_webpage_name,
		js.wl_ts 
	from web_logs
		lateral view 
			json_tuple(
			web_logs.line,
			'wl_customer_id',
			'wl_item_id',
			'wl_webpage_name',
			'wl_timestamp'
		) js as 
		wl_customer_id,
		wl_item_id,
		wl_webpage_name,
		wl_ts
	WHERE
	js.wl_customer_id IS NOT NULL
) n_logs 
	partition by wl_customer_id
	order by wl_ts
	arg1('other+.purchase'),
	arg2('other'),arg3( wl_webpage_name not in ('webpage#21', 'webpage#22', 'webpage#23', 'webpage#24', 'webpage#25')),
	arg4('purchase'),arg5( wl_webpage_name  in ('webpage#21', 'webpage#22', 'webpage#23', 'webpage#24', 'webpage#25')), 
	arg6('tpath.wl_webpage_name as path_to_purchase')
)
GROUP BY path_to_purchase
ORDER BY freq desc
LIMIT 5
;


-- hive SF1
--["webpage#11","webpage#22"]     1651
--["webpage#20","webpage#24"]     1639
--["webpage#20","webpage#23"]     1621
--["webpage#14","webpage#21"]     1607
--["webpage#15","webpage#22"]     1598
--Time taken: 294.218 seconds, Fetched: 5 row(s)


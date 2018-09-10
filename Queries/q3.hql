--Find the last 5 products that are mostly viewed before a given product
--was purchased online. Only products in certain categories and viewed within 30(10)
--days before the purchase date are considered.

-- set the database
use BigBenchV2;

set RESULT_TABLE=q3_results;
set q03_days_before_purchase=30;


SELECT
--	items[0] as lastviewed_item,
--	size_items,
--	items[size_items - 2] as purchased_item,
--	dates[0] as lastviewed_date,
--	size_dates,
--	dates[size_dates - 1] as purchased_date,
	i1.i_name,
	i2.i_name,
	count(*) as cnt
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
	arg1('A+.B+.C'),
	arg2('A'),arg3( wl_webpage_name in ('webpage#14', 'webpage#17', 'webpage#20', 'webpage#18', 'webpage#11', 'webpage#19', 'webpage#12', 'webpage#13', 'webpage#16')),  -- product look up pages
	arg4('B'),arg5( wl_webpage_name  in ('webpage#01', 'webpage#02', 'webpage#03', 'webpage#04', 'webpage#05', 'webpage#06', 'webpage#07', 'webpage#08', 'webpage#09', 'webpage#10')),  -- add to cart
	arg6('C'),arg7( wl_webpage_name  in ('webpage#23', 'webpage#22', 'webpage#25', 'webpage#21', 'webpage#24')), -- checkout pages
	arg8('tpath.wl_item_id as items, size(tpath.wl_item_id) as size_items, size(tpath.wl_ts) as size_dates, tpath.wl_ts as dates')
) t
inner join items i1 on i1.i_item_id = items[0] 
inner join items i2 on i2.i_item_id = items[size_items - 2]
WHERE
	items[size_items - 2] BETWEEN 500 and 550
	AND datediff( to_date(dates[size_dates - 1]), to_date(dates[0])) < ${hiveconf:q03_days_before_purchase}
	AND items[0] != items[size_items - 2]
GROUP BY i1.i_name, i2.i_name
ORDER BY cnt desc, i1.i_name
LIMIT 5
;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 46 minutes 32 seconds 910 msec
--OK
--item#0455       item#0508       3
--item#0001       item#0512       2
--item#0019       item#0530       2
--item#0023       item#0532       2
--item#0047       item#0544       2
--Time taken: 470.509 seconds, Fetched: 5 row(s)



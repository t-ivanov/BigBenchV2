--
-- Compare the average number of items purchased by registered users
-- from one year to the next. 
--

-- set the database
use BigBenchV2;
--set TEMP_TABLE1=PurchaseData;

--DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE1};
--CREATE TABLE ${hiveconf:TEMP_TABLE1} as
SELECT
		purchase_year,
		avg(items_per_user)
FROM
(
	SELECT
		userid as userid, 
		year(to_date(dates[size_dates - 1])) as purchase_year,
		sum(cart_items) as items_per_user
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
	-- (A|C) pattern is not supported by matchpath
		arg1('A+.B'),
		arg2('A'),arg3( wl_webpage_name in ('webpage#01', 'webpage#02', 'webpage#03'
					, 'webpage#04', 'webpage#05', 'webpage#06', 'webpage#07'
					, 'webpage#08', 'webpage#09', 'webpage#10', 'webpage#11'
					, 'webpage#12', 'webpage#13', 'webpage#14', 'webpage#15'
					, 'webpage#16', 'webpage#17', 'webpage#18', 'webpage#19'
					, 'webpage#20')),
		arg4('B'),arg5( wl_webpage_name  in ('webpage#21', 'webpage#22', 'webpage#23'
					, 'webpage#24', 'webpage#25')),
		arg6('tpath[0].wl_customer_id as userid, (size(tpath.wl_item_id)-1) as cart_items, tpath.wl_ts as dates, size(tpath.wl_ts) as size_dates')
	)
	GROUP BY userid, cart_items, dates[size_dates - 1]
) as t
GROUP BY purchase_year
ORDER BY purchase_year
;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 55 minutes 32 seconds 780 msec
--2013    8.5650447905031
--2014    8.569610728628128
--2015    9.036390101892286
--Time taken: 515.398 seconds, Fetched: 3 row(s)
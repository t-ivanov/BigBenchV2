--
-- Unique visitors per day.
--
 
-- set the database
use BigBenchV2;


select
	day(to_date(wl_timestamp)) as d,
	month(to_date(wl_timestamp)) as m,
	year(to_date(wl_timestamp)) as y,
	count(distinct wl_customer_id) as UniqueVisitors
from
	web_logs
		lateral view 
		json_tuple(
		web_logs.line,
		'wl_customer_id',
		'wl_timestamp'
	) l as 
		wl_customer_id,
		wl_timestamp
where
	wl_customer_id is not null
group by wl_timestamp
order by UniqueVisitors desc
limit 10;
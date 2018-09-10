--
-- Highest daily page views
--

-- set the database
use BigBenchV2;

select
	day(to_date(wl_timestamp)) as d,
	month(to_date(wl_timestamp)) as m,
	year(to_date(wl_timestamp)) as y,
	count(*) as PageViews
from
	web_logs
		lateral view 
		json_tuple(
		web_logs.line,
		'wl_timestamp'
	) logs as 
		wl_timestamp
group by wl_timestamp
order by PageViews desc
limit 10;
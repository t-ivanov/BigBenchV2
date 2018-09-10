--
-- Find the top 10 pages viewed for a pre-defined time frame.
--

-- set the database
use BigBenchV2;

select
	wl_webpage_name,
	count(*) as cnt
from
	web_logs
		lateral view 
		json_tuple(
		web_logs.line,
		'wl_webpage_name',
		'wl_timestamp'
	) logs as 
		wl_webpage_name,
		wl_timestamp
where
	wl_webpage_name is not null
and
	to_date(wl_timestamp) >= '2013-02-14' and to_date(wl_timestamp) < '2014-02-15'
group by wl_webpage_name
order by cnt desc
limit 10;
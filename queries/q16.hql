--
-- Find the top 10 pages viewed.
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
		'wl_webpage_name'
	) logs as 
		wl_webpage_name
where
	wl_webpage_name is not null
group by wl_webpage_name
order by cnt desc
limit 10;
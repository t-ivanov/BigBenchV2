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

-- hive SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 14 minutes 30 seconds 220 msec
--webpage#00      8006887
--webpage#11      3385709
--webpage#13      3384494
--webpage#14      3383651
--webpage#16      3382404
--webpage#19      3382203
--webpage#17      3382015
--webpage#18      3381470
--webpage#12      3380406
--webpage#20      3379567
--Time taken: 383.182 seconds, Fetched: 10 row(s)


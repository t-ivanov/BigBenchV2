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

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 1 hours 25 minutes 9 seconds 430 msec
--webpage#00      4016048
--webpage#11      1699109
--webpage#13      1697270
--webpage#12      1696185
--webpage#19      1695314
--webpage#14      1694993
--webpage#16      1694991
--webpage#17      1694066
--webpage#20      1693944
--webpage#18      1693885
--Time taken: 402.608 seconds, Fetched: 10 row(s)

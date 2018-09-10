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

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 0 days 2 hours 32 minutes 58 seconds 190 msec
--20      1       2013    13
--24      4       2013    13
--2       12      2013    12
--19      2       2013    12
--10      5       2014    12
--27      10      2013    12
--1       10      2013    12
--2       10      2013    12
--25      12      2014    12
--14      1       2014    11
--Time taken: 540.296 seconds, Fetched: 10 row(s)


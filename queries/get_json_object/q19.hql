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
	(
	select 
		get_json_object(web_logs.line, '$.wl_timestamp') as wl_timestamp
	from web_logs
	) logs
group by logs.wl_timestamp
order by PageViews desc
limit 10;

-- hive  SF1 clicks.json
-- Total MapReduce CPU Time Spent: 0 days 1 hours 23 minutes 45 seconds 800 msec
-- OK
-- 18	6	2013	9
-- 5	5	2013	9
-- 28	3	2014	9
-- 29	10	2014	9
-- 20	1	2013	9
-- 22	3	2013	9
-- 6	9	2013	9
-- 30	1	2014	9
-- 17	7	2014	9
-- 14	10	2014	9
-- Time taken: 346.788 seconds, Fetched: 10 row(s)



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
	spark_logs
group by wl_timestamp
order by PageViews desc
limit 10;

-- spark-sql  SF1 clicks.json
-- 5	5	2013	9
-- 14	10	2014	9
-- 18	6	2013	9
-- 28	1	2013	9
-- 22	3	2013	9
-- 29	10	2014	9
-- 20	1	2013	9
-- 17	7	2014	9
-- 30	1	2014	9
-- 6	9	2013	9
-- Time taken: 209.747 seconds, Fetched 10 row(s)


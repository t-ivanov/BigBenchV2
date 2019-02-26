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
	(
	select 
		get_json_object(web_logs.line, '$.wl_customer_id') as wl_customer_id,
		get_json_object(web_logs.line, '$.wl_timestamp') as wl_timestamp
	from web_logs
	) logs
where
	wl_customer_id is not null
group by wl_timestamp
order by UniqueVisitors desc
limit 10;

-- hive  SF1 clicks.json
-- Total MapReduce CPU Time Spent: 0 days 1 hours 9 minutes 26 seconds 360 msec
-- OK
-- 18	1	2014	4
-- 20	8	2013	4
-- 3	12	2014	4
-- 27	12	2013	3
-- 21	7	2014	3
-- 15	11	2014	3
-- 1	3	2014	3
-- 5	2	2014	3
-- 8	12	2013	3
-- 17	3	2014	3
-- Time taken: 288.495 seconds, Fetched: 10 row(s)


-- spark-sql  SF1 clicks.json
--20      8       2013    4
--3       12      2014    4
--12      11      2013    3
--12      5       2013    3
--12      9       2014    3
--23      12      2013    3
--2       9       2013    3
--30      10      2014    3
--13      10      2013    3
--12      5       2014    3
--Time taken: 196.782 seconds, Fetched 10 row(s)


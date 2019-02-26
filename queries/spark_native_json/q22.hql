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
	spark_logs
where
	wl_customer_id is not null
group by wl_timestamp
order by UniqueVisitors desc
limit 10;

-- hive  SF1 clicks.json
--20      8       2013    4
--3       12      2014    4
--23      12      2013    3
--12      3       2013    3
--18      12      2013    3
--12      6       2013    3
--19      8       2014    3
--5       7       2013    3
--19      6       2013    3
--28      8       2014    3
--Time taken: 267.555 seconds, Fetched: 10 row(s)

-- spark-sql  SF1 clicks.json
-- 18	1	2014	4
-- 20	8	2013	4
-- 3	12	2014	4
-- 12	11	2013	3
-- 15	6	2013	3
-- 18	12	2013	3
-- 8	7	2013	3
-- 2	9	2013	3
-- 3	4	2014	3
-- 30	10	2014	3
-- Time taken: 211.038 seconds, Fetched 10 row(s)



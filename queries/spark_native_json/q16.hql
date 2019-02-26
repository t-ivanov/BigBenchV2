--
-- Find the top 10 pages viewed.
--

-- set the database
use BigBenchV2;

select
	wl_webpage_name,
	count(*) as cnt
from
	spark_logs
where
	wl_webpage_name is not null
group by wl_webpage_name
order by cnt desc
limit 10;

-- hive SF1 clicks.json
-- webpage#00	4382447
-- webpage#13	1853023
-- webpage#11	1852802
-- webpage#14	1852478
-- webpage#16	1851743
-- webpage#17	1851545
-- webpage#18	1851057
-- webpage#19	1850788
-- webpage#20	1850554
-- webpage#15	1850167
-- Time taken: 203.871 seconds, Fetched 10 row(s)



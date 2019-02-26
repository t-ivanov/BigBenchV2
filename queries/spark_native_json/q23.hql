--
-- Users with most visits
--

-- set the database
use BigBenchV2;

select
	c_customer_id, 
	c_name, 
	count(*) as Visits
from
	spark_logs lg,
	customers
where lg.wl_customer_id is not null
and lg.wl_customer_id = c_customer_id
group by c_customer_id, c_name
order by Visits desc
limit 10;

-- hive  SF1 clicks.json
--Total MapReduce CPU Time Spent: 49 minutes 54 seconds 100 msec
--9210    customer#00009210       595
--665     customer#00000665       546
--941     customer#00000941       544
--5254    customer#00005254       543
--1536    customer#00001536       536
--8423    customer#00008423       535
--1622    customer#00001622       533
--1642    customer#00001642       531
--6957    customer#00006957       524
--5469    customer#00005469       520
--Time taken: 258.931 seconds, Fetched: 10 row(s)
--Time taken: 252.762 seconds, Fetched: 10 row(s)


-- spark-sql  SF1 clicks.json
-- 9210	customer#00009210	595
-- 665	customer#00000665	546
-- 941 customer#00000941	544
-- 5254	customer#00005254	543
-- 1536	customer#00001536	536
-- 8423	customer#00008423	535
-- 1622	customer#00001622	533
-- 1642	customer#00001642	531
-- 6957	customer#00006957	524
-- 10529	customer#00010529	522
-- Time taken: 204.352 seconds, Fetched 10 row(s)



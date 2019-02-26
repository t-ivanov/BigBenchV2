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
	(
	select 
		get_json_object(web_logs.line, '$.wl_customer_id') as wl_customer_id
	from web_logs
	) logs,
	customers
where logs.wl_customer_id is not null
and logs.wl_customer_id = c_customer_id
group by c_customer_id, c_name
order by Visits desc
limit 10;

-- hive  SF1 clicks.json
-- Total MapReduce CPU Time Spent: 39 minutes 46 seconds 210 msec
-- OK
-- 9210	customer#00009210	595
-- 665	customer#00000665	546
-- 941	customer#00000941	544
-- 5254	customer#00005254	543
-- 1536	customer#00001536	536
-- 8423	customer#00008423	535
-- 1622	customer#00001622	533
-- 1642	customer#00001642	531
-- 6957	customer#00006957	524
-- 10529	customer#00010529	522
-- Time taken: 242.108 seconds, Fetched: 10 row(s)



-- spark-sql  SF1 clicks.json
--9210    customer#00009210       595
--665     customer#00000665       546
--941     customer#00000941       544
--5254    customer#00005254       543
--1536    customer#00001536       536
--8423    customer#00008423       535
--1622    customer#00001622       533
--1642    customer#00001642       531
--6957    customer#00006957       524
--8871    customer#00008871       520
--Time taken: 190.024 seconds, Fetched 10 row(s)
--16/08/04 11:10:20 INFO CliDriver: Time taken: 190.024 seconds, Fetched 10 row(s)
--Time taken: 191.238 seconds, Fetched 10 row(s)
--16/08/04 11:27:12 INFO CliDriver: Time taken: 191.238 seconds, Fetched 10 row(s)


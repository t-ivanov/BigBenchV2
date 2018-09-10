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
		lg.wl_customer_id
	from web_logs wl
		lateral view 
		json_tuple(
		wl.line,
		'wl_customer_id'
	) lg as 
		wl_customer_id
	where 
		lg.wl_customer_id is not null
	) l,
	customers
where
	l.wl_customer_id = c_customer_id
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


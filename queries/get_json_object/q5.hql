--
-- Find the 10 most browsed products
--
set q5_limit=10;

-- set the database
use BigBenchV2;

-- query 5
select
	i_name,
	count(*) as cnt
from
	web_pages,
	items,
	(
	select 
		get_json_object(web_logs.line, '$.wl_customer_id') as wl_customer_id,
		get_json_object(web_logs.line, '$.wl_item_id') as wl_item_id,
		get_json_object(web_logs.line, '$.wl_webpage_name') as wl_webpage_name
	from web_logs 
	) logs
where
	logs.wl_customer_id is not NULL 
	and logs.wl_item_id is not NULL
	and logs.wl_webpage_name = w_web_page_name
	and w_web_page_type = 'product look up'
	and logs.wl_item_id = i_item_id
group by i_name
order by cnt desc
limit ${hiveconf:q5_limit};

-- result with SF1/40GB clicks.json
-- latest JSON UDF impl.
--OK
--item#0498	1822
--item#0629	1816
--item#0000	1803
--item#0109	1802
--item#0206	1799
--item#0957	1794
--item#0182	1786
--item#0477	1784
--item#0533	1782
--item#0956	1779
--Time taken: 174.457 seconds, Fetched: 10 row(s)


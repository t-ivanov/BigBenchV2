--
-- Find the 10 most browsed products
--
set q1_limit=10;

-- set the database
use BigBenchV2;

-- query 1
select
	i_name,
	count(*) as cnt
from
	web_pages,
	items,
	(
	select 
		js.wl_customer_id,
		js.wl_item_id,
		js.wl_webpage_name
	from web_logs
		lateral view 
			json_tuple(
			web_logs.line,
			'wl_customer_id',
			'wl_item_id',
			'wl_webpage_name'
		) js as 
		wl_customer_id,
		wl_item_id,
		wl_webpage_name
	where
		js.wl_customer_id is not NULL 
		and js.wl_item_id is not NULL
	) logs
where
	logs.wl_webpage_name = w_web_page_name
	and w_web_page_type = 'product look up'
	and logs.wl_item_id = i_item_id
group by i_name
order by cnt desc
limit ${hiveconf:q1_limit};
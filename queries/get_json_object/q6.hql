--
-- Find the 5 most browsed products that are not purchased.
--
-- Find out how many times a product is 'looked up' and
-- subtract from this count how many time this product has
-- been purchased (added to cart).
--

-- set the database
use BigBenchV2;

drop view if exists browsed;
create view browsed as
select
	wl_item_id as br_id, 
	count(*) as br_count
from
	web_pages,
	(
	select 
		get_json_object(web_logs.line, '$.wl_customer_id') as wl_customer_id,
		get_json_object(web_logs.line, '$.wl_item_id') as wl_item_id,
		get_json_object(web_logs.line, '$.wl_webpage_name') as wl_webpage_name
	from web_logs
	) logs 
where	logs.wl_customer_id is not NULL 
	and logs.wl_item_id is not NULL
	and logs.wl_webpage_name = w_web_page_name
	and w_web_page_type = 'product look up'
group by wl_item_id;


drop view if exists purchased;
create view purchased as
select
	wl_item_id as pu_id, 
	count(*) as pu_count
from
	web_pages,
	(
	select 
		get_json_object(web_logs.line, '$.wl_customer_id') as wl_customer_id,
		get_json_object(web_logs.line, '$.wl_item_id') as wl_item_id,
		get_json_object(web_logs.line, '$.wl_webpage_name') as wl_webpage_name
	from web_logs
	) logs 
where logs.wl_customer_id is not NULL 
	and logs.wl_item_id is not NULL
	and logs.wl_webpage_name = w_web_page_name
	and w_web_page_type = 'add to cart'
group by wl_item_id;

select 
	i_item_id,
	(br_count-pu_count) as cnt
from
	browsed, 
	purchased,
	items
where
	br_id = pu_id
	and br_id = i_item_id
order by cnt desc
limit 5;

-- result with SF1/40GB cliks.json
-- latest JSON UDF impl.
-- OK
-- 498	1514
-- 629	1505
-- 109	1490
-- 445	1485
-- 16	1475
-- Time taken: 501.485 seconds, Fetched: 5 row(s)



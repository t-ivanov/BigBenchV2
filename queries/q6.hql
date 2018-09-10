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
	wl_webpage_name = w_web_page_name
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
	wl_webpage_name = w_web_page_name
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
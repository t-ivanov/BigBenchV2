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
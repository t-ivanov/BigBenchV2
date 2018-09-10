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

-- result with SF1/40GB clicks.json
-- latest JSON UDF impl.
--Total MapReduce CPU Time Spent: 50 minutes 28 seconds 10 msec
--OK
--item#0001       2701
--item#0726       2678
--item#0821       2663
--item#0925       2661
--item#0235       2659
--item#0028       2656
--item#0640       2648
--item#0561       2646
--item#0899       2645
--item#1206       2644
--Time taken: 230.255 seconds, Fetched: 10 row(s)

--Total MapReduce CPU Time Spent: 50 minutes 36 seconds 180 msec
--OK
--item#0001       2701
--item#0726       2678
--item#0821       2663
--item#0925       2661
--item#0235       2659
--item#0028       2656
--item#0640       2648
--item#0561       2646
--item#0899       2645
--item#1206       2644
--Time taken: 234.186 seconds, Fetched: 10 row(s)

-- old implementation
-- Total MapReduce CPU Time Spent: 0 days 1 hours 22 minutes 12 seconds 10 msec
--OK
--item#0001       2701
--item#0726       2678
--item#0821       2663
--item#0925       2661
--item#0235       2659
--item#0028       2656
--item#0640       2648
--item#0561       2646
--item#0899       2645
--item#1206       2644
--Time taken: 301.365 seconds, Fetched: 10 row(s)

--Total MapReduce CPU Time Spent: 0 days 1 hours 24 minutes 9 seconds 370 msec
--OK
--item#0339       28397
--item#0242       28368
--item#0868       28333
--item#0606       28310
--item#0340       28278
--item#0526       28249
--item#1136       28235
--item#0456       28234
--item#0684       28232
--item#0633       28231
--Time taken: 301.405 seconds, Fetched: 10 row(s)

-- result with test.json 
--Total MapReduce CPU Time Spent: 6 seconds 930 msec
--OK
--item#1161       4
--item#1071       4
--item#0179       4
--item#0762       4
--item#0079       4
--item#1189       3
--item#0339       3
--item#0055       3
--item#0442       3
--item#1212       3
--Time taken: 54.276 seconds, Fetched: 10 row(s)

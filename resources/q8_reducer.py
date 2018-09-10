import sys
import logging
import traceback
import os
import time
from time import strftime

category=sys.argv[1] 

'''
To test this script exec:
#intput format tab separated with \t: uid\tc_date\tc_time\tsales_sk\twpt
echo -e "1\t1234\t1234\t1234\treview
1\t1235\t1235\t1234\treview
2\t1234\t1234\t234\treview
2\t1235\t1235\t234\treview" | python q8_reducer.py "review"
'''

def npath(vals):
	#vals ((int(c_date), int(c_time),sales_sk, wpt)
	vals.sort()
	ready = 0
	for val in vals:
		if ready == 0 and val[3] == category:
			ready = 1
			continue
		
		if ready == 1 and val[2] != '\N':
			c_date = val[0]
			sales_sk= val[2]
			print "%s\t%s" % (c_date, sales_sk)
			ready = 0

if __name__ == "__main__":
	line = ''
	try:
		current_key = ''
		vals = []
		#partition by uid
		#order by c_date, c_time
		#The plan: create an vals[] per uid, with layout (c_date, c_time, sales_sk, wpt)
		
		for line in sys.stdin:
			#print("line:" + line + "\n")
			uid, c_date, c_time, sales_sk, wpt = line.strip().split("\t")

			#ignore date time parsing errors
			try:
				c_date = int(c_date)
				c_time = int(c_time)
			except ValueError:
				c_date = -1
				c_time = -1
				continue

			if current_key == '' :
				current_key = uid
				vals.append((c_date, c_time, sales_sk, wpt))

			elif current_key == uid :
				vals.append((c_date, c_time, sales_sk, wpt))

			elif current_key != uid :
				npath(vals)
				vals = []
				current_key = uid
				vals.append((c_date, c_time, sales_sk, wpt))

		npath(vals)

	except:
	 ## should only happen if input format is not correct, like 4 instead of 5 tab separated values
		logging.basicConfig(level=logging.DEBUG, filename=strftime("/tmp/bigbench_q8_reducer_%Y%m%d-%H%M%S.log"))
		logging.info('category: ' +category )
		logging.info("line from hive: \"" + line + "\"")
		logging.exception("Oops:")
		sys.exit(1)
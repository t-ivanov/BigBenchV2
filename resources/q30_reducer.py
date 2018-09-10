import sys

def combinations(vals):
	vals.sort()
	last_cat_id = -1
        distinct_cat = []
	for i in vals:
		if last_cat_id != i[0]:
			last_cat_id = i[0]
			for j in distinct_cat:
				print "%s\t%s\t%s\t%s" % (i[0],i[1],j[0],j[1])
				print "%s\t%s\t%s\t%s" % (j[0],j[1],i[0],i[1])
                        distinct_cat.append(i)

if __name__ == "__main__":
	
	last_ordernum = ''
	vals = []

	for line in sys.stdin:
		ordernum, cat_id, cat = line.strip().split("\t")

		if ordernum != '\N' :
			if last_ordernum == ordernum :
				vals.append((cat_id, cat))
			else :
				combinations(vals)
				vals = []
				last_ordernum = ordernum;
				vals.append((cat_id,cat))
	combinations(vals)
	
except:
		logging.basicConfig(level=logging.DEBUG, filename=strftime("/tmp/bigbench_q30_reducer_%Y%m%d-%H%M%S.log"))
		logging.info('category: ' + ordernum )
		logging.info("line from hive: \"" + line + "\"")
		logging.exception("Oops:")
		sys.exit(1)
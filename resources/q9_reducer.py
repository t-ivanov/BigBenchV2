import sys

timeout = long(sys.argv[1])

def sessionize(vals):
	session = 1
	vals.sort()
	cur_time = vals[0][0]
	for tup in vals:
		if tup[0] - cur_time > timeout:
			session += 1
		cur_time = tup[0]
		print "%s\t%s\t%s" % (tup[1], tup[0], tup[1]+'_'+str(session))


if __name__ == "__main__":
	
	current_key = ''
	vals = []

	for line in sys.stdin:
		key, val3 = line.strip().split("\t")

		if current_key == '' :
			current_key = key
			vals.append((int(val3), key))

		elif current_key == key:
			vals.append((int(val3), key))

		elif current_key != key:
			sessionize(vals)
			vals = []
			current_key = key
			vals.append((int(val3), key))

	sessionize(vals)	
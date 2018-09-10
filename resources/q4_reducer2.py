import sys

def npath(vals):
	vals.sort()
	last_order = -1
	last_dynamic = -1
	for i in range(len(vals)):
		if vals[i][3] == 'order':
			last_order = i
		if vals[i][3] == 'dynamic':
			last_dynamic = i 
	
	if last_dynamic > last_order:	
		print "%s\t%s\t%s" % (vals[last_order+1][4], str(vals[last_order+1][0]), str(vals[-1][0]))
	

if __name__ == "__main__":
	
	current_key = ''
	vals = []

	for line in sys.stdin:
		val1, val2, val3, val4, key = line.strip().split("\t")

		if current_key == '' :
			current_key = key
			vals.append((int(val4), val1, val2, val3, key))

		elif current_key == key:
			vals.append((int(val4), val1, val2, val3, key))

		elif current_key != key:
			npath(vals)
			vals = []
			current_key = key
			vals.append((int(val4), val1, val2, val3, key))

	npath(vals)	
	
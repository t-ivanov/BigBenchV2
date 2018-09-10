import sys
import traceback
import os

days_param = long(sys.argv[1])

def npath(vals):
	vals.sort()
	last_viewed_item = -1
	last_viewed_date = -1
	for i in vals:
		if i[2] == '\N' and i[1] != '\N':
			last_viewed_item = i[1]
			last_viewed_date = i[0]
		elif i[2] != '\N' and i[1] != '\N' and last_viewed_item > -1 and last_viewed_date >= (i[0]- days_param ) :
			print "%s\t%s" % (last_viewed_item, i[1])
			last_viewed_item = i[1]
			last_viewed_date = i[0]
		

if __name__ == "__main__":

	last_user = ''
	vals = []

	for line in sys.stdin:
		user, wcs_date, item_key, sale = line.strip().split("\t")

		try:
			wcs_date = long(wcs_date)
			item_key = long(item_key)
		except ValueError:
			wcs_date = -1
			item_key = -1
			continue


		if last_user == user :
			vals.append((wcs_date, item_key, sale))
		else :
			npath(vals)
			vals = []
			last_user = user;
			vals.append((wcs_date, item_key, sale))
	npath(vals)	
	
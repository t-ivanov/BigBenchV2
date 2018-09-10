import json
import sys
# pip install pydoop
#import pydoop.hdfs as hdfs

# get the input vars
#attr_number = sys.argv.len

	
try:
	# get the values
	#with hdfs.open('/user/user1/semibench/data/web_logs/test.json') as f:
	#	for line in f:
		for line in sys.stdin:
			log_data = []
			attributes = []
			attributes = line.split('|')
			#print attributes[0]
			# load line as json
			json_row = json.loads(attributes[1])
			for attr in sys.argv[1:]:
				# if attribute not existing in json line skip the line
				if attr not in json_row:
					break
				# extract the attribute and append it in array
				log_data.append(json_row[attr]) 
			print "\t".join(str(x) for x in log_data)			
			#	print "%s\t%s\t%s" % (item, user, attr3)
		
except:
		## should only happen if input format is not correct, like 4 instead of 5 tab separated values
		logging.basicConfig(level=logging.DEBUG, filename=strftime("/tmp/stream_parse.py_%Y%m%d-%H%M%S.log"))
		#logging.info("sys.argv[1] timeout: " +str(timeout) + " line from hive: \"" + line + "\"")
		logging.exception("Oops:") 
		raise
		sys.exit(1)		
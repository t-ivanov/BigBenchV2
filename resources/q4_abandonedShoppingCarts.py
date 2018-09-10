#"INTEL CONFIDENTIAL"
#Copyright 2016 Intel Corporation All Rights Reserved. 
#
#The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
#
#No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

import sys
import logging
import traceback
import os
import time
from time import strftime


if __name__ == "__main__":
	#lines are expected to be grouped by sessionid and presorted by timestamp 
	line = ''
	current_key = ''
	session_row_counter = 0
	last_order_row = -1
	last_dynamic_row = -1

	try:

		for line in sys.stdin:
		
			wptype, sessionid  = line.strip().split("\t")

			if current_key != sessionid:
				#is abandoned shopping carts?
				if last_dynamic_row > last_order_row :	
				#if last_dynamic_row > last_order_row and (last_order_row == -1 or last_dynamic_tstamp >= last_order_tstamp ):	
					print session_row_counter
					
				#reset for next sessionid
				session_row_counter = 1
				current_key = sessionid
				last_order_row = -1
				last_dynamic_row = -1

				
			else :
				session_row_counter = session_row_counter + 1
				
			if wptype == 'add to cart':
				last_order_row = session_row_counter
			if wptype == 'checkout':
				last_dynamic_row = session_row_counter	
			
			#debug print
			#print "Debug: %s\t%s\t%s\t===\t%s\t%s" % (current_key,wptype,sessionid,last_order_row,last_dynamic_row)
			
		#process last tuple	
		if last_dynamic_row > last_order_row :	
			#if last_dynamic_row > last_order_row and (last_order_row == -1 or last_dynamic_tstamp >= last_order_tstamp ):	
			print session_row_counter

	except:
		## should only happen if input format is not correct
		logging.basicConfig(level=logging.DEBUG, filename=strftime("/tmp/bigbench_q8_abandonedreducer2.py_%Y%m%d-%H%M%S.log"))
		logging.info("line from hive: \"" + line + "\"")
		logging.exception("Oops:") 
		raise
		sys.exit(1)	
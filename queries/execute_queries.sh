#!/bin/bash

# Initialize log file for data loading times
LOG_FILE_EXEC_TIMES="query_times.csv"
if [ ! -e "$LOG_FILE_EXEC_TIMES" ]
  then
    touch "$LOG_FILE_EXEC_TIMES"
    echo "STARTDATE_EPOCH|STOPDATE_EPOCH|DURATION_MS|STARTDATE|STOPDATE|DURATION|BENCHMARK|DATABASE|SCALE_FACTOR|ENGINE|FILE_FORMAT|QUERY" >> "${LOG_FILE_EXEC_TIMES}"
fi

if [ ! -w "$LOG_FILE_EXEC_TIMES" ]
  then
    echo "ERROR: cannot write to: $LOG_FILE_EXEC_TIMES, no permission"
    return 1
fi


TEST_QUERIES="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 27 29 30"
#"12 13 14 15 16 17 18 19 20 21 22 23 24 27 29 30"
#"1 2 3 4 5 6 7 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 27 29 30"
# 8

for i in ${TEST_QUERIES}
do
#	if [ $HOSTFILE != "" ]
#	then
#	  echo "Drop all caches from all nodes list in ${HOSTFILE}"
#  	$BENCH_HOME/bin/drop-cache-all.sh $HOSTFILE
#	fi
HOSTLIST="141.2.2.172 141.2.2.171 141.2.2.170 141.2.2.169"
for HOST in $HOSTLIST; do
  # description of the command --> http://unix.stackexchange.com/questions/87908/how-do-you-empty-the-buffers-and-cache-on-a-linux-system
  ssh -t -t $HOST 'echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su' &
done

	
	# Measure time for query execution time
	# Start timer to measure data loading for the file formats
	STARTDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STARTDATE_EPOCH="`date +%s`" # seconds since epochstart
	
		echo "Hive query: ${i}"
		hive  -f q${i}.hql > query${i}_log.txt 2>&1

	# Calculate the time
	STOPDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STOPDATE_EPOCH="`date +%s`" # seconds since epoch
	DIFF_s="$(($STOPDATE_EPOCH - $STARTDATE_EPOCH))"
	DIFF_ms="$(($DIFF_s * 1000))"
	DURATION="$(($DIFF_s / 3600 ))h $((($DIFF_s % 3600) / 60))m $(($DIFF_s % 60))s"
	# log the times in load_time.csv file
	echo "${STARTDATE_EPOCH}|${STOPDATE_EPOCH}|${DIFF_ms}|${STARTDATE}|${STOPDATE}|${DURATION}|${BENCHMARK}|${DATABASE}|${SCALE}|${ENGINE}|${FILE_FORMAT}|Query ${i}" >> ${LOG_FILE_EXEC_TIMES}


done

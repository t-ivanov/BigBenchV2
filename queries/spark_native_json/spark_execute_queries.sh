#!/bin/bash

# define the number of runs that should be executed with the same scale factor
number_runs="1 2 3"

for r in ${number_runs}
do

# create the result directory
mkdir run${r}



# Initialize log file for data loading times
LOG_FILE_EXEC_TIMES="query_times.csv"
if [ ! -e "run${r}/$LOG_FILE_EXEC_TIMES" ]
  then
    touch "run${r}/$LOG_FILE_EXEC_TIMES"
    echo "STARTDATE_EPOCH|STOPDATE_EPOCH|DURATION_MS|STARTDATE|STOPDATE|DURATION|BENCHMARK|DATABASE|SCALE_FACTOR|ENGINE|FILE_FORMAT|QUERY" >> run${r}/${LOG_FILE_EXEC_TIMES}
fi

if [ ! -w "run${r}/$LOG_FILE_EXEC_TIMES" ]
  then
    echo "ERROR: cannot write to: run${r}/$LOG_FILE_EXEC_TIMES, no permission"
    return 1
fi


#HOSTFILE=$BENCH_HOME/bin/hostlist

TEST_QUERIES="24_part1"
#"5 6 7 9 12 13 16 17 19 22 23 24"
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
	
		echo "Spark query: ${i}"
		/home/user1/spark/bin/spark-sql "-v --driver-memory 10g --executor-memory 9g --executor-cores 3 --num-executors 9 --master yarn" -f q${i}.hql > run${r}/query${i}_log.txt 2>&1

	# Calculate the time
	STOPDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STOPDATE_EPOCH="`date +%s`" # seconds since epoch
	DIFF_s="$(($STOPDATE_EPOCH - $STARTDATE_EPOCH))"
	DIFF_ms="$(($DIFF_s * 1000))"
	DURATION="$(($DIFF_s / 3600 ))h $((($DIFF_s % 3600) / 60))m $(($DIFF_s % 60))s"
	# log the times in load_time.csv file
	echo "${STARTDATE_EPOCH}|${STOPDATE_EPOCH}|${DIFF_ms}|${STARTDATE}|${STOPDATE}|${DURATION}|${BENCHMARK}|${DATABASE}|${SCALE}|${ENGINE}|${FILE_FORMAT}|Query ${i}" >> run${r}/${LOG_FILE_EXEC_TIMES}

	
done

 # sleep 5 minutes between the runs
 sleep 5m

 # finish the run
done 
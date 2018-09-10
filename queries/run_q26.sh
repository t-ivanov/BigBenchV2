#!/bin/bash

echo "executing query 26"
# Measure time for query execution time
	# Start timer to measure data loading for the file formats
	STARTDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STARTDATE_EPOCH="`date +%s`" # seconds since epochstart

	#EXECUTION Plan:
	#step 1.  hive q26.sql		:	Run hive querys to extract kmeans input data
	#step 2.  mahout input		:	Generating sparse vectors
	#step 3.  mahout kmeans		:	Calculating k-means"
	#step 4.  mahout dump > hdfs/res:	Converting result and copy result do hdfs query result folder
	#step 5.  hive && hdfs 		:	cleanup.sql && hadoop fs rm MH

#step 1. 
# run Hive and create the tables
# Write input for k-means into temp table
hive -f q26.hql
	
	
TEMP_RESULT_DIR="/user/hive/warehouse/bigbenchv2.db/q26_results"	
MAHOUT_TEMP_DIR="/tmp/mahout_temp"
# create the result dir
# hadoop fs -mkdir /user/user1/bigbenchv2/q26
RESULT_DIR="/user/user1/bigbenchv2/q26"
# clean the result before starting
hadoop fs -rm -r -f /user/user1/bigbenchv2/q26/cluster.txt

#step 2.
mahout org.apache.mahout.clustering.conversion.InputDriver -i "${TEMP_RESULT_DIR}" -o "${TEMP_RESULT_DIR}/Vec" -v org.apache.mahout.math.RandomAccessSparseVector
	
#step 3.
mahout kmeans --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_RESULT_DIR/Vec" -c "$TEMP_RESULT_DIR/init-clusters" -o "$TEMP_RESULT_DIR/kmeans-clusters" -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -k 8 -ow -cl

#step 4.
mahout clusterdump --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_RESULT_DIR"/kmeans-clusters/clusters-*-final  -dm org.apache.mahout.common.distance.CosineDistanceMeasure -of TEXT | hadoop fs -copyFromLocal - "${RESULT_DIR}/cluster.txt"

#step 5.
hadoop fs -rm -r -f "$TEMP_RESULT_DIR"

# Calculate the time
	STOPDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STOPDATE_EPOCH="`date +%s`" # seconds since epoch
	DIFF_s="$(($STOPDATE_EPOCH - $STARTDATE_EPOCH))"
	DIFF_ms="$(($DIFF_s * 1000))"
	DURATION="$(($DIFF_s / 3600 ))h $((($DIFF_s % 3600) / 60))m $(($DIFF_s % 60))s"
# print times
 echo "query execution time: ${DIFF_s} (sec)| ${DURATION}"
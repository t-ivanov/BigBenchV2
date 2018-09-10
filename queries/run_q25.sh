#!/bin/bash

echo "executing query 25"
# Measure time for query execution time
	# Start timer to measure data loading for the file formats
	STARTDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STARTDATE_EPOCH="`date +%s`" # seconds since epochstart

# run Hive and create the tables
# Write input for k-means into temp table
hive -f q25.hql

	#EXECUTION Plan:
	#step 1.  hive q25.sql		:	Run hive querys to extract kmeans input data
	#step 2.  mahout input		:	Generating sparse vectors
	#step 3.  mahout kmeans		:	Calculating k-means"
	#step 4.  mahout dump > hdfs/res:	Converting result and copy result do hdfs query result folder
	#step 5.  hive && hdfs 		:	cleanup.sql && hadoop fs rm MH
	
#TEMP_RESULT_TABLE="q25_results"
#TEMP_DIR="/user/user1"
TEMP_RESULT_DIR="/user/hive/warehouse/bigbenchv2.db/q25_results"	
MAHOUT_TEMP_DIR="/tmp/mahout_temp"
# create the result dir
# hadoop fs -mkdir /user/hive/warehouse/bigbenchv2.db/q25_results
RESULT_DIR="/user/user1/bigbenchv2/q25"
# clean the result before starting
hadoop fs -rm -r -f /user/user1/bigbenchv2/q25/cluster.txt

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

#-- hive  SF1 clicks.json 
#-- Hive + Mahout on MR
#VL-6542{n=1998 c=[13475.105, 0.082, 6.539, 1029.392] r=[4529.429, 0.274, 3.137, 414.071]}
#VL-12389{n=5747 c=[4692.175, 0.787, 16.083, 6219.525] r=[3848.406, 0.410, 6.118, 3079.372]}
#VL-12657{n=2171 c=[11603.980, 0.381, 11.781, 2624.504] r=[4626.772, 0.486, 4.975, 1084.904]}
#VL-4053{n=2323 c=[10495.183, 0.769, 15.548, 5794.262] r=[5122.283, 0.421, 5.884, 2832.443]}
#VL-16141{n=2041 c=[11185.812, 0.676, 13.899, 4451.333] r=[5272.784, 0.468, 5.174, 2144.566]}
#VL-19303{n=1382 c=[13700.217, 0.024, 2.982, 362.705] r=[4424.664, 0.153, 1.716, 219.948]}
#VL-7029{n=1330 c=[11187.561, 0.545, 12.498, 3392.283] r=[5074.571, 0.498, 4.884, 1550.970]}
#VL-15992{n=2817 c=[13122.954, 0.178, 10.350, 1868.314] r=[4564.757, 0.382, 4.376, 712.404]}
 
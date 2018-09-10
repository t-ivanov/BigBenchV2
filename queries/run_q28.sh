#!/bin/bash

echo "executing query 28"
# Measure time for query execution time
	# Start timer to measure data loading for the file formats
	STARTDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STARTDATE_EPOCH="`date +%s`" # seconds since epochstart

TEMP_TABLE1="/user/hive/warehouse/bigbenchv2.db/q28_temp1_training"
#TEMP_DIR1="$TEMP_DIR/$TEMP_TABLE1"
TEMP_TABLE2="/user/hive/warehouse/bigbenchv2.db/q28_temp2_testing"
#TEMP_DIR2="$TEMP_DIR/$TEMP_TABLE2"
MAHOUT_TEMP_DIR="/tmp/mahout_temp"	
# create the result dir
# hadoop fs -mkdir /user/user1/bigbenchv2/q28
RESULT_DIR="/user/user1/bigbenchv2/q28"
HDFS_RESULT_FILE="${RESULT_DIR}/classifierResult.txt"
HDFS_RAW_RESULT_FILE="${RESULT_DIR}/classifierResult_raw.txt"

# create the temp dir
# hadoop fs -mkdir /user/user1/bigbenchv2/q28/temp
TEMP_DIR="/user/user1/bigbenchv2/q28/temp"	
SEQ_FILE_1="$TEMP_DIR/Seq1"
SEQ_FILE_2="$TEMP_DIR/Seq2"
VEC_FILE_1="$TEMP_DIR/Vec1"
VEC_FILE_2="$TEMP_DIR/Vec2"


#step 1/7:Executing hive queries
hive -f q28.hql


#step 2/7: Generating sequence files
hadoop jar "/home/user1/semi_bench/queries/resources/bigbenchqueriesmr.jar" de.bankmark.bigbench.queries.q28.ToSequenceFile "${TEMP_TABLE1}" "$SEQ_FILE_1"
hadoop jar "/home/user1/semi_bench/queries/resources/bigbenchqueriesmr.jar" de.bankmark.bigbench.queries.q28.ToSequenceFile "${TEMP_TABLE2}" "$SEQ_FILE_2"

#step 3/7: Generating sparse vectors from sequence files
mahout seq2sparse -i "$SEQ_FILE_1" -o "$VEC_FILE_1" -ow -lnorm -nv -wt tfidf
mahout seq2sparse -i "$SEQ_FILE_2" -o "$VEC_FILE_2" -ow -lnorm -nv -wt tfidf

#step 4/7: Training Classifier
mahout trainnb --tempDir "$MAHOUT_TEMP_DIR" -i "$VEC_FILE_1/tfidf-vectors" -o "$TEMP_DIR/model" -el -li "$TEMP_DIR/labelindex" -ow

#step 5/7: Testing Classifier
mahout testnb --tempDir "$MAHOUT_TEMP_DIR" -i "$VEC_FILE_2/tfidf-vectors" -m "$TEMP_DIR/model" -l "$TEMP_DIR/labelindex" -ow -o "$TEMP_DIR/result" |& tee >( grep -A 300 "Standard NB Results:" | hadoop fs  -copyFromLocal -f - "$HDFS_RESULT_FILE" )

#step 6/7: dump result to hdfs
mahout seqdumper --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_DIR/result/part-m-00000" | hadoop fs -copyFromLocal -f - "$HDFS_RAW_RESULT_FILE"

#Step 7/7: Clean up
hadoop fs -rm -r -f "$TEMP_DIR"

# Calculate the time
	STOPDATE="`date +%Y/%m/%d:%H:%M:%S`"
	STOPDATE_EPOCH="`date +%s`" # seconds since epoch
	DIFF_s="$(($STOPDATE_EPOCH - $STARTDATE_EPOCH))"
	DIFF_ms="$(($DIFF_s * 1000))"
	DURATION="$(($DIFF_s / 3600 ))h $((($DIFF_s % 3600) / 60))m $(($DIFF_s % 60))s"
# print times
 echo "query execution time: ${DIFF_s} (sec)| ${DURATION}"

#16/07/03 14:17:49 INFO test.TestNaiveBayesDriver: Standard NB Results:
#=======================================================
#Summary
#-------------------------------------------------------
#Correctly Classified Instances          :      21871       73.5555%
#Incorrectly Classified Instances        :       7863       26.4445%
#Total Classified Instances              :      29734
#=======================================================
#Confusion Matrix
#-------------------------------------------------------
#a       b       c       <--Classified as
#5310    560     0        |  5870        a     = NEG
#3578    13072   3363     |  20013       b     = NEU
#0       362     3489     |  3851        c     = POS
#=======================================================
#Statistics
#-------------------------------------------------------
#Kappa                                       0.5549
#Accuracy                                   73.5555%
#Reliability                                61.5943%
#Reliability (standard deviation)            0.4275
#16/07/03 14:17:49 INFO driver.MahoutDriver: Program took 18743 ms (Minutes: 0.31238333333333335)
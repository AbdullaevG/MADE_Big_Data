#!/usr/bin/env bash
set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
HDFS_INPUT_DIR=$1
HDFS_OUTPUT_DIR=$2
JOB_NAME=$3

hdfs dfs -rm -r -skipTrash $HDFS_OUTPUT_DIR

( yarn jar $HADOOP_STREAMING_JAR \
        -files count_mapper.py,sum_reducer.py \
        -D mapreduce.job.name="Calculate tags count" \
        -D stream.num.map.output.key.fields=2 \
        -D stream.num.reduce.output.key.fields=2 \
        -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
        -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2" \
        -D mapreduce.partition.keypartitioner.options=-k1,2 \
        -numReduceTasks 8 \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -mapper 'python3 mapper_1.py' \
        -combiner 'python3 reducer_1.py' \
	-reducer 'python3 reducer_1.py' \
        -input $HDFS_INPUT_DIR \
        -output ${HDFS_OUTPUT_DIR}_tmp &&

yarn jar $HADOOP_STREAMING_JAR \
    -files top_reducer.py,top_mapper.py \
    -D mapreduce.job.name="Count top tags" \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2nr" \
    -mapper 'python3 mapper_2.py' \
    -reducer 'python3 mapper_2.py' \
    -numReduceTasks 1 \
    -input ${HDFS_OUTPUT_DIR}_tmp \
    -output ${HDFS_OUTPUT_DIR}
) || echo "!!!Error!!!"

hdfs dfs -rm -r -skipTrash ${HDFS_OUTPUT_DIR}_tmp

hdfs dfs -cat ${HDFS_OUTPUT_DIR}/*

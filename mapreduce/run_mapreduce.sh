#!/bin/bash
# Runs the streaming job. Adjust the jar path to your Hadoop install.
hdfs dfs -rm -r -f /disaster/mr_out
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapreduce/mapper.py,mapreduce/reducer.py \
  -mapper mapper.py -reducer reducer.py \
  -input /disaster/curated -output /disaster/mr_out
hdfs dfs -cat /disaster/mr_out/part-* | sort -t$'\t' -k2 -nr | head -20

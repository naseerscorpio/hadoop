#!/usr/bin/env bash

/usr/hdp/current/spark2-client/bin/spark-submit \
  --class com.demo.hadoop.JavaWordCount \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 1G \
  --num-executors 2 \
  hadoop-practice-1.0-SNAPSHOT-jar-with-dependencies.jar \
  sample.txt
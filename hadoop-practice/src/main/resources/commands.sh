#!/usr/bin/env bash

/usr/hdp/current/spark2-client/bin/spark-submit \
  --class com.demo.hadoop.JavaWordCount \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 1G \
  --num-executors 2 \
  hadoop-practice-1.0-SNAPSHOT-jar-with-dependencies.jar \
  sample.txt


/usr/hdp/current/spark2-client/bin/spark-submit \
  --class com.demo.hadoop.DirectKafkaStream \
  --master yarn \
  --deploy-mode cluster \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=conf/log4j-spark.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=conf/log4j-spark.properties" \
  --jars "/usr/hdp/current/hbase-client/lib/*.jar" \
  --executor-memory 1G \
  --num-executors 2 \
  hadoop-practice-1.0-SNAPSHOT-jar-with-dependencies.jar \
  "sandbox-hdp.hortonworks.com:6667" "twitterdemo2" "twitter_info"
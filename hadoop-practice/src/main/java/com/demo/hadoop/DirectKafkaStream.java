package com.demo.hadoop;

import com.demo.hadoop.hbase.HBaseDataframeWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

public class DirectKafkaStream {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: DirectKafkaStream <brokers> <groupId> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <groupId> is a consumer group name to consume from topics\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String groupId = args[1];
        String topics = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("DirectKafkaStream");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", LongDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        JavaInputDStream<ConsumerRecord<Long, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
                );

        // new Hadoop API configuration
        stream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
            //Convert to Dataframe
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            Dataset<Row> df = sqlContext.read().json(rdd.map(record -> record.value()));

            //Write DF to hbase
            HBaseDataframeWriter writer = new HBaseDataframeWriter("rdl:tweet_info", "a", "id");
            writer.write(df);

            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}

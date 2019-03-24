package com.demo.hadoop;

import com.demo.hadoop.hbase.HBaseDataframeWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

public class DirectKafkaStream {

    static Logger LOGGER = Logger.getLogger(DirectKafkaStream.class);

    private static String CHECKPOINT_DIR = "/tmp/spark";

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            LOGGER.error("Usage: DirectKafkaStream <brokers> <groupId> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <groupId> is a consumer group name to consume from topics\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String groupId = args[1];
        String topics = args[2];

        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR,
                () -> {
                    SparkConf conf = new SparkConf().setAppName("DirectKafkaStream");
                    JavaSparkContext sc = new JavaSparkContext(conf);
                    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));
                    ssc.checkpoint(CHECKPOINT_DIR);
                    return ssc;
                }, new Configuration(), false);

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
                );

        // new Hadoop API configuration
        stream.foreachRDD(rdd -> {
            LOGGER.info("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                LOGGER.info(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
            if (!rdd.isEmpty()) {
                //Convert to Dataframe
                SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
                Dataset<Row> df = sqlContext.read().json(rdd.map(record -> record.value()));
                df.registerTempTable("tweetDF");
                df.printSchema();

                Dataset<Row> transformedDF = df.sqlContext().sql("SELECT id, text, lang, retweet_count, favorite_count, " +
                        " user.id as user_id, user.screen_name as screen_name, user.location as location " +
                        "  FROM tweetDF ");

                //Write DF to hbase
                HBaseDataframeWriter writer = new HBaseDataframeWriter("rdl:tweet_info", "a", "id");
                writer.write(transformedDF);

                ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            }
        });

        jssc.checkpoint(CHECKPOINT_DIR);

        //run tasks
        stream.count();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}

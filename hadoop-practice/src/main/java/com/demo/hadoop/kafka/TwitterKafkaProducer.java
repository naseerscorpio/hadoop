package com.demo.hadoop.kafka;

import com.demo.hadoop.model.Tweet;
import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer {
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;
    private String topic;

    public TwitterKafkaProducer(String consumerKey, String consumerSecret, String token, String tokenSecret,
                                String topic, String[] tags) {
        this.topic = topic;
        // Configure auth
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        // track the terms of your choice. here im only tracking #bigdata.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Arrays.asList(tags));

        queue = new LinkedBlockingQueue<>(10000);

        client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(authentication)
                .processor(new StringDelimitedProcessor(queue)).build();
        gson = new Gson();
    }

    private Producer<String, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public void run() {
        client.connect();
        try (Producer<String, String> producer = getProducer()) {
            while (true) {

                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);

                String key = tweet.getId_str();
                String msg = gson.toJson(tweet);

                System.out.printf("Fetched tweet msg %s\n", msg);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Message with offset %d acknowledged by partition %d\n",
                                metadata.offset(), metadata.partition());
                    } else {
                        System.out.println(exception.getMessage());
                    }
                });
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    public static void main(String[] args) {
        TwitterKafkaProducer producer = new TwitterKafkaProducer(args[0], args[1], args[2], args[4],
                args[5], Arrays.copyOfRange(args, 6, args.length)
        );
        producer.run();
    }
}

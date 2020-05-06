// Ref: https://github.com/twitter/hbc
package com.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaTwitterProducer {

    // initialize logger
    private final static Logger logger = LoggerFactory.getLogger(KafkaTwitterProducer.class.getName());

    // define auth
    private final static String CONSUMER_KEY =  System.getenv("TWITTER_CONSUMER_KEY");
    private final static String CONSUMER_KEY_SECRET = System.getenv("TWITTER_CONSUMER_KEY_SECRET");
    private final static String ACCESS_TOKEN = System.getenv("TWITTER_ACCESS_TOKEN");
    private final static String ACCESS_TOKEN_SECRET = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    // Generate Client for Twitter
    private static Client getClient(BlockingQueue<String> msgQueue , List<String> terms ){

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a ENV
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    // Search and publish tweets to Kafka
    private static void publish(KafkaProducer<String, String> producer, String topic, List<String> terms, int limit){

        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);

        logger.debug("Creating twitter client");
        Client hosebirdClient = getClient(msgQueue, terms);
        logger.debug("Twitter client");


        logger.info("Connecting Twitter Client");
        // connect with twitter
        hosebirdClient.connect();
        logger.debug("Conneting client");


        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone() && (limit > 0 || limit == -1)) {
            limit -= 1;
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                System.out.println(msg);

                // create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic , msg);

                //  send data
                producer.send(record);

            } catch (InterruptedException e) {
                logger.error("Error occurred while processing: " + e);
                logger.error("Stopping all the processes");
                break;
            }
        }

        // flush producer
        producer.flush();
        logger.debug("Flushed kafka producer!!!");

        // flush and close producer
        producer.close();
        logger.info("Closed kafka producer!!!");

        // stop the twitter connection
        hosebirdClient.stop();
    }

    // main method
    public static void main(String[] args) {

        // define bootstrap server endpoint
        String bootstrapServer = "127.0.0.1:9092";

        // define topic
        // on terminal: kafka-topics --bootstrapServer 127.0.0.1:9092 --topic twitter_covid_topic --create \
        // --partitions 3 --replication-factor 1
        String topic = "twitter_covid_topic";
        logger.info("Kafka Topic: " + topic);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // define search terms
        List<String> terms = Lists.newArrayList ("covid-19");
        logger.info("Twitter Terms: " + terms.toString());

        // define limit. if limit == -1 => No limit
        int limit = 10;
        logger.info("Total limit is set to: " + limit);

        // call pushlish to start producing messages and send to kafka
        publish(producer, topic, terms, limit);

    }

}

package com.kafkabasics.tutorials;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){

        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-second_application";
        String topic = "my_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating a consumer thread");
        Runnable myConsumerRunnable= new ConsumerRunnable(
                topic,
                bootstrapServer,
                groupId,
                latch
        );

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        // add the shutdown
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caugth shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application is closing");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application get interrupted");
        } finally {
            logger.info("Application is closing");
        }


    }

    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String topic,
                              String bootstrapServer,
                              String groupId,
                              CountDownLatch latch){
            this.latch = latch;

            // create properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to out topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run(){
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // special method to interrupt consumer.poll()
            // it will throw WakeUpException
            consumer.wakeup();
        }
    }
}

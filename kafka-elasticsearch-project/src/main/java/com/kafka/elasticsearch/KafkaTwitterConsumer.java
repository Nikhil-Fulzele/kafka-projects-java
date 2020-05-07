package com.kafka.elasticsearch;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaTwitterConsumer {

    private static JsonParser jsonParser = new JsonParser();

    // initialize logger
    private final static Logger logger = LoggerFactory.getLogger(KafkaTwitterConsumer.class.getName());

    private static String getId(String paylaod){
        return jsonParser.parse(paylaod).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) {

        // define bootstrap server endpoint
        String bootstrapServer = "127.0.0.1:9092";

        // define groupID
        String groupId = "twitter-app";

        // define topic
        String topic = "twitter_covid_topic";

        // elastic search hostname
        String hostname = "127.0.0.1";
        int port = 9200;

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic)); // for many topics

        // Create client
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, port, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder;
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);


        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){
                logger.info(records.toString());
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                // Create ES request
                String id = getId(record.value());
                IndexRequest request = new IndexRequest(topic).source(record.value(), XContentType.JSON).id(id);
                try {
                    // Index data
                    IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
                    logger.info("Index resp : " + indexResponse.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

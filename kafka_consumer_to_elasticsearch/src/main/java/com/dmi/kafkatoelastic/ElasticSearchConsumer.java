package com.dmi.kafkatoelastic;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Slf4j
public class ElasticSearchConsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch countDownLatch;
    private volatile  boolean isDone = false;

    public ElasticSearchConsumer(String topicName, CountDownLatch countDownLatch) {
        this.consumer = createConsumer(topicName);
        this.countDownLatch = countDownLatch;
    }

    public RestHighLevelClient createClient() {
        String hostname = "192.168.0.106";
//        String username = "";
//        String password = "";

//        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 9200, "http"));
//                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
//                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    public KafkaConsumer<String, String> createConsumer(String topicName) {
        StringJoiner bootstrapServers = new StringJoiner(",");
        bootstrapServers.add("127.0.0.1:9092");
        String kafkaStringDeserializer = StringDeserializer.class.getName();
        String groupId = "kafka-demo-elasticsearch3";
        String resetOffset = "earliest";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.toString());
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, kafkaStringDeserializer);
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, kafkaStringDeserializer);
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, resetOffset);

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //subscribe consumer to topic
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        return kafkaConsumer;
    }

    @Override
    public void run() {
        try (RestHighLevelClient client = createClient()) {
            JSONObject json = new JSONObject();
            while (!isDone) {
                ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {}, value: {}", record.key(), record.value());
                    log.info("Partition: {}, offset: {}", record.partition(), record.offset());

                    json.put("uuid", record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(json.toString(), XContentType.JSON);

                    IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = index.getId();
                    log.info(id);
                }
                log.info("1");
            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal!");
        } catch (IOException e) {
            log.error("Getting error!");
        } finally {
            log.info("Closing consumer ...");
            consumer.close();
            countDownLatch.countDown();
            log.info("Consumer has closed!");
        }
    }

    public void shutdown() {
        isDone = true;
        consumer.wakeup();
    }
}
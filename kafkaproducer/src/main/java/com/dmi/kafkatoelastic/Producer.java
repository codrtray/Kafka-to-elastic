package com.dmi.kafkatoelastic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.StringJoiner;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class Producer implements Runnable {

    private final KafkaProducer<String, String> producer;

    private final String topicName;
    private final MessageCreator messageCreator;


    public Producer(String topicName) {
        this.topicName = topicName;

        Properties properties = createProperties();

        //create the producer
        this.producer = new KafkaProducer<>(properties);
        messageCreator = new MessageCreator();
    }

    private Properties createProperties() {
        // create Producer properties
        StringJoiner bootstrapServers = new StringJoiner(",");
        bootstrapServers.add("127.0.0.1:9092");

        String kafkaStringSerializer = StringSerializer.class.getName();

        Properties prop = new Properties();
        prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.toString());
        prop.setProperty(KEY_SERIALIZER_CLASS_CONFIG, kafkaStringSerializer);
        prop.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, kafkaStringSerializer);

        //set as safer producer
        prop.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ACKS_CONFIG, "all");
        prop.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        prop.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer
        prop.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        prop.setProperty(LINGER_MS_CONFIG, "20");
        prop.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32 KB batch size

        return prop;
    }

    @Override
    public void run() {

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topicName, "Message - " + messageCreator.getMessage());
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("\n--------------Received new metadata.-------------- \n" +
                                "Topic: {}\n" +
                                "Partition: {}\n" +
                                "Offset: {}\n" +
                                "Timestamp: {}", metadata.topic(), metadata.partition(),
                        metadata.offset(), metadata.timestamp());
            } else {
                log.error("Error while producing {}", exception.getMessage());
            }
        });
        producer.flush();
    }

    public void closeProducer() {
        producer.close();
    }

}

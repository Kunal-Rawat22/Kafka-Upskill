package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("Starting ConsumerDemo");

        String group_id = "my-java-application";
        String topic = "demo_java";

        Properties consumerProperties = new Properties();

        // connection to local Kafka cluster
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");

        // connection to Conduktor Cloud
//        producerProperties.setProperty("security.protocol", "SASL_SSL");
//        producerProperties.setProperty("security.mechanism", "PLAINTEXT");

        // set consumer properties
        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("group.id", group_id);

        //If a consumer already has committed offsets, earliest does nothing
        consumerProperties.setProperty("auto.offset.reset", "earliest");

        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        // subscribe consumer to our topic
        consumer.subscribe(List.of(topic));

        // poll for new data
        while (true) {
            log.info("Waiting for messages to be delivered...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record: records) {
                log.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
            }
        }

    }

}

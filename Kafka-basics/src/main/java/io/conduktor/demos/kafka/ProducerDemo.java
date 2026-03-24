package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Starting ProducerDemo");

        Properties producerProperties = new Properties();

        // connection to local Kafka cluster
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        // connection to Conduktor Cloud
//        producerProperties.setProperty("security.protocol", "SASL_SSL");
//        producerProperties.setProperty("security.mechanism", "PLAINTEXT");

        // set producer properties
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("acks", "1");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        // create a producer record
        String topic = "demo_java";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello World");

        // send data - asynchronous
        producer.send(record);

        // flush data - synchronous
        producer.flush();

        // close the producer
        producer.close();
    }
}

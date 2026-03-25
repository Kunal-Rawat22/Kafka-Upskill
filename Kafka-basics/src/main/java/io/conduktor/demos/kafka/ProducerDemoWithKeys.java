package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

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
        producerProperties.setProperty("batch.size", "400");

        // To specify a custom partitioner, you can set the "partitioner.class" property to the fully qualified class name of your custom partitioner. For example, if you have a custom partitioner class named "MyCustomPartitioner", you would set the property like this:
//        producerProperties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        ProducerDemoWithKeys.sendMultipleMessages(producer);

        // flush data - synchronous
        producer.flush();

        // close the producer
        producer.close();
    }

    public static void sendMultipleMessages(KafkaProducer<String, String> producer) {
        for(int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                // create a producer record
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data - asynchronous
                producer.send(record, (metadata, e) -> {
                    if (Objects.isNull(e)) {
                        log.info("Received new metadata. Key: {} | Partition: {}", key, metadata.partition());
                    } else {
                        log.error("Error while producing", e);
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerDemoWithCallbacks {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

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

        ProducerDemoWithCallbacks.sendMultipleMessages(producer);

        // flush data - synchronous
        producer.flush();

        // close the producer
        producer.close();
    }

    public static void sendMultipleMessages(KafkaProducer<String, String> producer) {
        // By Default, the producer will batch records together to improve efficiency. The "batch.size" property controls the maximum size of a batch in bytes. When the batch size is reached, the producer will send the batch of records to the broker. You can adjust this property based on your use case and the expected size of your messages. In this example, we set the batch size to 400 bytes.
        // By Default, it uses sticky partitioning, which means that all messages with the same key will be sent to the same partition. If you want to use a different partitioning strategy, you can specify a custom partitioner by setting the "partitioner.class" property in the producer configuration. For example, you can use the RoundRobinPartitioner to distribute messages evenly across partitions.
        for(int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {

                // create a producer record
                String topic = "demo_java";
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello World " + i);

                // send data - asynchronous
                producer.send(record, (metadata, e) -> {
                    if (Objects.isNull(e)) {
                        log.info("Received new metadata.\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
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

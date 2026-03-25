package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        log.info("Starting ConsumerDemo");

        String group_id = "my-java-application";
        String topic = "demo_java";

        Properties consumerProperties = new Properties();

        // connection to local Kafka cluster
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");


        // set consumer properties
        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("group.id", group_id);

        //If a consumer already has committed offsets, earliest does nothing
        consumerProperties.setProperty("auto.offset.reset", "earliest");

        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();
            // join the main thread to allow the execution of the code in the main thread to complete
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        // subscribe consumer to our topic
        consumer.subscribe(List.of(topic));

        try {
            // poll for new data
            while (true) {
                log.info("Waiting for messages to be delivered...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("The consumer is now starting to shutdown...");
        } catch (Exception e) {
            log.error("Error occurred while consuming messages", e);
        } finally {
            consumer.close();   // this will also commit the offsets if needed
            log.info("The consumer has now gracefully shutdown");
        }
    }

}

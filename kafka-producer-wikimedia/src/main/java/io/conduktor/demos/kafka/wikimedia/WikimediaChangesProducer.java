package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

public class WikimediaChangesProducer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting WikimediaChangesProducer");

        String bootstrapServers = "localhost:9092";
        String topic = "wikimedia.recentchange";
        String eventUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // set producer properties
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");

        // Setting for safe Kafka Producers (Kafka <= 2.8)
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        producerProperties.setProperty("min.insync.replicas", "2");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(120000));
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // High throughput producer (at the expense of a bit of latency and CPU usage)
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        BackgroundEventSource eventSource = buildEventHandler(producer, topic, eventUrl);
        eventSource.start();

        Thread.sleep(600000);
    }

    private static BackgroundEventSource buildEventHandler(final KafkaProducer<String, String> producer, String topic, String eventSourceUrl) {
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        //Build connect strategy with headers
        HttpConnectStrategy connectStrategy = HttpConnectStrategy.http(URI.create(eventSourceUrl)).header("User-Agent","my-kafka-producer/1.0(contact: admin@ourcompany.com)");

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(connectStrategy);

        return new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder).build();
    }
}

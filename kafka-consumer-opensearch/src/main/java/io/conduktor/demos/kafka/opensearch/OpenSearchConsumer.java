package io.conduktor.demos.kafka.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public class OpenSearchConsumer {
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);
    public static String INDEX_NAME = "wikimedia";

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static void main(String[] args) throws IOException {
        // Create the OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        ObjectMapper objectMapper = new ObjectMapper();

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            kafkaConsumer.wakeup();
            // join the main thread to allow the execution of the code in the main thread to complete
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        // We need to create index if it doesn't exist already
        try (openSearchClient; kafkaConsumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Created index: {}", INDEX_NAME);
            } else log.info("Index already exists");

            kafkaConsumer.subscribe(List.of("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(java.time.Duration.ofMillis(3000));
                log.info("Received {} records", records.count());

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    // Strategy 1: Define an ID using Kafka record coordinates to make our consumer idempotent
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Strategy 2: Extract an ID from the JSON value and use it as document ID in OpenSearch to make our consumer idempotent
                    String id = extractIdFromJson(record.value());

                    // Parse JSON
                    ObjectNode jsonNode = (ObjectNode) objectMapper.readTree(record.value());

                    // Remove problematic field
                    jsonNode.remove("log_params");

                    log.info("Record Id: {}, record: {}", id, record.value());
                    IndexRequest indexRequest = new IndexRequest(INDEX_NAME).source(jsonNode.toString(), XContentType.JSON).id(id);
                    bulkRequest.add(indexRequest);
//                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                    log.info("Indexed record: {}", indexResponse.getId());
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Indexed {} records", bulkResponse.getItems().length);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.error("Error while sleeping", e);
                    }

                    // commit offsets after every batch
                    kafkaConsumer.commitSync();
                    log.info("Committed {} records", records.count());
                }
            }
        } catch (WakeupException e) {
            log.info("The consumer is now starting to shutdown...");
        } catch (Exception e) {
            log.error("Error occurred while consuming messages", e);
        } finally {
            kafkaConsumer.close();   // this will also commit the offsets if needed
            openSearchClient.close();
            log.info("The consumer has now gracefully shutdown");
        }

    }

    public static String extractIdFromJson(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        String group_id = "my-java-application";

        Properties consumerProperties = new Properties();

        // connection to local Kafka cluster
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        // set consumer properties
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //If a consumer already has committed offsets, earliest does nothing
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the consumer
        return new KafkaConsumer<>(consumerProperties);
    }
}


package opensearch.learn.kafka;

import com.google.gson.JsonParser;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.xcontent.XContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient(){
        String connString = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        URI uri = URI.create(connString);

        String userInfo = uri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("http", uri.getHost(), uri.getPort())));
        }
        else {
            String[] auth = userInfo.split(":");

            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(new AuthScope(uri.getHost(), uri.getPort()),new UsernamePasswordCredentials(auth[0],auth[1].toCharArray()));

            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("http", uri.getHost(), uri.getPort())).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(cp)));

        }

        return restHighLevelClient;
    }

    private  static KafkaConsumer<String,String> createKafkaConsumer(){
        String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";
        String groupId = "opensearch-consumers";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
    private static String extractID(String value) {
        return JsonParser.parseString(value).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutting down Detected!! OpenSearch Consumer will now terminate ...");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try (openSearchClient;consumer) {
            if (openSearchClient.indices().exists(new GetIndexRequest("wikipedia"), RequestOptions.DEFAULT)){
                log.info("WIKIPEDIA EXISTS");
            }else {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikipedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("WIKIPEDIA CREATED");
            }

            consumer.subscribe(Collections.singletonList("wikimedia.latestchanges"));

            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(3000));

                int recordsCount = records.count();
                log.info("Consumer Records: " + recordsCount + "records");
                for (ConsumerRecord<String,String> record : records) {
                    try {
                        String record_id = extractID(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikipedia")
                                .source(record.value(), XContentType.JSON)
                                .id(record_id);
                        IndexResponse response = openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
                        log.info("Inserted document with recordID : {} into Open Search Index. Index ID : {}", record_id,response.getId());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }


}

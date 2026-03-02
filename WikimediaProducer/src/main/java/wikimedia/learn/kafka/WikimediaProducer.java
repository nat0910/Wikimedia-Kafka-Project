package wikimedia.learn.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class WikimediaProducer {
    private static final Logger log = Logger.getLogger(WikimediaProducer.class.getName());

    public static void main(String[] args) {
        log.info("Starting Wikimedia Producer");

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            String topic = "wikimedia.latestchanges";
            String url = "https://stream.wikimedia.org/v2/stream/recentchange";

            EventHandler eventHandler = new WikiMediaEventHandler(producer, topic);
            Headers headers = new Headers.Builder().add("User-Agent","wikimedia-kafka-producer (test@gmail.com)").build();

            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url)).headers(headers);

            try (EventSource eventSource = builder.build()) {
                eventSource.start();

                TimeUnit.MINUTES.sleep(5);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

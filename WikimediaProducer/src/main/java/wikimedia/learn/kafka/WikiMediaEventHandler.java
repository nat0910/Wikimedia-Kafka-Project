package wikimedia.learn.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikiMediaEventHandler.class);
    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private  final Logger logger = LoggerFactory.getLogger(WikiMediaEventHandler.class.getName());

    public WikiMediaEventHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        log.info("Stream opened");
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("WikiMediaEventHandler onError", throwable);
    }
}

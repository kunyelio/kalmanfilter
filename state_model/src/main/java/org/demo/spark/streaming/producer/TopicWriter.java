package org.demo.spark.streaming.producer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

// See http://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html
public class TopicWriter {
    private static Producer<Long, String> producer = null;
    private static String TOPIC_NAME = null;
    private static String SERVER_NAME = null;

    public static void init(String bootstrapServer, String topic) {
        SERVER_NAME = bootstrapServer;
        TOPIC_NAME = topic;
    }

    private static Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SERVER_NAME);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "TopicWriter");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    public static void write(String message){
        if(producer == null){
            producer = getProducer();
        }
        long time = System.currentTimeMillis();
        final ProducerRecord<Long, String> record =
                new ProducerRecord<>(TOPIC_NAME, time,  message);
        producer.send(record);
        producer.flush();
    }

    public static void close(){
        if(producer == null){
            producer.close();
        }
    }
}

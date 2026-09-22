package ru.otus.kafka.hw7.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Producer для генерации сообщений в топик.
 * 
 * Реализует AutoCloseable, чтобы использовать try-with-resources.
 */
public class TopicProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public TopicProducer(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessages(int count) {
        for (int i = 1; i <= count; i++) {
            String key = "key-" + (i % 3);
            String value = "{\"messageId\":" + i + ",\"key\":\"" + key + "\"}";

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("📤 Sent: key=%s partition=%d offset=%d%n",
                        key, metadata.partition(), metadata.offset());
                } else {
                    System.err.printf("❌ Error: %s%n", exception.getMessage());
                }
            });
        }
        producer.flush();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}

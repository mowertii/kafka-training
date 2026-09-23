package ru.otus.kafka.hw9.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Producer для событий OrderCreated.
 */
public class OrderProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public OrderProducer(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Отправляет N событий OrderCreated.
     */
    public void sendOrders(int count) {
        int[] userIds = {42, 43, 44};

        for (int i = 1; i <= count; i++) {
            // 👇 final-копии для lambda
            final int orderId = i;
            final int userId = userIds[i % 3];
            final int amount = 100 * i;

            final String key = "order-" + orderId;
            final String value = String.format(
                "{\"orderId\":%d,\"userId\":%d,\"amount\":%d}",
                orderId, userId, amount);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("📤 Sent: orderId=%d userId=%d amount=%d partition=%d offset=%d%n",
                        orderId, userId, amount, metadata.partition(), metadata.offset());
                } else {
                    System.err.printf("❌ Error: orderId=%d error=%s%n", orderId, exception.getMessage());
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

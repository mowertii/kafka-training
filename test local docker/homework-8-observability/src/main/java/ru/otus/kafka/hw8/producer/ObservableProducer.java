package ru.otus.kafka.hw8.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

/**
 * Producer с correlationId в headers.
 */
public class ObservableProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public ObservableProducer(String bootstrapServers, String topic) {
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
     * Отправляет N сообщений с correlationId.
     */
    public void sendMessages(int count, boolean showLogs) {
        for (int i = 1; i <= count; i++) {
            // 👇 Создаём final-копии для lambda
            final int messageId = i;
            final String correlationId = UUID.randomUUID().toString();
            final String key = "key-" + (i % 3);
            final String value = String.format(
                "{\"messageId\":%d,\"correlationId\":\"%s\",\"payload\":\"data-%d\"}",
                messageId, correlationId, messageId);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // 👇 Добавляем correlationId в Kafka headers
            record.headers().add(new RecordHeader(
                "correlationId",
                correlationId.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(
                "messageId",
                String.valueOf(messageId).getBytes(StandardCharsets.UTF_8)));

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    if (showLogs) {
                        System.out.printf("📤 Sent: msgId=%d correlationId=%s partition=%d offset=%d%n",
                            messageId, correlationId, metadata.partition(), metadata.offset());
                    }
                } else {
                    System.err.printf("❌ Error: msgId=%d correlationId=%s error=%s%n",
                        messageId, correlationId, exception.getMessage());
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

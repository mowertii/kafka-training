package ru.otus.kafka.project.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.otus.kafka.project.config.AppConfig;
import ru.otus.kafka.project.observability.CorrelationId;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DLT Consumer — читает сообщения из Dead Letter Topic.
 */
public class DltConsumer implements Runnable {

    private static final ObjectMapper JSON = new ObjectMapper()
        .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    private final String bootstrapServers;
    private final DataSource dataSource;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private KafkaConsumer<String, String> consumer;

    public DltConsumer(String bootstrapServers, DataSource dataSource) {
        this.bootstrapServers = bootstrapServers;
        this.dataSource = dataSource;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.DLT_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(List.of(AppConfig.DLT_TOPIC));
            System.out.printf("💀 [%s] DLT Consumer запущен. Topic=%s%n",
                AppConfig.DLT_GROUP, AppConfig.DLT_TOPIC);

            while (running.get()) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    processDltRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {
            System.out.println("⏹️  DLT Consumer shutdown");
        } catch (Exception e) {
            System.err.println("❌ DLT Consumer error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void processDltRecord(ConsumerRecord<String, String> record) {
        String correlationId = CorrelationId.fromHeaders(record.headers());
        String errorMessage = extractHeader(record.headers(), "x-error", "unknown");
        String originalTopic = extractHeader(record.headers(), "x-original-topic", "unknown");
        int retryCount = extractRetryCount(record.headers());

        CorrelationId.log(correlationId,
            "💀 [DLT] Получено: key=" + record.key() +
            " originalTopic=" + originalTopic +
            " retryCount=" + retryCount +
            " error=" + errorMessage);

        try (Connection conn = dataSource.getConnection()) {
            String sql = """
                INSERT INTO dlt_messages(event_id, original_topic, error_message, payload, retry_count)
                VALUES (?::uuid, ?, ?, ?, ?)
                """;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, extractEventId(record.value()));
                ps.setString(2, originalTopic);
                ps.setString(3, errorMessage);
                ps.setString(4, record.value());
                ps.setInt(5, retryCount);
                ps.executeUpdate();
            }

            CorrelationId.log(correlationId,
                "💀 [DLT] Сохранено в БД для анализа. Требуется ручной разбор!");

        } catch (Exception e) {
            System.err.println("❌ [DLT] Ошибка сохранения: " + e.getMessage());
        }
    }

    private String extractHeader(Iterable<Header> headers, String key, String defaultValue) {
        for (Header header : headers) {
            if (key.equals(header.key())) {
                return new String(header.value());
            }
        }
        return defaultValue;
    }

    private int extractRetryCount(Iterable<Header> headers) {
        String value = extractHeader(headers, "x-retry-count", "0");
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String extractEventId(String payload) {
        try {
            com.fasterxml.jackson.databind.JsonNode node = JSON.readTree(payload);
            return node.path("eventId").asText(java.util.UUID.randomUUID().toString());
        } catch (Exception e) {
            return java.util.UUID.randomUUID().toString();
        }
    }

    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}

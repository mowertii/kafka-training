package ru.otus.kafka.project.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.otus.kafka.project.config.AppConfig;
import ru.otus.kafka.project.metrics.Metrics;
import ru.otus.kafka.project.observability.CorrelationId;
import ru.otus.kafka.project.retry.RetryManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Payment Consumer с:
 * - Идемпотентностью (Inbox pattern)
 * - Retry/DLT при ошибках
 * - Correlation ID в логах
 * - Метриками
 * 
 * 🎯 КЛЮЧЕВЫЕ ПАТТЕРНЫ:
 * 
 * 1. Idempotent Consumer (Inbox):
 *    - Проверяем eventId в inbox
 *    - Если уже обработан → SKIP
 *    - Если нет → бизнес-логика + INSERT inbox (ОДНА ТРАНЗАКЦИЯ!)
 * 
 * 2. Retry/DLT:
 *    - При ошибке → retry.1
 *    - При ошибке в retry.1 → retry.2
 *    - При ошибке в retry.2 → DLT
 * 
 * 3. Manual Commit:
 *    - Commit offset ТОЛЬКО после успешной обработки
 *    - При ошибке offset НЕ коммитим
 */
public class PaymentConsumer implements Runnable {

    private static final ObjectMapper JSON = new ObjectMapper()
        .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    private final String bootstrapServers;
    private final String sourceTopic;
    private final DataSource dataSource;
    private final RetryManager retryManager;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private KafkaConsumer<String, String> consumer;

    public PaymentConsumer(String bootstrapServers, String sourceTopic,
                            DataSource dataSource, RetryManager retryManager) {
        this.bootstrapServers = bootstrapServers;
        this.sourceTopic = sourceTopic;
        this.dataSource = dataSource;
        this.retryManager = retryManager;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.PAYMENT_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 👈 Ручной commit

        consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(List.of(sourceTopic));
            System.out.printf("🚀 [%s] Consumer запущен. Topic=%s%n",
                AppConfig.PAYMENT_GROUP, sourceTopic);

            while (running.get()) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (WakeupException e) {
            System.out.println("⏹️  Получен сигнал shutdown");
        } catch (Exception e) {
            System.err.println("❌ Consumer error: " + e.getMessage());
        } finally {
            try {
                consumer.close();
                System.out.println("✅ Consumer закрыт корректно");
            } catch (Exception e) {
                System.err.println("❌ Ошибка закрытия: " + e.getMessage());
            }
        }
    }

    /**
     * Обработка одной записи.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        long startTime = System.currentTimeMillis();

        // 👇 Извлекаем correlationId и retry count из headers
        String correlationId = CorrelationId.fromHeaders(record.headers());
        int retryCount = extractRetryCount(record.headers());

        try {
            JsonNode event = JSON.readTree(record.value());
            String eventId = event.path("eventId").asText();
            String eventType = event.path("eventType").asText();
            String orderId = event.path("aggregateId").asText();
            JsonNode payload = event.path("payload");
            String userId = payload.path("userId").asText();
            int amount = payload.path("amount").asInt();

            CorrelationId.log(correlationId,
                "📩 [CONSUMER] Получено: eventId=" + eventId +
                " eventType=" + eventType + " orderId=" + orderId +
                " retryCount=" + retryCount);

            // ============================================================
            // 1. IDEMPOTENT CONSUMER: проверка inbox
            // ============================================================
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);

                try {
                    // Проверяем, не обрабатывали ли уже это событие
                    if (isAlreadyProcessed(conn, eventId)) {
                        CorrelationId.log(correlationId,
                            "⚠️  DUPLICATE: eventId=" + eventId + " — уже обработано, пропускаем");
                        Metrics.recordEventDuplicated();
                        conn.commit();
                        return;
                    }

                    // ============================================================
                    // 2. Имитация ошибки для orderId, содержащего "fail"
                    // ============================================================
                    if (userId.contains("fail")) {
                        throw new RuntimeException("Имитация ошибки: userId=" + userId);
                    }

                    // ============================================================
                    // 3. Бизнес-логика: создаём платёж
                    // ============================================================
                    savePayment(conn, orderId, userId, amount);
                    CorrelationId.log(correlationId,
                        "💰 [CONSUMER] Платёж создан: orderId=" + orderId +
                        " userId=" + userId + " amount=" + amount);

                    // ============================================================
                    // 4. Записываем eventId в inbox (ТА ЖЕ ТРАНЗАКЦИЯ!)
                    // ============================================================
                    saveInbox(conn, eventId, AppConfig.PAYMENT_GROUP);
                    CorrelationId.log(correlationId,
                        "✅ [CONSUMER] Inbox записан: eventId=" + eventId);

                    conn.commit();

                    long processingTime = System.currentTimeMillis() - startTime;
                    Metrics.recordEventProcessed(processingTime);

                    CorrelationId.log(correlationId,
                        "✅ PROCESSED за " + processingTime + "ms");

                } catch (Exception e) {
                    conn.rollback();
                    throw e; // Пробрасываем наверх для retry
                }
            }

        } catch (Exception e) {
            // ============================================================
            // 5. ОШИБКА → RETRY/DLT
            // ============================================================
            Metrics.recordError();
            CorrelationId.log(correlationId,
                "❌ [CONSUMER] Ошибка: " + e.getMessage());

            try {
                // Backoff перед retry
                retryManager.backoff(retryCount);

                // Отправляем в retry или DLT
                retryManager.sendToRetry(
                    record.key(),
                    record.value(),
                    correlationId,
                    sourceTopic,
                    retryCount,
                    e.getMessage()
                );

                if (sourceTopic.equals(AppConfig.ORDERS_TOPIC) ||
                    sourceTopic.equals(AppConfig.RETRY_1_TOPIC)) {
                    Metrics.recordEventRetried();
                } else {
                    Metrics.recordEventDlt();
                }

            } catch (Exception retryError) {
                CorrelationId.log(correlationId,
                    "❌ Ошибка retry: " + retryError.getMessage());
            }
        }
    }

    /**
     * Проверка inbox (идемпотентность).
     */
    private boolean isAlreadyProcessed(Connection conn, String eventId) throws Exception {
        String sql = "SELECT 1 FROM inbox WHERE event_id = ?::uuid";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * INSERT INTO payments.
     */
    private void savePayment(Connection conn, String orderId,
                              String userId, int amount) throws Exception {
        String sql = """
            INSERT INTO payments(order_id, user_id, amount, status)
            VALUES (?, ?, ?, 'PAID')
            ON CONFLICT (order_id) DO NOTHING
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, orderId);
            ps.setString(2, userId);
            ps.setInt(3, amount);
            ps.executeUpdate();
        }
    }

    /**
     * INSERT INTO inbox (идемпотентность).
     */
    private void saveInbox(Connection conn, String eventId,
                            String consumerGroup) throws Exception {
        String sql = "INSERT INTO inbox(event_id, consumer_group) VALUES (?::uuid, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, eventId);
            ps.setString(2, consumerGroup);
            ps.executeUpdate();
        }
    }

    /**
     * Извлекает retry count из headers.
     */
    private int extractRetryCount(Iterable<Header> headers) {
        for (Header header : headers) {
            if ("x-retry-count".equals(header.key())) {
                try {
                    return Integer.parseInt(new String(header.value()));
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return 0;
    }

    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}

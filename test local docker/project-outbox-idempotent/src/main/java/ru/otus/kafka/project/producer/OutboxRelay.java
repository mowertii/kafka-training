package ru.otus.kafka.project.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.otus.kafka.project.config.AppConfig;
import ru.otus.kafka.project.metrics.Metrics;
import ru.otus.kafka.project.observability.CorrelationId;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Outbox Relay — читает события из outbox и публикует в Kafka.
 * 
 * 🎯 КЛЮЧЕВОЙ ПАТТЕРН: двухфазная публикация
 * 
 * Фаза 1 (короткая транзакция):
 *   - SELECT ... WHERE status='pending' LIMIT 100 FOR UPDATE SKIP LOCKED
 *   - UPDATE status='processing'
 *   - COMMIT
 * 
 * Фаза 2 (вне транзакции):
 *   - Отправка в Kafka
 * 
 * Фаза 3 (короткая транзакция):
 *   - UPDATE status='published' WHERE id IN (sent)
 *   - UPDATE status='pending' WHERE id IN (failed)
 *   - COMMIT
 * 
 * Почему не одна длинная транзакция?
 *   Медленный брокер (100ms × 1000 событий = 100 секунд)
 *   → долгие блокировки FOR UPDATE
 *   → bloat, рост WAL
 *   → параллельные relay'и ждут
 */
public class OutboxRelay implements Runnable {

    private final DataSource dataSource;
    private final KafkaProducer<String, String> producer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public OutboxRelay(DataSource dataSource, String bootstrapServers) {
        this.dataSource = dataSource;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        System.out.println("🚀 [RELAY] Outbox Relay запущен");

        while (running.get()) {
            try {
                int published = relayBatch();
                if (published == 0) {
                    Thread.sleep(AppConfig.OUTBOX_POLL_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("❌ [RELAY] Ошибка: " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ie) { break; }
            }
        }

        producer.close();
        System.out.println("🛑 [RELAY] Outbox Relay остановлен");
    }

    /**
     * Обрабатывает один батч outbox-событий.
     */
    private int relayBatch() throws Exception {
        // ============================================================
        // ФАЗА 1: Захватить батч и пометить 'processing'
        // ============================================================
        List<OutboxRecord> batch = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // SELECT ... FOR UPDATE SKIP LOCKED
            String selectSql = """
                SELECT id, aggregate_id, event_type, payload
                FROM outbox
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;

            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setInt(1, AppConfig.OUTBOX_BATCH_SIZE);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        batch.add(new OutboxRecord(
                            rs.getString("id"),
                            rs.getString("aggregate_id"),
                            rs.getString("event_type"),
                            rs.getString("payload")
                        ));
                    }
                }
            }

            if (batch.isEmpty()) {
                conn.commit();
                return 0;
            }

            // UPDATE status='processing'
            String updateSql = "UPDATE outbox SET status = 'processing' WHERE id = ?::uuid";
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                for (OutboxRecord record : batch) {
                    ps.setString(1, record.id());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            conn.commit();
        }

        // ============================================================
        // ФАЗА 2: Отправка в Kafka (ВНЕ транзакции БД!)
        // ============================================================
        List<String> sentIds = new ArrayList<>();
        List<String> failedIds = new ArrayList<>();

        for (OutboxRecord record : batch) {
            try {
                publishToKafka(record);
                sentIds.add(record.id());
            } catch (Exception e) {
                System.err.printf("❌ [RELAY] Ошибка отправки id=%s: %s%n",
                    record.id(), e.getMessage());
                failedIds.add(record.id());
            }
        }

        // ============================================================
        // ФАЗА 3: Обновить статусы
        // ============================================================
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Успешные → 'published'
            if (!sentIds.isEmpty()) {
                updateStatus(conn, sentIds, "published");
            }

            // Ошибочные → обратно в 'pending'
            if (!failedIds.isEmpty()) {
                updateStatus(conn, failedIds, "pending");
                incrementRetryCount(conn, failedIds);
            }

            conn.commit();
        }

        if (!sentIds.isEmpty()) {
            System.out.printf("📤 [RELAY] Батч завершён: отправлено=%d, ошибок=%d%n",
                sentIds.size(), failedIds.size());
        }

        return sentIds.size();
    }

    /**
     * Публикация события в Kafka.
     */
    private void publishToKafka(OutboxRecord record) throws Exception {
        // Извлекаем correlationId из payload
        String correlationId = extractCorrelationId(record.payload());

        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
            AppConfig.ORDERS_TOPIC,
            record.aggregateId(),  // key = aggregateId (для партиционирования)
            record.payload()
        );

        // 👇 Добавляем correlationId в headers
        kafkaRecord.headers().add(CorrelationId.toHeader(correlationId));
        kafkaRecord.headers().add("eventType",
            record.eventType().getBytes());
        kafkaRecord.headers().add("outboxId",
            record.id().getBytes());

        // Синхронная отправка
        producer.send(kafkaRecord).get();

        CorrelationId.log(correlationId,
            "📤 [KAFKA] Опубликовано: id=" + record.id() +
            " eventType=" + record.eventType());

        Metrics.recordOutboxPublished();
    }

    /**
     * Обновление статуса батча.
     */
    private void updateStatus(Connection conn, List<String> ids, String status) throws Exception {
        String sql = "UPDATE outbox SET status = ?, published_at = CASE WHEN ? = 'published' THEN NOW() ELSE published_at END WHERE id = ?::uuid";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String id : ids) {
                ps.setString(1, status);
                ps.setString(2, status);
                ps.setString(3, id);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * Увеличение retry_count для ошибочных.
     */
    private void incrementRetryCount(Connection conn, List<String> ids) throws Exception {
        String sql = "UPDATE outbox SET retry_count = retry_count + 1 WHERE id = ?::uuid";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String id : ids) {
                ps.setString(1, id);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /**
     * Извлекает correlationId из JSON payload.
     */
    private String extractCorrelationId(String payload) {
        try {
            com.fasterxml.jackson.databind.JsonNode node =
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload);
            return node.path("correlationId").asText("unknown");
        } catch (Exception e) {
            return "unknown";
        }
    }

    public void shutdown() {
        running.set(false);
    }

    /**
     * Запись из outbox.
     */
    private record OutboxRecord(String id, String aggregateId,
                                  String eventType, String payload) {}
}

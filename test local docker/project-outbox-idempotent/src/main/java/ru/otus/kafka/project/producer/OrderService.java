package ru.otus.kafka.project.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.otus.kafka.project.metrics.Metrics;
import ru.otus.kafka.project.model.Event;
import ru.otus.kafka.project.model.Order;
import ru.otus.kafka.project.observability.CorrelationId;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Бизнес-сервис для создания заказов.
 * 
 * 🎯 КЛЮЧЕВОЙ ПАТТЕРН: Transactional Outbox
 * 
 * Проблема dual-write:
 *   save(order) → OK
 *   producer.send(event) → FAIL
 *   → Заказ создан, но событие потеряно!
 * 
 * Решение:
 *   1. В ОДНОЙ транзакции БД:
 *      - INSERT INTO orders
 *      - INSERT INTO outbox (status='pending')
 *   2. Отдельный OutboxRelay читает outbox и отправляет в Kafka
 *   3. После успешной отправки → UPDATE status='published'
 * 
 * Гарантия: если транзакция закоммичена, событие ОБЯЗАТЕЛЬНО будет отправлено
 * (даже если Kafka временно недоступна).
 */
public class OrderService {

    private static final ObjectMapper JSON = new ObjectMapper()
        .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final DataSource dataSource;

    public OrderService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Создаёт заказ и сохраняет событие в outbox.
     * 
     * ВСЁ В ОДНОЙ ТРАНЗАКЦИИ:
     * 1. INSERT INTO orders
     * 2. INSERT INTO outbox
     * 
     * Если что-то падает — откатывается ВСЁ.
     * 
     * @return ID созданного заказа
     */
    public String createOrder(String userId, int amount) throws SQLException {
        String correlationId = CorrelationId.generate();
        String orderId = "order-" + UUID.randomUUID().toString().substring(0, 8);

        Order order = Order.newOrder(orderId, userId, amount);

        CorrelationId.log(correlationId, "📝 Создание заказа: " + orderId);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false); // 👈 НАЧИНАЕМ ТРАНЗАКЦИЮ

            try {
                // ============================================================
                // 1. Сохраняем заказ (бизнес-данные)
                // ============================================================
                saveOrder(conn, order);
                CorrelationId.log(correlationId,
                    "✅ [БД] orders: id=" + orderId + " userId=" + userId + " amount=" + amount);

                // ============================================================
                // 2. Сохраняем событие в outbox (ТА ЖЕ ТРАНЗАКЦИЯ!)
                // ============================================================
                Event event = createOrderCreatedEvent(order, correlationId);
                saveOutboxEvent(conn, event);
                CorrelationId.log(correlationId,
                    "✅ [БД] outbox: eventId=" + event.eventId() +
                    " eventType=" + event.eventType() + " status=pending");

                // ============================================================
                // 3. КОММИТИМ ТРАНЗАКЦИЮ
                // ============================================================
                conn.commit();
                CorrelationId.log(correlationId,
                    "🎯 [БД] Транзакция закоммичена: order + outbox сохранены АТОМАРНО");

                Metrics.recordOrderCreated();
                return orderId;

            } catch (Exception e) {
                conn.rollback();
                CorrelationId.log(correlationId, "❌ [БД] Откат транзакции: " + e.getMessage());
                throw new SQLException("Failed to create order", e);
            }
        }
    }

    /**
     * INSERT INTO orders
     */
    private void saveOrder(Connection conn, Order order) throws SQLException {
        String sql = "INSERT INTO orders(id, user_id, amount, status) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, order.id());
            ps.setString(2, order.userId());
            ps.setInt(3, order.amount());
            ps.setString(4, order.status());
            ps.executeUpdate();
        }
    }

    /**
     * INSERT INTO outbox
     */
    private void saveOutboxEvent(Connection conn, Event event) throws SQLException {
        String sql = """
            INSERT INTO outbox(id, aggregate_id, event_type, payload, status)
            VALUES (?::uuid, ?, ?, ?, 'pending')
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, event.eventId().toString());
            ps.setString(2, event.aggregateId());
            ps.setString(3, event.eventType());
            ps.setString(4, JSON.writeValueAsString(event));
            ps.executeUpdate();
        } catch (Exception e) {
            throw new SQLException("Failed to serialize event", e);
        }
    }

    /**
     * Создаёт событие OrderCreated.
     */
    private Event createOrderCreatedEvent(Order order, String correlationId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("orderId", order.id());
        payload.put("userId", order.userId());
        payload.put("amount", order.amount());
        payload.put("status", order.status());

        return Event.create("OrderCreated", order.id(), correlationId, payload);
    }
}

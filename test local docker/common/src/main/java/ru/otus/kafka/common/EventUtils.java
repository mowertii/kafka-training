package ru.otus.kafka.common;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Утилиты для создания событий в формате envelope.
 */
public final class EventUtils {

    private EventUtils() {
        // utility class
    }

    /**
     * Создаёт событие с автоматически сгенерированным eventId.
     */
    public static String event(String eventType, String aggregateId, Map<String, Object> payload) {
        return eventWithId(UUID.randomUUID().toString(), eventType, aggregateId, payload);
    }

    /**
     * Создаёт событие с указанным eventId (для тестирования дублей).
     */
    public static String eventWithId(String eventId, String eventType,
                                      String aggregateId, Map<String, Object> payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", eventId);
        envelope.put("eventType", eventType);
        envelope.put("eventVersion", 1);
        envelope.put("occurredAt", Instant.now().toString());
        envelope.put("producer", "kafka-training");
        envelope.put("correlationId", UUID.randomUUID().toString());
        envelope.put("aggregateId", aggregateId);
        envelope.put("payload", payload);
        return JsonUtils.toJson(envelope);
    }
}
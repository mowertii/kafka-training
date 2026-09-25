package ru.otus.kafka.project.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Событие в формате envelope.
 * 
 * Все события имеют:
 * - eventId — уникальный ID (для идемпотентности)
 * - eventType — тип события
 * - correlationId — для трассировки
 * - payload — бизнес-данные
 */
public record Event(
    UUID eventId,
    String eventType,
    String correlationId,
    String aggregateId,
    Map<String, Object> payload,
    Instant occurredAt
) {
    public static Event create(String eventType, String aggregateId,
                                String correlationId, Map<String, Object> payload) {
        return new Event(UUID.randomUUID(), eventType, correlationId,
                         aggregateId, payload, Instant.now());
    }
}

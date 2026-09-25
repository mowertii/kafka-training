package ru.otus.kafka.project.observability;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Утилита для работы с correlationId.
 * 
 * CorrelationId передаётся через Kafka headers и добавляется во все логи.
 */
public final class CorrelationId {

    public static final String HEADER_NAME = "correlationId";

    private CorrelationId() {}

    /**
     * Генерирует новый correlationId.
     */
    public static String generate() {
        return UUID.randomUUID().toString();
    }

    /**
     * Создаёт Kafka header с correlationId.
     */
    public static Header toHeader(String correlationId) {
        return new RecordHeader(HEADER_NAME, correlationId.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Извлекает correlationId из headers.
     */
    public static String fromHeaders(Iterable<Header> headers) {
        for (Header header : headers) {
            if (HEADER_NAME.equals(header.key())) {
                return new String(header.value(), StandardCharsets.UTF_8);
            }
        }
        return "unknown";
    }

    /**
     * Логирование с correlationId.
     */
    public static void log(String correlationId, String message) {
        System.out.printf("[correlationId=%s] %s%n", correlationId, message);
    }
}

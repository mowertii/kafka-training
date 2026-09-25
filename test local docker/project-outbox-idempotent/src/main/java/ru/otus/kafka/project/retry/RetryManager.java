package ru.otus.kafka.project.retry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.otus.kafka.project.config.AppConfig;
import ru.otus.kafka.project.observability.CorrelationId;

import java.time.Instant;
import java.util.Properties;

/**
 * Менеджер Retry/DLT.
 * 
 * 🎯 КЛЮЧЕВОЙ ПАТТЕРН: Retry Topics + DLT
 * 
 * Логика:
 *   1. При ошибке обработки → отправляем в retry.1
 *   2. При ошибке в retry.1 → отправляем в retry.2
 *   3. При ошибке в retry.2 → отправляем в DLT
 * 
 * Каждое сообщение получает headers:
 *   - x-retry-count: номер попытки
 *   - x-original-topic: исходный топик
 *   - x-error: причина ошибки
 *   - x-error-time: время ошибки
 */
public class RetryManager {

    private final KafkaProducer<String, String> producer;

    public RetryManager(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Определяет следующий топик для retry.
     */
    public String getNextTopic(String currentTopic, int currentRetryCount) {
        if (currentTopic.equals(AppConfig.ORDERS_TOPIC)) {
            return AppConfig.RETRY_1_TOPIC;
        } else if (currentTopic.equals(AppConfig.RETRY_1_TOPIC)) {
            return AppConfig.RETRY_2_TOPIC;
        } else if (currentTopic.equals(AppConfig.RETRY_2_TOPIC)) {
            return AppConfig.DLT_TOPIC;
        }
        return AppConfig.DLT_TOPIC;
    }

    /**
     * Отправляет сообщение в следующий топик (retry или DLT).
     */
    public void sendToRetry(String key, String value, String correlationId,
                             String currentTopic, int currentRetryCount,
                             String errorMessage) throws Exception {

        String targetTopic = getNextTopic(currentTopic, currentRetryCount);
        int newRetryCount = currentRetryCount + 1;

        ProducerRecord<String, String> record = new ProducerRecord<>(targetTopic, key, value);

        // 👇 Добавляем headers с информацией о retry
        record.headers().add(CorrelationId.toHeader(correlationId));
        record.headers().add("x-retry-count",
            String.valueOf(newRetryCount).getBytes());
        record.headers().add("x-original-topic",
            currentTopic.getBytes());
        record.headers().add("x-error",
            errorMessage.getBytes());
        record.headers().add("x-error-time",
            Instant.now().toString().getBytes());

        producer.send(record).get();

        String emoji = targetTopic.equals(AppConfig.DLT_TOPIC) ? "💀" : "🔄";
        CorrelationId.log(correlationId,
            emoji + " RETRY #" + newRetryCount + " → " + targetTopic +
            " (причина: " + errorMessage + ")");
    }

    /**
     * Backoff перед retry.
     */
    public void backoff(int retryCount) throws InterruptedException {
        long delay = AppConfig.BACKOFF_MS * retryCount;
        if (delay > 0) {
            Thread.sleep(delay);
        }
    }

    public void close() {
        producer.close();
    }
}

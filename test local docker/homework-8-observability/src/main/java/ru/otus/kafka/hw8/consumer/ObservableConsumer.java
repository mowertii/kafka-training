package ru.otus.kafka.hw8.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.otus.kafka.hw8.metrics.ConsumerMetrics;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer с:
 * - Чтением correlationId из headers
 * - Метриками (processed, errors)
 * - Диагностикой lag
 * - Graceful shutdown
 */
public class ObservableConsumer implements Runnable {

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final String consumerName;
    private final ConsumerMetrics metrics;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private volatile long processingDelayMs = 0;
    private KafkaConsumer<String, String> consumer;

    public ObservableConsumer(String bootstrapServers, String groupId, String topic,
                              String consumerName, ConsumerMetrics metrics) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.consumerName = consumerName;
        this.metrics = metrics;
    }

    /**
     * Устанавливает задержку обработки (для создания lag).
     */
    public void setProcessingDelayMs(long delayMs) {
        this.processingDelayMs = delayMs;
        log("⏱️  Processing delay set to " + delayMs + " ms");
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log("⚠️  onPartitionsRevoked: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log("✅ onPartitionsAssigned: " + partitions);
                }
            });

            log("🚀 Consumer запущен: group=" + groupId + ", topic=" + topic);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                // Периодически логируем lag
                logLag();
            }
        } catch (WakeupException e) {
            log("⏹️  WakeupException — graceful shutdown");
        } catch (Exception e) {
            log("❌ Ошибка: " + e.getMessage());
        } finally {
            try {
                consumer.close();
                log("✅ Consumer закрыт корректно");
            } catch (Exception e) {
                log("❌ Ошибка при закрытии: " + e.getMessage());
            }
        }
    }

    /**
     * Обработка сообщения с correlationId и метриками.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        long startTime = System.currentTimeMillis();

        // 👇 Извлекаем correlationId из headers
        String correlationId = "unknown";
        Header header = record.headers().lastHeader("correlationId");
        if (header != null) {
            correlationId = new String(header.value(), StandardCharsets.UTF_8);
        }

        try {
            // 👇 Все логи содержат correlationId
            System.out.printf("[%s] 📩 correlationId=%s partition=%d offset=%d key=%s%n",
                consumerName, correlationId, record.partition(), record.offset(), record.key());

            // 👇 Искусственная задержка для создания lag
            if (processingDelayMs > 0) {
                Thread.sleep(processingDelayMs);
            }

            // Имитация обработки
            System.out.printf("[%s] ✅ PROCESSED correlationId=%s value=%s%n",
                consumerName, correlationId, record.value());

            metrics.recordProcessed(System.currentTimeMillis() - startTime);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("❌ Interrupted: correlationId=" + correlationId);
        } catch (Exception e) {
            metrics.recordError();
            System.err.printf("[%s] ❌ ERROR correlationId=%s error=%s%n",
                consumerName, correlationId, e.getMessage());
        }
    }

    /**
     * Логирует текущий lag по всем партициям.
     */
    private void logLag() {
        try {
            var assignment = consumer.assignment();
            if (assignment.isEmpty()) return;

            var endOffsets = consumer.endOffsets(assignment);
            long totalLag = 0;

            for (TopicPartition tp : assignment) {
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long currentOffset = consumer.position(tp);
                long lag = endOffset - currentOffset;
                totalLag += lag;
            }

            if (totalLag > 0) {
                System.out.printf("[%s] 📊 LAG = %d (processed=%d, errors=%d)%n",
                    consumerName, totalLag,
                    metrics.getProcessedMessages(), metrics.getErrorMessages());
            }
        } catch (Exception ignored) {
            // Игнорируем ошибки при логировании lag
        }
    }

    public void shutdown() {
        log("🛑 Получен сигнал shutdown");
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void log(String msg) {
        System.out.printf("[%s] %s%n", consumerName, msg);
    }
}

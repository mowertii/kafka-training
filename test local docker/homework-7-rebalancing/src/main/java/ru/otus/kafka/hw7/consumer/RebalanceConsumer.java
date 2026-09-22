package ru.otus.kafka.hw7.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer с логированием rebalance-событий.
 * 
 * Демонстрирует:
 * - Назначение partitions (onPartitionsAssigned)
 * - Отзыв partitions (onPartitionsRevoked)
 * - Cooperative assignor
 * - Static membership
 * - Graceful shutdown
 */
public class RebalanceConsumer implements Runnable {

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final String consumerName;
    private final boolean useCooperative;
    private final boolean useStaticMembership;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private KafkaConsumer<String, String> consumer;

    public RebalanceConsumer(String bootstrapServers, String groupId, String topic,
                             String consumerName, boolean useCooperative, boolean useStaticMembership) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.consumerName = consumerName;
        this.useCooperative = useCooperative;
        this.useStaticMembership = useStaticMembership;
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

        // ============================================================
        // 🎯 Cooperative assignor
        // ============================================================
        if (useCooperative) {
            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
            log("🔧 Используется CooperativeStickyAssignor");
        } else {
            props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RangeAssignor");
            log("🔧 Используется RangeAssignor (eager)");
        }

        // ============================================================
        // 🎯 Static membership
        // ============================================================
        if (useStaticMembership) {
            // group.instance.id должен быть уникальным для каждого consumer
            // и постоянным между перезапусками.
            String instanceId = consumerName; // например, "consumer-1"
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
            log("🔧 Static membership: group.instance.id=" + instanceId);
        } else {
            log("🔧 Dynamic membership (без group.instance.id)");
        }

        consumer = new KafkaConsumer<>(props);

        // ============================================================
        // 🎯 ConsumerRebalanceListener — логирование событий
        // ============================================================
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log("⚠️  onPartitionsRevoked: " + partitions);
                // Для cooperative assignor: отзываются только те партиции, которые нужно передать
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log("✅ onPartitionsAssigned: " + partitions);
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                log("💀 onPartitionsLost: " + partitions);
            }
        };

        try {
            consumer.subscribe(List.of(topic), listener);
            log("🚀 Consumer запущен: name=" + consumerName + ", group=" + groupId + ", topic=" + topic);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[%s] 📩 partition=%d offset=%d key=%s value=%s%n",
                        consumerName, record.partition(), record.offset(),
                        record.key(), record.value());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // Ожидаемое исключение при graceful shutdown
            log("⏹️  WakeupException — graceful shutdown");
        } catch (Exception e) {
            log("❌ Ошибка: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // ============================================================
            // 🎯 Graceful shutdown
            // ============================================================
            try {
                consumer.close(); // Закрываем consumer, отзываем partitions
                log("✅ Consumer закрыт корректно");
            } catch (Exception e) {
                log("❌ Ошибка при закрытии: " + e.getMessage());
            }
        }
    }

    /**
     * Graceful shutdown: прерывает poll() и позволяет consumer.close() выполниться.
     */
    public void shutdown() {
        log("🛑 Получен сигнал shutdown");
        running.set(false);
        if (consumer != null) {
            consumer.wakeup(); // Прерывает poll()
        }
    }

    private void log(String msg) {
        System.out.printf("[%s] %s%n", consumerName, msg);
    }
}

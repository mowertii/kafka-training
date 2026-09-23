package ru.otus.kafka.hw9.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams Topology:
 * 
 * 1. Читает OrderCreated из topic orders
 * 2. Парсит JSON → "userId:amount"
 * 3. groupBy(userId) → REPARTITION
 * 4. aggregate(sum) → STATE STORE "order-totals-store"
 * 5. Выводит в output topic
 */
public class OrderAggregationTopology {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void build(StreamsBuilder builder, String sourceTopic) {

        // 1. Читаем события
        KStream<String, String> orders = builder.stream(
            sourceTopic,
            Consumed.with(Serdes.String(), Serdes.String())
        );

        // 2. Парсим JSON → "userId:amount"
        KStream<String, String> userIdAmount = orders.mapValues(value -> {
            try {
                JsonNode node = JSON.readTree(value);
                int userId = node.path("userId").asInt();
                int amount = node.path("amount").asInt();
                System.out.printf("🔍 Parsed: userId=%d amount=%d%n", userId, amount);
                return userId + ":" + amount;
            } catch (Exception e) {
                System.err.println("❌ Parse error: " + e.getMessage());
                return "0:0";
            }
        });

        // 3. groupBy(userId) → REPARTITION
        KGroupedStream<String, String> grouped = userIdAmount.groupBy(
            (key, value) -> value.split(":")[0],
            Grouped.with(Serdes.String(), Serdes.String())
        );

        // 4. Агрегация с ЯВНЫМИ типами
        KTable<String, String> totals = grouped.aggregate(
            // Initializer
            () -> "0",
            // Aggregator
            (userId, value, currentTotal) -> {
                int amount = Integer.parseInt(value.split(":")[1]);
                int total = Integer.parseInt(currentTotal) + amount;
                System.out.printf("💰 Aggregated: userId=%s amount=%d total=%d%n",
                    userId, amount, total);
                return String.valueOf(total);
            },
            // 👇 ЯВНО указываем типы: Materialized<String, String, KeyValueStore<Bytes, byte[]>>
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("order-totals-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        // 5. Выводим в output topic
        totals.toStream()
            .mapValues(total -> {
                System.out.printf("📊 TOTAL: %s%n", total);
                return total;
            })
            .to(
                "order-totals-output",
                Produced.with(Serdes.String(), Serdes.String())
            );
    }
}

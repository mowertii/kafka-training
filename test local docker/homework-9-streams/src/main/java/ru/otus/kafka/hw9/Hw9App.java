package ru.otus.kafka.hw9;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import ru.otus.kafka.common.EnvUtils;
import ru.otus.kafka.common.LogUtils;
import ru.otus.kafka.hw9.producer.OrderProducer;
import ru.otus.kafka.hw9.streams.OrderAggregationTopology;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * ДЗ №9: Kafka Streams — state store, repartition, restore, exactly-once-v2.
 *
 * Режимы:
 *   - init        — создание топиков orders и order-totals-output
 *   - produce     — отправка N событий OrderCreated
 *   - streams     — запуск Kafka Streams приложения
 *   - demo        — полная демонстрация (init + produce + streams)
 */
public class Hw9App {

    private static final String BOOTSTRAP = EnvUtils.getBootstrapServers();
    private static final String ORDERS_TOPIC = "orders";
    private static final String OUTPUT_TOPIC = "order-totals-output";
    private static final String APP_ID = "hw9-streams-app";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        LogUtils.banner("HW9: mode=" + mode);

        switch (mode) {
            case "init" -> initTopics();
            case "produce" -> produceOrders(20);
            case "produce-more" -> produceOrders(10);
            case "streams" -> runStreams();
            case "demo" -> runDemo();
            default -> printUsage();
        }
    }

    private static void initTopics() throws Exception {
        LogUtils.info("Создание топиков: " + ORDERS_TOPIC + ", " + OUTPUT_TOPIC);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

        try (AdminClient admin = AdminClient.create(props)) {
            for (String topic : List.of(ORDERS_TOPIC, OUTPUT_TOPIC)) {
                if (admin.listTopics().names().get().contains(topic)) {
                    LogUtils.info("Топик " + topic + " уже существует.");
                } else {
                    admin.createTopics(Collections.singletonList(
                        new NewTopic(topic, 3, (short) 1))).all().get();
                    LogUtils.success("Топик " + topic + " создан (3 partitions)");
                }
            }
        }
    }

    private static void produceOrders(int count) {
        LogUtils.info("Отправка " + count + " событий OrderCreated...");
        try (OrderProducer producer = new OrderProducer(BOOTSTRAP, ORDERS_TOPIC)) {
            producer.sendOrders(count);
        }
        LogUtils.success(count + " событий отправлено");
    }

    private static void runStreams() throws Exception {
        LogUtils.info("Запуск Kafka Streams приложения...");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // ============================================================
        // 🎯 EXACTLY-ONCE-V2 (требование ДЗ)
        // ============================================================
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // State store directory
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        StreamsBuilder builder = new StreamsBuilder();
        OrderAggregationTopology.build(builder, ORDERS_TOPIC);

        Topology topology = builder.build();
        System.out.println("=== TOPOLOGY ===");
        System.out.println(topology.describe());
        System.out.println();

        KafkaStreams streams = new KafkaStreams(topology, props);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LogUtils.warning("Получен сигнал shutdown...");
            streams.close();
        }));

        streams.start();
        LogUtils.success("Streams запущен. Application ID: " + APP_ID);
        LogUtils.info("Ожидание 30 секунд...");

        Thread.sleep(30000);

        streams.close();
        LogUtils.success("Streams остановлен");
    }

    private static void runDemo() throws Exception {
        LogUtils.info("=== ПОЛНАЯ ДЕМОНСТРАЦИЯ ===");

        // 1. Создаём топики
        initTopics();

        // 2. Отправляем первые 20 событий
        LogUtils.info("Шаг 1: Отправляем 20 событий");
        produceOrders(20);

        // 3. Запускаем Streams
        LogUtils.info("Шаг 2: Запускаем Streams");
        Thread streamsThread = new Thread(() -> {
            try {
                runStreams();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        streamsThread.start();

        // 4. Ждём обработки
        Thread.sleep(15000);

        // 5. Отправляем ещё 10 событий (состояние должно сохраниться)
        LogUtils.info("Шаг 3: Отправляем ещё 10 событий (состояние сохранилось?)");
        produceOrders(10);

        // 6. Ждём
        streamsThread.join(30000);
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar hw9-streams.jar <mode>");
        System.out.println("Modes:");
        System.out.println("  init         - create topics (orders, order-totals-output)");
        System.out.println("  produce      - send 20 OrderCreated events");
        System.out.println("  produce-more - send 10 more events");
        System.out.println("  streams      - run Kafka Streams app");
        System.out.println("  demo         - full demo (init + produce + streams)");
    }
}

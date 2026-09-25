package ru.otus.kafka.hw7;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import ru.otus.kafka.common.EnvUtils;
import ru.otus.kafka.common.LogUtils;
import ru.otus.kafka.hw7.consumer.RebalanceConsumer;
import ru.otus.kafka.hw7.producer.TopicProducer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * ДЗ №7: Rebalancing, static membership, cooperative assignor, graceful shutdown.
 *
 * Режимы:
 *   - init              — создание топика
 *   - produce           — отправка сообщений
 *   - consumer-eager    — consumer с eager assignor (RangeAssignor)
 *   - consumer-cooperative — consumer с CooperativeStickyAssignor
 *   - consumer-static   — consumer со static membership
 *   - demo-eager        — 2 consumer + rebalance (eager)
 *   - demo-cooperative  — 2 consumer + rebalance (cooperative)
 *   - demo-static       — 2 consumer + static membership
 */
public class Hw7App {

    private static final String BOOTSTRAP = EnvUtils.getBootstrapServers();
    private static final String TOPIC = "hw7-rebalance-topic";
    private static final String GROUP = "hw7-rebalance-group";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        LogUtils.banner("HW7: mode=" + mode);

        switch (mode) {
            case "init" -> initTopic();
            case "produce" -> produceMessages();
            case "consumer-eager" -> runSingleConsumer(false, false, args);
            case "consumer-cooperative" -> runSingleConsumer(true, false, args);
            case "consumer-static" -> runSingleConsumer(true, true, args);
            case "demo-eager" -> demoRebalance(false, false);
            case "demo-cooperative" -> demoRebalance(true, false);
            case "demo-static" -> demoRebalance(true, true);
            default -> printUsage();
        }
    }

    // ============================================================
    // Создание топика
    // ============================================================
    private static void initTopic() throws Exception {
        LogUtils.info("Создание топика " + TOPIC + " с 3 партициями...");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

        try (AdminClient admin = AdminClient.create(props)) {
            if (admin.listTopics().names().get().contains(TOPIC)) {
                LogUtils.info("Топик уже существует. Удаляем и пересоздаём...");
                admin.deleteTopics(List.of(TOPIC)).all().get();
                Thread.sleep(2000);
            }

            NewTopic newTopic = new NewTopic(TOPIC, 3, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();

            LogUtils.success("Топик " + TOPIC + " создан (3 partitions, RF=1)");
        }
    }

    // ============================================================
    // Отправка сообщений
    // ============================================================
    private static void produceMessages() {
        LogUtils.info("Отправка 30 сообщений в " + TOPIC + "...");
        try (TopicProducer producer = new TopicProducer(BOOTSTRAP, TOPIC)) {
            producer.sendMessages(30);
        }
        LogUtils.success("Сообщения отправлены");
    }

    // ============================================================
    // Запуск одного consumer (для ручного теста)
    // ============================================================
    private static void runSingleConsumer(boolean cooperative, boolean staticMembership, String[] args) throws Exception {
        String consumerName = args.length > 1 ? args[1] : "consumer-1";

        RebalanceConsumer consumer = new RebalanceConsumer(
            BOOTSTRAP, GROUP, TOPIC, consumerName, cooperative, staticMembership);

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        consumer.run();
    }

    // ============================================================
    // Демонстрация rebalance: 2 consumer → остановка одного
    // ============================================================
    private static final ObjectMapper JSON = new ObjectMapper()
        .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS) 
        LogUtils.info("Демонстрация rebalance: 2 consumer, затем остановка одного");

        CountDownLatch latch = new CountDownLatch(2);

        RebalanceConsumer c1 = new RebalanceConsumer(BOOTSTRAP, GROUP, TOPIC, "consumer-1", cooperative, staticMembership);
        RebalanceConsumer c2 = new RebalanceConsumer(BOOTSTRAP, GROUP, TOPIC, "consumer-2", cooperative, staticMembership);

        Thread t1 = new Thread(() -> { c1.run(); latch.countDown(); });
        Thread t2 = new Thread(() -> { c2.run(); latch.countDown(); });

        t1.start();
        t2.start();

        // дать consumer'ам время вступить в группу
        LogUtils.info("Ждём 10 секунд, чтобы consumer'ы вступили в группу...");
        Thread.sleep(10000);

        // Отправляем сообщения
        LogUtils.info("Отправляем сообщения...");
        produceMessages();

        // Ждём 15 секунд
        Thread.sleep(15000);

        // Останавливаем consumer-1
        LogUtils.warning("Останавливаем consumer-1 → ожидаем rebalance");
        c1.shutdown();

        // Ждём ещё 15 секунд
        Thread.sleep(15000);

        // Останавливаем consumer-2
        LogUtils.warning("Останавливаем consumer-2 → graceful shutdown");
        c2.shutdown();

        latch.await();
        LogUtils.success("Демонстрация завершена");
    }
    private static void printUsage() {
        System.out.println("Usage: java -jar hw7-rebalancing.jar <mode> [consumer-name]");
        System.out.println("Modes:");
        System.out.println("  init                  - create topic with 3 partitions");
        System.out.println("  produce               - send 30 messages");
        System.out.println("  consumer-eager        - run single consumer (eager assignor)");
        System.out.println("  consumer-cooperative  - run single consumer (cooperative assignor)");
        System.out.println("  consumer-static       - run single consumer (static membership)");
        System.out.println("  demo-eager            - demo rebalance with eager assignor");
        System.out.println("  demo-cooperative      - demo rebalance with cooperative assignor");
        System.out.println("  demo-static           - demo rebalance with static membership");
    }
}

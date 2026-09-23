package ru.otus.kafka.hw8;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import ru.otus.kafka.common.EnvUtils;
import ru.otus.kafka.common.LogUtils;
import ru.otus.kafka.hw8.consumer.ObservableConsumer;
import ru.otus.kafka.hw8.metrics.ConsumerMetrics;
import ru.otus.kafka.hw8.producer.ObservableProducer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * ДЗ №8: Метрики, correlation ID, диагностика lag.
 *
 * Режимы:
 *   - init        — создание топика
 *   - produce     — отправка N сообщений с correlationId
 *   - consume     — чтение с метриками (без задержки)
 *   - lag-demo    — демонстрация lag (1000 сообщений + sleep)
 */
public class Hw8App {

    private static final String BOOTSTRAP = EnvUtils.getBootstrapServers();
    private static final String TOPIC = "hw8-observability-topic";
    private static final String GROUP = "hw8-observability-group";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        LogUtils.banner("HW8: mode=" + mode);

        switch (mode) {
            case "init" -> initTopic();
            case "produce" -> produceMessages(1000);
            case "produce-small" -> produceMessages(10);
            case "consume" -> consumeMessages(0);
            case "consume-slow" -> consumeMessages(50);
            case "lag-demo" -> lagDemo();
            default -> printUsage();
        }
    }

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

    private static void produceMessages(int count) {
        LogUtils.info("Отправка " + count + " сообщений с correlationId...");
        try (ObservableProducer producer = new ObservableProducer(BOOTSTRAP, TOPIC)) {
            producer.sendMessages(count, count <= 20);
        }
        LogUtils.success(count + " сообщений отправлено");
    }

    private static void consumeMessages(long delayMs) throws Exception {
        ConsumerMetrics metrics = new ConsumerMetrics();
        ObservableConsumer consumer = new ObservableConsumer(
            BOOTSTRAP, GROUP, TOPIC, "consumer-1", metrics);

        if (delayMs > 0) {
            consumer.setProcessingDelayMs(delayMs);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        Thread t = new Thread(consumer);
        t.start();
        t.join(30000);

        consumer.shutdown();
        metrics.print();
    }

    /**
     * Демонстрация consumer lag:
     * 1. Отправляем 1000 сообщений
     * 2. Запускаем consumer с задержкой 50ms → lag растёт
     * 3. Через 15 секунд снимаем задержку → lag уменьшается
     */
    private static void lagDemo() throws Exception {
        LogUtils.info("=== LAG DEMO ===");

        // 1. Отправляем 1000 сообщений
        LogUtils.info("Шаг 1: Отправляем 1000 сообщений...");
        produceMessages(1000);

        // 2. Запускаем consumer с задержкой 50ms
        LogUtils.info("Шаг 2: Запускаем consumer с задержкой 50ms (lag будет расти)...");
        ConsumerMetrics metrics = new ConsumerMetrics();
        ObservableConsumer consumer = new ObservableConsumer(
            BOOTSTRAP, GROUP + "-lag-demo", TOPIC, "lag-consumer", metrics);
        consumer.setProcessingDelayMs(50);

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // 3. Ждём 15 секунд — lag растёт
        LogUtils.warning("Ждём 15 секунд (lag растёт)...");
        Thread.sleep(15000);

        // 4. Снимаем задержку — lag уменьшается
        LogUtils.warning("Шаг 3: Снимаем задержку (lag должен уменьшаться)...");
        consumer.setProcessingDelayMs(0);

        // 5. Ждём ещё 15 секунд — lag уменьшается
        LogUtils.warning("Ждём 15 секунд (lag уменьшается)...");
        Thread.sleep(15000);

        // 6. Останавливаем
        consumer.shutdown();
        consumerThread.join(5000);

        // 7. Выводим метрики
        metrics.print();

        LogUtils.success("LAG DEMO завершён");
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar hw8-observability.jar <mode>");
        System.out.println("Modes:");
        System.out.println("  init           - create topic with 3 partitions");
        System.out.println("  produce        - send 1000 messages with correlationId");
        System.out.println("  produce-small  - send 10 messages (for quick test)");
        System.out.println("  consume        - consume without delay");
        System.out.println("  consume-slow   - consume with 50ms delay per message");
        System.out.println("  lag-demo       - full lag demo (1000 msgs + slow consumer)");
    }
}

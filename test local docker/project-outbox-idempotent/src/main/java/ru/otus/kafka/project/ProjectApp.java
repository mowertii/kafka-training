package ru.otus.kafka.project;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import ru.otus.kafka.common.LogUtils;
import ru.otus.kafka.project.config.AppConfig;
import ru.otus.kafka.project.consumer.DltConsumer;
import ru.otus.kafka.project.consumer.PaymentConsumer;
import ru.otus.kafka.project.metrics.Metrics;
import ru.otus.kafka.project.producer.OrderService;
import ru.otus.kafka.project.producer.OutboxRelay;
import ru.otus.kafka.project.retry.RetryManager;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 🎯 ФИНАЛЬНЫЙ ПРОЕКТ: Transactional Outbox + Idempotent Consumer + Retry/DLT + Observability
 * 
 * Режимы:
 *   - init         — создание топиков и таблиц БД
 *   - create-order — создать заказ (Producer + Outbox)
 *   - demo         — полная демонстрация (создание заказов + consumer)
 *   - producer     — запустить только OutboxRelay
 *   - consumer     — запустить только PaymentConsumer
 *   - dlt          — запустить DltConsumer
 *   - metrics      — показать метрики
 */
public class ProjectApp {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        LogUtils.banner("PROJECT: mode=" + mode);

        switch (mode) {
            case "init" -> init();
            case "create-order" -> createOrder(args);
            case "demo" -> runDemo();
            case "producer" -> runProducer();
            case "consumer" -> runConsumer();
            case "dlt" -> runDltConsumer();
            case "metrics" -> Metrics.print();
            default -> printUsage();
        }
    }

    /**
     * Инициализация: создание топиков + таблиц БД.
     */
    private static void init() throws Exception {
        LogUtils.info("Инициализация: создание топиков и таблиц...");

        // ============================================================
        // 1. Создание Kafka топиков
        // ============================================================
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            List<String> topics = List.of(
                AppConfig.ORDERS_TOPIC,
                AppConfig.RETRY_1_TOPIC,
                AppConfig.RETRY_2_TOPIC,
                AppConfig.DLT_TOPIC
            );

            for (String topic : topics) {
                if (admin.listTopics().names().get().contains(topic)) {
                    LogUtils.info("Топик " + topic + " уже существует.");
                } else {
                    admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get();
                    LogUtils.success("Топик " + topic + " создан (3 partitions)");
                }
            }
        }

        // ============================================================
        // 2. Создание таблиц БД
        // ============================================================
        DataSource ds = AppConfig.createDataSource();
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement()) {

            // Читаем schema.sql из resources
            InputStream is = ProjectApp.class.getClassLoader()
                .getResourceAsStream("db/schema.sql");
            if (is == null) {
                throw new RuntimeException("schema.sql not found in resources");
            }
            String schema = new String(is.readAllBytes(), StandardCharsets.UTF_8);

            // Выполняем каждый statement отдельно
            for (String sql : schema.split(";")) {
                String trimmed = sql.trim();
                if (!trimmed.isEmpty()) {
                    st.execute(trimmed);
                }
            }

            LogUtils.success("Таблицы БД созданы: orders, outbox, inbox, payments, dlt_messages");
        }

        LogUtils.success("✅ Инициализация завершена");
    }

    /**
     * Создание заказа (Producer + Outbox).
     */
    private static void createOrder(String[] args) throws Exception {
        String userId = args.length > 1 ? args[1] : "user-" + System.currentTimeMillis();
        int amount = args.length > 2 ? Integer.parseInt(args[2]) : 1000;

        DataSource ds = AppConfig.createDataSource();
        OrderService service = new OrderService(ds);

        String orderId = service.createOrder(userId, amount);
        LogUtils.success("Заказ создан: " + orderId);

        // Запускаем relay ОДИН РАЗ для публикации
        OutboxRelay relay = new OutboxRelay(ds, AppConfig.BOOTSTRAP_SERVERS);
        Thread relayThread = new Thread(relay);
        relayThread.start();

        Thread.sleep(3000); // Дать время на публикацию
        relay.shutdown();
        relayThread.join(5000);
    }

    /**
     * Демонстрация
     */
    private static void runDemo() throws Exception {
        LogUtils.info("=== ПОЛНАЯ ДЕМОНСТРАЦИЯ ===");

        // 1. Инициализация
        init();

        DataSource ds = AppConfig.createDataSource();

        // 2. Запускаем OutboxRelay
        LogUtils.info("Запуск OutboxRelay...");
        OutboxRelay relay = new OutboxRelay(ds, AppConfig.BOOTSTRAP_SERVERS);
        Thread relayThread = new Thread(relay);
        relayThread.start();

        // 3. RetryManager (общий)
        RetryManager retryManager = new RetryManager(AppConfig.BOOTSTRAP_SERVERS);

        // 4. Запускаем PaymentConsumer для MAIN топика
        LogUtils.info("Запуск PaymentConsumer (main)...");
        PaymentConsumer mainConsumer = new PaymentConsumer(
            AppConfig.BOOTSTRAP_SERVERS, AppConfig.ORDERS_TOPIC, ds, retryManager);
        Thread mainThread = new Thread(mainConsumer);
        mainThread.start();

        // 👇 5. Запускаем PaymentConsumer для RETRY.1
        LogUtils.info("Запуск PaymentConsumer (retry.1)...");
        PaymentConsumer retry1Consumer = new PaymentConsumer(
            AppConfig.BOOTSTRAP_SERVERS, AppConfig.RETRY_1_TOPIC, ds, retryManager);
        Thread retry1Thread = new Thread(retry1Consumer);
        retry1Thread.start();

        // 👇 6. Запускаем PaymentConsumer для RETRY.2
        LogUtils.info("Запуск PaymentConsumer (retry.2)...");
        PaymentConsumer retry2Consumer = new PaymentConsumer(
            AppConfig.BOOTSTRAP_SERVERS, AppConfig.RETRY_2_TOPIC, ds, retryManager);
        Thread retry2Thread = new Thread(retry2Consumer);
        retry2Thread.start();

        // 7. Запускаем DltConsumer
        LogUtils.info("Запуск DltConsumer...");
        DltConsumer dltConsumer = new DltConsumer(AppConfig.BOOTSTRAP_SERVERS, ds);
        Thread dltThread = new Thread(dltConsumer);
        dltThread.start();

        // 8. Создаём заказы
        LogUtils.info("Создание заказов...");
        OrderService service = new OrderService(ds);

        for (int i = 1; i <= 5; i++) {
            service.createOrder("user-" + i, 1000 * i);
            Thread.sleep(500);
        }

        LogUtils.warning("Создание 'плохого' заказа...");
        service.createOrder("user-fail", 9999);

        // 9. Ждём обработки (retry занимает время: 3s + 6s + обработка)
        LogUtils.info("Ожидание обработки (45 секунд)...");
        Thread.sleep(45000);

        // 10. Остановка
        LogUtils.warning("Остановка...");
        relay.shutdown();
        mainConsumer.shutdown();
        retry1Consumer.shutdown();
        retry2Consumer.shutdown();
        dltConsumer.shutdown();

        relayThread.join(5000);
        mainThread.join(5000);
        retry1Thread.join(5000);
        retry2Thread.join(5000); 
        dltThread.join(5000);

        // 11. Метрики
        Metrics.print();

        // 12. Проверка БД
        printDbState(ds);

        LogUtils.success("✅ Демонстрация завершена");
    }

    /**
     * Только OutboxRelay.
     */
    private static void runProducer() throws Exception {
        DataSource ds = AppConfig.createDataSource();
        OutboxRelay relay = new OutboxRelay(ds, AppConfig.BOOTSTRAP_SERVERS);

        Runtime.getRuntime().addShutdownHook(new Thread(relay::shutdown));

        relay.run(); // Блокирующий вызов
    }

    /**
     * Только PaymentConsumer.
     */
    private static void runConsumer() throws Exception {
        DataSource ds = AppConfig.createDataSource();
        RetryManager retryManager = new RetryManager(AppConfig.BOOTSTRAP_SERVERS);
        PaymentConsumer consumer = new PaymentConsumer(
            AppConfig.BOOTSTRAP_SERVERS, AppConfig.ORDERS_TOPIC, ds, retryManager);

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        consumer.run();
    }

    /**
     * Только DltConsumer.
     */
    private static void runDltConsumer() throws Exception {
        DataSource ds = AppConfig.createDataSource();
        DltConsumer consumer = new DltConsumer(AppConfig.BOOTSTRAP_SERVERS, ds);

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        consumer.run();
    }

    /**
     * Печать состояния БД.
     */
    private static void printDbState(DataSource ds) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                       DB STATE                               ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");

        try (Connection conn = ds.getConnection()) {
            printCount(conn, "orders", "Orders");
            printCount(conn, "outbox", "Outbox");
            printCount(conn, "inbox", "Inbox");
            printCount(conn, "payments", "Payments");
            printCount(conn, "dlt_messages", "DLT messages");
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
        }

        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    private static void printCount(Connection conn, String table, String label) {
        try (var st = conn.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            System.out.printf("║ %-25s %10d                       ║%n", label + ":", rs.getInt(1));
        } catch (Exception e) {
            System.out.printf("║ %-25s %10s                       ║%n", label + ":", "ERROR");
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar project-outbox-idempotent.jar <mode> [args]");
        System.out.println("Modes:");
        System.out.println("  init              - create Kafka topics and DB tables");
        System.out.println("  create-order      - create order (userId amount)");
        System.out.println("  demo              - full demo (init + relay + consumers + orders)");
        System.out.println("  producer          - run OutboxRelay only");
        System.out.println("  consumer          - run PaymentConsumer only");
        System.out.println("  dlt               - run DltConsumer only");
        System.out.println("  metrics           - print metrics");
    }
    
}

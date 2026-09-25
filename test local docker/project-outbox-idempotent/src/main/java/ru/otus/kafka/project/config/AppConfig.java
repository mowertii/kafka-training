package ru.otus.kafka.project.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import ru.otus.kafka.common.EnvUtils;

import javax.sql.DataSource;

/**
 * Конфигурация приложения.
 * 
 * Читает переменные окружения:
 * - BOOTSTRAP_SERVERS
 * - JDBC_URL, JDBC_USER, JDBC_PASSWORD
 */
public final class AppConfig {

    // Kafka
    public static final String BOOTSTRAP_SERVERS = EnvUtils.getBootstrapServers();
    public static final String ORDERS_TOPIC = "orders.events";
    public static final String RETRY_1_TOPIC = "orders.retry.1";
    public static final String RETRY_2_TOPIC = "orders.retry.2";
    public static final String DLT_TOPIC = "orders.dlt";

    // Consumer
    public static final String PAYMENT_GROUP = "payment-service";
    public static final String DLT_GROUP = "dlt-service";

    // Retry
    public static final int MAX_RETRY_ATTEMPTS = 2;
    public static final long BACKOFF_MS = 3000;

    // Outbox Relay
    public static final int OUTBOX_BATCH_SIZE = 100;
    public static final long OUTBOX_POLL_INTERVAL_MS = 1000;

    private AppConfig() {}

    /**
     * Создаёт DataSource (HikariCP).
     */
    public static DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(EnvUtils.getJdbcUrl());
        config.setUsername(EnvUtils.getJdbcUser());
        config.setPassword(EnvUtils.getJdbcPassword());
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30_000);
        config.setPoolName("kafka-training-pool");
        return new HikariDataSource(config);
    }
}

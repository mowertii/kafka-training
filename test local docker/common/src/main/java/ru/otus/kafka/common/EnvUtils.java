package ru.otus.kafka.common;

/**
 * Утилиты для чтения переменных окружения.
 */
public final class EnvUtils {

    private EnvUtils() {
        // utility class
    }

    public static String get(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    public static String getBootstrapServers() {
        return get("BOOTSTRAP_SERVERS", "localhost:9094");
    }

    public static String getSchemaRegistryUrl() {
        return get("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    }

    public static String getJdbcUrl() {
        return get("JDBC_URL", "jdbc:postgresql://localhost:5433/kafkademo");
    }

    public static String getJdbcUser() {
        return get("JDBC_USER", "demo");
    }

    public static String getJdbcPassword() {
        return get("JDBC_PASSWORD", "demo");
    }
}
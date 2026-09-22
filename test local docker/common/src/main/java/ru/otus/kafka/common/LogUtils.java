
package ru.otus.kafka.common;

/**
 * Утилиты для красивого логирования.
 */
public final class LogUtils {

    private LogUtils() {
        // utility class
    }

    public static void log(String message) {
        System.out.println("[demo] " + message);
    }

    public static void banner(String title) {
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("  " + title);
        System.out.println("=".repeat(70));
    }

    public static void success(String message) {
        System.out.println("✅ " + message);
    }

    public static void error(String message) {
        System.err.println("❌ " + message);
    }

    public static void warning(String message) {
        System.out.println("⚠️  " + message);
    }

    public static void info(String message) {
        System.out.println("ℹ️  " + message);
    }
}
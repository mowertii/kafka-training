package ru.otus.kafka.project.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Простые метрики приложения.
 * 
 * В production используются Micrometer + Prometheus.
 * Здесь — упрощённая версия для наглядности.
 */
public final class Metrics {

    private static final AtomicLong ordersCreated = new AtomicLong(0);
    private static final AtomicLong outboxPublished = new AtomicLong(0);
    private static final AtomicLong eventsProcessed = new AtomicLong(0);
    private static final AtomicLong eventsDuplicated = new AtomicLong(0);
    private static final AtomicLong eventsRetried = new AtomicLong(0);
    private static final AtomicLong eventsDlt = new AtomicLong(0);
    private static final AtomicLong processingErrors = new AtomicLong(0);
    private static final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    private Metrics() {}

    public static void recordOrderCreated() { ordersCreated.incrementAndGet(); }
    public static void recordOutboxPublished() { outboxPublished.incrementAndGet(); }
    public static void recordEventProcessed(long timeMs) {
        eventsProcessed.incrementAndGet();
        totalProcessingTimeMs.addAndGet(timeMs);
    }
    public static void recordEventDuplicated() { eventsDuplicated.incrementAndGet(); }
    public static void recordEventRetried() { eventsRetried.incrementAndGet(); }
    public static void recordEventDlt() { eventsDlt.incrementAndGet(); }
    public static void recordError() { processingErrors.incrementAndGet(); }

    public static void print() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                       METRICS SUMMARY                        ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║ Orders created:            %10d                       ║%n", ordersCreated.get());
        System.out.printf("║ Outbox published:          %10d                       ║%n", outboxPublished.get());
        System.out.printf("║ Events processed:          %10d                       ║%n", eventsProcessed.get());
        System.out.printf("║ Events duplicated:         %10d                       ║%n", eventsDuplicated.get());
        System.out.printf("║ Events retried:            %10d                       ║%n", eventsRetried.get());
        System.out.printf("║ Events sent to DLT:        %10d                       ║%n", eventsDlt.get());
        System.out.printf("║ Processing errors:         %10d                       ║%n", processingErrors.get());
        long processed = eventsProcessed.get();
        double avg = processed == 0 ? 0.0 : (double) totalProcessingTimeMs.get() / processed;
        System.out.printf("║ Avg processing time (ms):  %10.2f                       ║%n", avg);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }
}

package ru.otus.kafka.hw8.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Простые метрики consumer'а.
 * 
 * Используем AtomicLong для thread-safe счётчиков.
 */
public class ConsumerMetrics {

    private final AtomicLong processedMessages = new AtomicLong(0);
    private final AtomicLong errorMessages = new AtomicLong(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    public void recordProcessed(long processingTimeMs) {
        processedMessages.incrementAndGet();
        totalProcessingTimeMs.addAndGet(processingTimeMs);
    }

    public void recordError() {
        errorMessages.incrementAndGet();
    }

    public long getProcessedMessages() {
        return processedMessages.get();
    }

    public long getErrorMessages() {
        return errorMessages.get();
    }

    public long getTotalProcessingTimeMs() {
        return totalProcessingTimeMs.get();
    }

    public double getAvgProcessingTimeMs() {
        long count = processedMessages.get();
        return count == 0 ? 0.0 : (double) totalProcessingTimeMs.get() / count;
    }

    public void print() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║                    METRICS SUMMARY                        ║");
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.printf("║ Processed messages:  %10d                         ║%n", processedMessages.get());
        System.out.printf("║ Error messages:      %10d                         ║%n", errorMessages.get());
        System.out.printf("║ Total processing ms: %10d                         ║%n", totalProcessingTimeMs.get());
        System.out.printf("║ Avg processing ms:   %10.2f                         ║%n", getAvgProcessingTimeMs());
        System.out.println("╚══════════════════════════════════════════════════════════╝");
    }
}

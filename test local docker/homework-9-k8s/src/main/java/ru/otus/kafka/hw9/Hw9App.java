package ru.otus.kafka.hw9;

/**
 * ДЗ №9: Производительность, Kubernetes, Kafka 4.x, Disaster Recovery.
 *
 * Режимы:
 *   - perf-producer  — нагрузочный тест producer
 *   - perf-consumer  — нагрузочный тест consumer
 *   - dr-setup       — настройка Disaster Recovery
 *   - cluster-info   — информация о кластере
 */
public class Hw9App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        System.out.println("=== Homework 9: mode=" + mode + " ===");

        switch (mode) {
            case "perf-producer" -> System.out.println("TODO: perf producer");
            case "perf-consumer" -> System.out.println("TODO: perf consumer");
            case "dr-setup" -> System.out.println("TODO: DR setup");
            case "cluster-info" -> System.out.println("TODO: cluster info");
            default -> printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar hw9-k8s.jar <mode>");
        System.out.println("Modes: perf-producer, perf-consumer, dr-setup, cluster-info");
    }
}

package ru.otus.kafka.hw8;

/**
 * ДЗ №8: Kafka Streams и эксплуатационные особенности.
 *
 * Режимы:
 *   - topology       — запуск Streams Topology
 *   - count          — подсчёт событий
 *   - aggregate      — агрегация
 *   - join           — join двух потоков
 *   - interactive    — Interactive Queries
 */
public class Hw8App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        System.out.println("=== Homework 8: mode=" + mode + " ===");

        switch (mode) {
            case "topology" -> System.out.println("TODO: run topology");
            case "count" -> System.out.println("TODO: run count");
            case "aggregate" -> System.out.println("TODO: run aggregate");
            case "join" -> System.out.println("TODO: run join");
            case "interactive" -> System.out.println("TODO: run interactive queries");
            default -> printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar hw8-streams.jar <mode>");
        System.out.println("Modes: topology, count, aggregate, join, interactive");
    }
}

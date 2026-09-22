package ru.otus.kafka.hw7;

/**
 * ДЗ №7: Kafka Connect, MirrorMaker, troubleshooting интеграций.
 *
 * Режимы:
 *   - register-source    — регистрация Source Connector
 *   - register-sink      — регистрация Sink Connector
 *   - list-connectors    — список коннекторов
 *   - status             — статус коннектора
 *   - mm2-setup          — настройка MirrorMaker 2
 */
public class Hw7App {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        System.out.println("=== Homework 7: mode=" + mode + " ===");

        switch (mode) {
            case "register-source" -> System.out.println("TODO: register source connector");
            case "register-sink" -> System.out.println("TODO: register sink connector");
            case "list-connectors" -> System.out.println("TODO: list connectors");
            case "status" -> System.out.println("TODO: show connector status");
            case "mm2-setup" -> System.out.println("TODO: setup MirrorMaker 2");
            default -> printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar hw7-connect.jar <mode>");
        System.out.println("Modes: register-source, register-sink, list-connectors, status, mm2-setup");
    }
}

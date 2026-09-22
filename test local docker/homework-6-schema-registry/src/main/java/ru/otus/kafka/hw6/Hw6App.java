package ru.otus.kafka.hw6;

import ru.otus.kafka.common.EnvUtils;
import ru.otus.kafka.common.LogUtils;
import ru.otus.kafka.hw6.schema.SchemaCompatibilityCheck;
import ru.otus.kafka.hw6.schema.SchemaRegistrar;

public class Hw6App {

    private static final String SCHEMA_REGISTRY_URL = EnvUtils.getSchemaRegistryUrl();
    private static final String SUBJECT = "orders-value";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];
        LogUtils.banner("HW6: mode=" + mode);

        switch (mode) {
            // ============================================================
            // Schema Registry + Avro (Задания 1-3)
            // ============================================================
            case "register-v1" -> registerV1();
            case "register-v2" -> registerV2();
            case "check-v2" -> checkV2();
            case "check-v3" -> checkV3();

            // ============================================================
            // Monitoring (Задание 4)
            // ============================================================
            case "health" -> System.out.println("TODO: health");
            case "metrics" -> System.out.println("TODO: metrics");

            default -> printUsage();
        }
    }

    // ============================================================
    // Schema Registry: регистрация V1
    // ============================================================
    private static void registerV1() throws Exception {
        LogUtils.info("Регистрация схемы V1 (orderId, userId)...");

        SchemaRegistrar registrar = new SchemaRegistrar(SCHEMA_REGISTRY_URL, SUBJECT);
        registrar.registerSchema("avro/OrderCreated.avsc");

        LogUtils.success("V1 зарегистрирована. Версий: " + registrar.getVersionCount());
    }

    // ============================================================
    // Schema Registry: регистрация V2 (совместимая)
    // ============================================================
    private static void registerV2() throws Exception {
        LogUtils.info("Регистрация схемы V2 (+createdAt)...");

        SchemaCompatibilityCheck check = new SchemaCompatibilityCheck(SCHEMA_REGISTRY_URL, SUBJECT);

        // 1. Проверяем совместимость
        boolean compatible = check.check("avro/OrderCreated_v2.avsc");
        LogUtils.info("V2 совместима с зарегистрированными: " + compatible);

        if (!compatible) {
            LogUtils.error("V2 НЕсовместима! Регистрация отменена.");
            return;
        }

        // 2. Регистрируем
        SchemaRegistrar registrar = new SchemaRegistrar(SCHEMA_REGISTRY_URL, SUBJECT);
        registrar.registerSchema("avro/OrderCreated_v2.avsc");

        LogUtils.success("V2 зарегистрирована. Версий: " + registrar.getVersionCount());
    }

    // ============================================================
    // Schema Registry: проверка V2 (совместимая)
    // ============================================================
    private static void checkV2() throws Exception {
        LogUtils.info("Проверка совместимости V2 (+createdAt)...");

        SchemaCompatibilityCheck check = new SchemaCompatibilityCheck(SCHEMA_REGISTRY_URL, SUBJECT);
        boolean compatible = check.check("avro/OrderCreated_v2.avsc");

        if (compatible) {
            LogUtils.success("V2 СОВМЕСТИМА ✅ (BACKWARD)");
        } else {
            LogUtils.error("V2 НЕсовместима ❌");
        }
    }

    // ============================================================
    // Schema Registry: проверка V3 (НЕсовместимая)
    // ============================================================
    private static void checkV3() throws Exception {
        LogUtils.info("Проверка совместимости V3 (orderId: int → string)...");

        SchemaCompatibilityCheck check = new SchemaCompatibilityCheck(SCHEMA_REGISTRY_URL, SUBJECT);

        try {
            boolean compatible = check.check("avro/OrderCreated_incompatible.avsc");
            if (compatible) {
                LogUtils.error("ОШИБКА: V3 должна быть НЕсовместима!");
            } else {
                LogUtils.success("V3 НЕсовместима ✅ (как и ожидалось)");
            }
        } catch (Exception e) {
            LogUtils.info("V3 отклонена Schema Registry: " + e.getMessage());
            LogUtils.success("V3 НЕсовместима ✅ (как и ожидалось)");
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar hw6-monitoring.jar <mode>");
        System.out.println("Modes:");
        System.out.println("  Schema Registry:");
        System.out.println("    register-v1  — регистрация схемы V1");
        System.out.println("    register-v2  — регистрация V2 (совместимой)");
        System.out.println("    check-v2     — проверка совместимости V2");
        System.out.println("    check-v3     — проверка НЕсовместимости V3");
        System.out.println("  Monitoring:");
        System.out.println("    health       — health-checks");
        System.out.println("    metrics      — метрики");
    }
}
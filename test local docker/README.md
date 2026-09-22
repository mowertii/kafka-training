# Домашнее задание №6 — Schema Registry и эволюция схем

## Описание

Настройка работы с **Avro** и **Schema Registry**, проверка совместимости версий схем и выявление несовместимых изменений до их использования в приложении.

**Цель работы:** настроить работу с Avro и Schema Registry, проверить совместимость версий схем и научиться выявлять несовместимые изменения до их использования в приложении.

## Структура проекта

```
kafka-training/
├── common/                              # Общие утилиты
│   └── src/main/java/ru/otus/kafka/common/
│       ├── KafkaUtils.java
│       ├── JsonUtils.java
│       ├── EnvUtils.java
│       ├── DbUtils.java
│       └── LogUtils.java
│
├── homework-6-schema-registry/          # ДЗ №6: Schema Registry + Avro
│   ├── src/main/avro/                   # Avro-схемы
│   │   ├── OrderCreated.avsc            # V1
│   │   ├── OrderCreated_v2.avsc         # V2 (совместимая)
│   │   └── OrderCreated_incompatible.avsc  # V3 (несовместимая)
│   │
│   ├── src/main/resources/avro/         # Копии схем для загрузки
│   │   ├── OrderCreated.avsc
│   │   ├── OrderCreated_v2.avsc
│   │   └── OrderCreated_incompatible.avsc
│   │
│   ├── src/main/java/ru/otus/kafka/hw6/
│   │   ├── Hw6App.java                  # Точка входа
│   │   └── schema/
│   │       ├── SchemaRegistrar.java             # Регистрация схем
│   │       └── SchemaCompatibilityCheck.java    # Проверка совместимости
│   │
│   ├── Dockerfile                       # Multi-stage сборка
│   └── pom.xml
│
├── docker-compose.yml                   # Kafka + PostgreSQL + Schema Registry
├── pom.xml                              # Parent POM
├── pom-docker.xml                       # POM для Docker-сборки
├── hw6.cmd                              # Скрипт запуска (Windows)
├── hw6.sh                               # Скрипт запуска (Linux/Git Bash)
└── README.md                            # Этот файл
```

## Требования

| Компонент | Версия |
|---|---|
| Docker Desktop | 20.10+ |
| Docker Compose | 2.0+ |
| Java | 21 |
| Apache Kafka | 4.3.1 (KRaft) |
| Schema Registry | Confluent 7.6.0 |
| Avro | 1.11.3 |

## Запуск

### Единый скрипт (рекомендуется)

**Windows CMD:**
```cmd
hw6.cmd
```

**Git Bash / Linux:**
```bash
./hw6.sh
```

Скрипт делает всё автоматически:
1. Запускает инфраструктуру
2. Ждёт готовности Schema Registry (90 секунд)
3. Регистрирует V1
4. Регистрирует V2 (совместимую)
5. Проверяет V3 (несовместимую)
6. Показывает результаты через REST API

### Пошаговый запуск (для отладки)

```bash
# 1. Инфраструктура
docker compose -p kafka-training --profile hw6 up -d

# 2. Регистрация V1
docker compose -p kafka-training --profile hw6 run --rm hw6-app register-v1

# 3. Регистрация V2
docker compose -p kafka-training --profile hw6 run --rm hw6-app register-v2

# 4. Проверка V3
docker compose -p kafka-training --profile hw6 run --rm hw6-app check-v3
```

### Проверка через REST API

```bash
# Список subjects
curl http://localhost:8081/subjects

# Версии схемы
curl http://localhost:8081/subjects/orders-value/versions

# Последняя схема
curl http://localhost:8081/subjects/orders-value/versions/latest

# Режим совместимости
curl http://localhost:8081/config/orders-value
```

### Остановка

```bash
docker compose -p kafka-training down --remove-orphans
```

## Avro-схемы

### V1 — базовая схема

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "ru.otus.kafka.hw6.avro",
  "fields": [
    {"name": "orderId", "type": "int"},
    {"name": "userId", "type": "int"}
  ]
}
```

### V2 — совместимая (добавили `createdAt`)

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "ru.otus.kafka.hw6.avro",
  "fields": [
    {"name": "orderId", "type": "int"},
    {"name": "userId", "type": "int"},
    {"name": "createdAt", "type": "string", "default": ""}
  ]
}
```

**Почему совместима:**
- добавили поле `createdAt` с `default`;
- старые consumer'ы смогут читать новые данные (получат пустую строку);
- BACKWARD совместимо.

### V3 — несовместимая (изменили тип `orderId`)

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "ru.otus.kafka.hw6.avro",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "userId", "type": "int"}
  ]
}
```

**Почему несовместима:**
- изменили тип `orderId` с `int` на `string`;
- старые consumer'ы ожидают `int` → получат ошибку парсинга;
- Schema Registry отклонит регистрацию.

## Ответы на вопросы

### 1. Что хранит Schema Registry?

Schema Registry — сервис, который хранит и управляет Avro-схемами для Kafka-топиков.

| Что хранит | Описание |
|---|---|
| Схемы | Avro, JSON Schema, Protobuf |
| Версии схем | История изменений (v1, v2, v3...) |
| Уникальные ID | Каждая схема получает `schemaId` |
| Маппинг | `subject` → версии схем |
| Режим совместимости | BACKWARD, FORWARD, FULL, NONE |
| Метаданные | Кто, когда, зачем зарегистрировал |

**Как это работает:**

```
Producer → Schema Registry (регистрирует схему, получает schemaId)
        → Kafka (отправляет сообщение + schemaId)

Consumer → Kafka (читает сообщение + schemaId)
        → Schema Registry (получает схему по schemaId)
        → Десериализует сообщение
```

**Ключевые понятия:**
- Subject = `<topic>-value` или `<topic>-key`
- Schema ID = уникальный идентификатор схемы
- Version = версия схемы в рамках subject

### 2. Что означает BACKWARD compatibility?

BACKWARD compatibility — режим совместимости, при котором новая схема может читать данные, записанные старой схемой.

```
Producer V1 ──▶ Kafka ──▶ Consumer V1
Producer V1 ──▶ Kafka ──▶ Consumer V2 ✅ (BACKWARD)
```

**Что можно делать:**

| Изменение | Совместимо? |
|---|---|
| Добавить поле с `default` | ✅ Да |
| Удалить поле с `default` | ✅ Да |
| Добавить поле без `default` | ❌ Нет |
| Переименовать поле | ❌ Нет |
| Изменить тип поля | ❌ Нет |

**Порядок обновления:** сначала обновляем Consumer, потом обновляем Producer.

### 3. Почему первая модификация совместима, а вторая — нет?

**V1 → V2: совместима ✅**

```diff
  {"name": "orderId", "type": "int"},
  {"name": "userId", "type": "int"},
+ {"name": "createdAt", "type": "string", "default": ""}
```

Почему совместима: добавили поле с `default`; старые данные не содержат `createdAt`; Consumer V2 подставит `default`; ничего не сломается.

**V2 → V3: несовместима ❌**

```diff
- {"name": "orderId", "type": "int"},
+ {"name": "orderId", "type": "string"},
  {"name": "userId", "type": "int"}
```

Почему несовместима: изменили тип `orderId` с `int` на `string`; старые данные содержат `int`; Consumer V3 ожидает `string` → ошибка парсинга; Schema Registry отклонит регистрацию.

## Технологии

| Компонент | Версия |
|---|---|
| Java | 21 |
| Apache Kafka | 4.3.1 (KRaft) |
| Confluent Schema Registry | 7.6.0 |
| Apache Avro | 1.11.3 |
| PostgreSQL | 16 |
| Docker Compose | latest |
| Maven | 3.9.9 |

## Ссылки

- [Apache Avro Documentation](https://avro.apache.org/docs/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Schema Evolution](https://docs.confluent.io/platform/current/schema-registry/avro.html)

## Автор

- **Имя:** Ilyas
- **Курс:** Otus «Администрирование платформы Apache Kafka»
- **Дата:** 2026-09-22


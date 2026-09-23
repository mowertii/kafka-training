# Домашнее задание №8 — Метрики, tracing/correlation ID, диагностика lag

## 📌 Описание

Настройка наблюдаемости Kafka producer/consumer с помощью **correlation ID** и **базовых метрик**, а также диагностика **consumer lag** при замедлении обработки сообщений.

**Цель работы:**  
Настроить наблюдаемость Kafka producer/consumer с помощью correlation ID и базовых метрик, а также научиться диагностировать consumer lag при замедлении обработки сообщений.

---

## 📁 Структура проекта

~~~text
kafka-training/
├── common/                                    # Общие утилиты
│   └── src/main/java/ru/otus/kafka/common/
│       ├── KafkaUtils.java
│       ├── EnvUtils.java
│       └── LogUtils.java
│
├── homework-8-observability/                  # ДЗ №8
│   ├── src/main/java/ru/otus/kafka/hw8/
│   │   ├── Hw8App.java                        # Точка входа
│   │   ├── producer/
│   │   │   └── ObservableProducer.java        # Producer с correlationId
│   │   ├── consumer/
│   │   │   └── ObservableConsumer.java        # Consumer с метриками и lag
│   │   └── metrics/
│   │       └── ConsumerMetrics.java           # Простые метрики
│   ├── Dockerfile
│   └── pom.xml
│
├── docker-compose.yml
├── pom.xml
├── pom-docker-hw8.xml                         # POM для HW8
├── hw8-full.cmd                               # Единый скрипт (Windows)
├── hw8-full.sh                                # Единый скрипт (Linux)
└── README.md
~~~

---

## ⚙️ Требования

~~~text
Docker Desktop 20.10+
Docker Compose 2.0+
Java 21
Apache Kafka 4.3.1 (KRaft)
~~~

---

## 🚀 Запуск

### Единый скрипт (рекомендуется)

**Windows CMD:**
~~~cmd
hw8-full.cmd
~~~

**Git Bash / Linux:**
~~~bash
./hw8-full.sh
~~~

Скрипт делает всё автоматически:
1. Запускает Kafka
2. Создаёт топик `hw8-observability-topic` (3 partitions)
3. Отправляет 1000 сообщений с correlationId
4. Запускает LAG DEMO: медленный consumer → lag растёт → ускорение → lag уменьшается
5. Выводит метрики

### Пошаговый запуск (для отладки)

~~~bash
# 1. Инфраструктура
docker compose -p kafka-training --profile hw8 up -d kafka

# 2. Создать топик
docker compose -p kafka-training --profile hw8 run --rm --build hw8-app init

# 3. Отправить 1000 сообщений
docker compose -p kafka-training --profile hw8 run --rm hw8-app produce

# 4. Быстрая проверка (10 сообщений)
docker compose -p kafka-training --profile hw8 run --rm hw8-app produce-small
docker compose -p kafka-training --profile hw8 run --rm hw8-app consume

# 5. LAG DEMO
docker compose -p kafka-training --profile hw8 run --rm hw8-app lag-demo
~~~

### Остановка

~~~bash
docker compose -p kafka-training down --remove-orphans
~~~

---

## 📊 Результаты проверки

### 1. Correlation ID в headers

Producer добавляет `correlationId` в Kafka headers:

~~~java
record.headers().add(new RecordHeader(
    "correlationId",
    correlationId.getBytes(StandardCharsets.UTF_8)));
~~~

Consumer читает `correlationId` из headers:

~~~java
Header header = record.headers().lastHeader("correlationId");
if (header != null) {
    correlationId = new String(header.value(), StandardCharsets.UTF_8);
}
~~~

### 2. Логи с correlationId

**Producer:**
~~~text
📤 Sent: msgId=1 correlationId=a1b2c3d4-... partition=0 offset=0
📤 Sent: msgId=2 correlationId=e5f6g7h8-... partition=1 offset=0
~~~

**Consumer:**
~~~text
[lag-consumer] 📩 correlationId=1c565459-8439-4d67-b401-714c29227f55 partition=2 offset=657 key=key-2
[lag-consumer] ✅ PROCESSED correlationId=1c565459-8439-4d67-b401-714c29227f55 value={"messageId":974,...}
~~~

### 3. Метрики

~~~text
╔══════════════════════════════════════════════════════════╗
║                    METRICS SUMMARY                        ║
╠══════════════════════════════════════════════════════════╣
║ Processed messages:        2000                         ║
║ Error messages:               0                         ║
║ Total processing ms:      15526                         ║
║ Avg processing ms:         7.76                         ║
╚══════════════════════════════════════════════════════════╝
~~~

### 4. LAG DEMO

~~~text
[lag-consumer] ⏱️  Processing delay set to 50 ms
[lag-consumer] 📊 LAG = 980 (processed=20, errors=0)    ← lag растёт
[lag-consumer] 📊 LAG = 850 (processed=150, errors=0)
[lag-consumer] ⏱️  Processing delay set to 0 ms         ← снимаем задержку
[lag-consumer] 📊 LAG = 200 (processed=800, errors=0)   ← lag уменьшается
[lag-consumer] 📊 LAG = 0 (processed=1000, errors=0)    ← lag = 0
~~~

---

## 📝 Ответы на вопросы

### 1. Что такое consumer lag?

**Consumer lag** — это **разница между последним offset в партиции и текущим offset consumer'а**.

~~~text
Partition 0:
[msg0] [msg1] [msg2] [msg3] [msg4] [msg5] [msg6] [msg7] [msg8] [msg9]
                                              ↑
                                        Consumer offset = 4
                                        End offset = 10
                                        LAG = 10 - 4 = 6
~~~

**Формула:**
~~~text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
~~~

**Когда растёт:**
- Consumer медленнее, чем Producer
- Consumer упал / перезапускается
- Обработка сообщений занимает много времени
- Consumer не успевает обрабатывать из-за нагрузок

**Когда уменьшается:**
- Consumer догоняет Producer
- Задержка обработки уменьшается
- Добавили больше consumer'ов (rebalance)

**Почему важен:**
- ✅ Показывает, **успевает ли** consumer обрабатывать
- ✅ Растёт → **проблема** (сообщения копятся)
- ✅ 0 → **всё хорошо**
- ✅ Основа для **алертов** в production

**Как измерить:**
~~~bash
# Через Kafka CLI
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --describe
~~~

~~~text
GROUP           TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
my-group        orders     0          100             150             50
my-group        orders     1          200             200             0
my-group        orders     2          300             350             50
~~~

---

### 2. Зачем нужен correlation ID?

**Correlation ID** — это **уникальный идентификатор**, который присваивается сообщению и **передаётся через все сервисы** в цепочке обработки.

**Проблема без correlation ID:**
~~~text
Service A → Kafka → Service B → Kafka → Service C
          
При ошибке в Service C:
- Логи Service A: "Обработано сообщение"
- Логи Service B: "Обработано сообщение"
- Логи Service C: "ОШИБКА"

Вопрос: какое сообщение вызвало ошибку? НЕПОНЯТНО!
~~~

**С correlation ID:**
~~~text
Service A → Kafka → Service B → Kafka → Service C
   ↓           ↓         ↓          ↓         ↓
corr-123   corr-123  corr-123   corr-123  corr-123

При ошибке в Service C:
grep "corr-123" logs/
→ Все логи от всех сервисов для этого сообщения!
~~~

**Что даёт:**
- ✅ **Трассировка** сообщения через все сервисы
- ✅ Быстрый **поиск** в логах (`grep correlationId`)
- ✅ **Отладка** распределённых систем
- ✅ **Аудит** (кто, когда, что обработал)
- ✅ **SLA** (время обработки end-to-end)

**Как работает:**

1. **Producer** генерирует `correlationId` (UUID) и кладёт в headers:
~~~java
String correlationId = UUID.randomUUID().toString();
record.headers().add(new RecordHeader(
    "correlationId", correlationId.getBytes()));
~~~

2. **Consumer** читает `correlationId` из headers:
~~~java
Header header = record.headers().lastHeader("correlationId");
String correlationId = new String(header.value());
~~~

3. **Все логи** содержат `correlationId`:
~~~text
[consumer-1] 📩 correlationId=abc-123 partition=0 offset=5
[consumer-1] ✅ PROCESSED correlationId=abc-123
~~~

4. **При ошибке** — `grep correlationId=abc-123` → все логи цепочки.

**Best practices:**
- ✅ Использовать **UUID** (уникальность)
- ✅ Передавать через **headers** (не value)
- ✅ Логировать **всегда** (на входе и выходе)
- ✅ Использовать **MDC** (Mapped Diagnostic Context) в logback/log4j
- ✅ Пробрасывать в **downstream** сервисы

---

## 🛠️ Технологии

~~~text
Java                 21
Apache Kafka         4.3.1 (KRaft)
Docker Compose       latest
Maven                3.9.9 (в Docker)
~~~

---

## 📎 Ссылки

- [Kafka Consumer Lag](https://kafka.apache.org/documentation/#consumerconfigs)
- [Correlation ID Pattern](https://microservices.io/patterns/observability/correlation-id.html)
- [Micrometer Metrics](https://micrometer.io/)

---

## 👨‍🎓 Автор

**Имя:** Ilyas  
**Курс:** Otus "Администрирование платформы Apache Kafka"  
**Дата:** 2026-09-23

---

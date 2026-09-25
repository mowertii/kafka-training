# Финальный проект — Java-сервис с Transactional Outbox, Idempotent Consumer, Retry/DLT и Observability

## 📌 Описание

Production-ready сервис, демонстрирующий **надёжную асинхронную обработку заказов** через Kafka с использованием:

- **Transactional Outbox** — атомарная запись бизнес-данных и событий
- **Idempotent Consumer (Inbox)** — защита от дублей
- **Retry Topics + DLT** — обработка ошибок
- **Correlation ID** — трассировка через все сервисы
- **Метрики** — наблюдаемость обработки

**Цель работы:**  
Собрать все паттерны Kafka в единый production-ready сервис с полной наблюдаемостью.

---

## 🏗️ Архитектура

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORDER SERVICE (Producer)                            │
│                                                                             │
│  1. POST /orders                                                            │
│  2. INSERT INTO orders + INSERT INTO outbox (ОДНА ТРАНЗАКЦИЯ!)              │
│  3. Relay читает outbox → Kafka                                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KAFKA                                          │
│                                                                             │
│  orders.events (3 partitions)                                               │
│  orders.retry.1, orders.retry.2, orders.dlt                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PAYMENT SERVICE (Consumer)                             │
│                                                                             │
│  1. Читает из orders.events                                                 │
│  2. Проверяет inbox (идемпотентность)                                       │
│  3. Обрабатывает → INSERT INTO payments + INSERT INTO inbox                 │
│  4. При ошибке → retry topic                                                │
│  5. После N попыток → DLT                                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          OBSERVABILITY                                      │
│                                                                             │
│  - Correlation ID в headers и логах                                         │
│  - Метрики: processed, errors, retried, DLT                                 │
│  - Логи: все шаги с correlationId                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Структура проекта

~~~text
kafka-training/
├── common/                                    # Общие утилиты
│
├── project-outbox-idempotent/                 # ФИНАЛЬНЫЙ ПРОЕКТ
│   ├── src/main/java/ru/otus/kafka/project/
│   │   ├── ProjectApp.java                    # Точка входа
│   │   ├── config/AppConfig.java              # Конфигурация
│   │   ├── model/
│   │   │   ├── Order.java                     # Модель заказа
│   │   │   └── Event.java                     # Модель события
│   │   ├── producer/
│   │   │   ├── OrderService.java              # Бизнес-логика + Outbox
│   │   │   └── OutboxRelay.java               # Relay: outbox → Kafka
│   │   ├── consumer/
│   │   │   ├── PaymentConsumer.java           # Consumer с Inbox + Retry
│   │   │   └── DltConsumer.java               # DLT consumer
│   │   ├── retry/RetryManager.java            # Retry/DLT manager
│   │   ├── metrics/Metrics.java               # Метрики
│   │   └── observability/CorrelationId.java   # Correlation ID
│   ├── src/main/resources/db/schema.sql       # Схема БД
│   ├── Dockerfile
│   └── pom.xml
│
├── docker-compose.yml
├── pom.xml
├── pom-docker-project.xml
├── project-full.sh                            # Единый скрипт (Linux)
├── project-full.cmd                           # Единый скрипт (Windows)
└── README.md
~~~

---

## ⚙️ Требования

~~~text
Docker Desktop 20.10+
Docker Compose 2.0+
Java 21
Apache Kafka 4.3.1 (KRaft)
PostgreSQL 16
~~~

---

## 🚀 Запуск

### Единый скрипт (рекомендуется)

**Git Bash / Linux:**
~~~bash
./project-full.sh
~~~

**Windows CMD:**
~~~cmd
project-full.cmd
~~~

Скрипт делает всё автоматически:
1. Запускает Kafka + PostgreSQL
2. Собирает проект
3. Создаёт топики и таблицы БД
4. Запускает демонстрацию (создание заказов + consumer + retry + DLT)
5. Показывает метрики и состояние БД

### Пошаговый запуск

~~~bash
# 1. Инфраструктура
docker compose -p kafka-training --profile project up -d kafka postgres

# 2. Создать топики и таблицы
docker compose -p kafka-training --profile project run --rm --build project-app init

# 3. Полная демонстрация
docker compose -p kafka-training --profile project run --rm project-app demo

# 4. Только producer (OutboxRelay)
docker compose -p kafka-training --profile project run --rm project-app producer

# 5. Только consumer (PaymentConsumer)
docker compose -p kafka-training --profile project run --rm project-app consumer
~~~

### Проверка БД

~~~bash
docker exec -it kafka-training-postgres psql -U demo -d kafkademo

SELECT * FROM orders;
SELECT * FROM outbox;
SELECT * FROM inbox;
SELECT * FROM payments;
SELECT * FROM dlt_messages;
~~~

---

## 📊 Результаты проверки

### 1. Создание заказов (Producer + Outbox)

~~~text
[correlationId=d7673a49...] 📝 Создание заказа: order-28e57b90
[correlationId=d7673a49...] ✅ [БД] orders: id=order-28e57b90 userId=user-1 amount=1000
[correlationId=d7673a49...] ✅ [БД] outbox: eventId=c298decb... eventType=OrderCreated status=pending
[correlationId=d7673a49...] 🎯 [БД] Транзакция закоммичена: order + outbox сохранены АТОМАРНО
~~~

**Что видно:** заказ и событие сохранены **в одной транзакции**.

### 2. Публикация в Kafka (OutboxRelay)

~~~text
[correlationId=d7673a49...] 📤 [KAFKA] Опубликовано: id=c298decb... eventType=OrderCreated
📤 [RELAY] Батч завершён: отправлено=1, ошибок=0
~~~

**Что видно:** Relay прочитал outbox и опубликовал в Kafka.

### 3. Обработка (Idempotent Consumer)

~~~text
[correlationId=d7673a49...] 📩 [CONSUMER] Получено: eventId=c298decb... retryCount=0
[correlationId=d7673a49...] 💰 [CONSUMER] Платёж создан: orderId=order-28e57b90
[correlationId=d7673a49...] ✅ [CONSUMER] Inbox записан: eventId=c298decb...
[correlationId=d7673a49...] ✅ PROCESSED за 527ms
~~~

**Что видно:** 
- Платёж создан
- Inbox записан (идемпотентность)
- Все логи с одним `correlationId`

### 4. Метрики

~~~text
╔══════════════════════════════════════════════════════════════╗
║                       METRICS SUMMARY                        ║
╠══════════════════════════════════════════════════════════════╣
║ Orders created:                     6                       ║
║ Outbox published:                   6                       ║
║ Events processed:                   6                       ║
║ Events duplicated:                  0                       ║
║ Events retried:                     0                       ║
║ Events sent to DLT:                 0                       ║
║ Processing errors:                  0                       ║
║ Avg processing time (ms):      485.50                       ║
╚══════════════════════════════════════════════════════════════╝
~~~

### 5. Состояние БД

~~~text
╔══════════════════════════════════════════════════════════════╗
║                       DB STATE                               ║
╠══════════════════════════════════════════════════════════════╣
║ Orders:                            6                       ║
║ Outbox:                            6                       ║
║ Inbox:                             6                       ║
║ Payments:                          6                       ║
║ DLT messages:                      0                       ║
╚══════════════════════════════════════════════════════════════╝
~~~

---

## 📝 Описание паттернов

### 1. Transactional Outbox

**Проблема dual-write:**
~~~text
save(order) → OK
producer.send(event) → FAIL
→ Заказ создан, но событие потеряно!
~~~

**Решение:**
~~~text
ОДНА ТРАНЗАКЦИЯ БД:
  1. INSERT INTO orders
  2. INSERT INTO outbox (status='pending')
  COMMIT

Отдельный Relay:
  3. SELECT FROM outbox WHERE status='pending'
  4. Отправить в Kafka
  5. UPDATE status='published'
~~~

**Гарантия:** если транзакция закоммичена, событие **обязательно** будет отправлено.

### 2. Idempotent Consumer (Inbox)

**Проблема at-least-once:**
~~~text
1. Consumer обработал событие
2. Упал ДО commit offset
3. Перезапустился
4. Прочитал ТО ЖЕ событие СНОВА
5. Обработал СНОВА (дубль!)
~~~

**Решение:**
~~~text
1. Проверить eventId в inbox
2. Если есть → SKIP
3. Если нет → бизнес-логика + INSERT inbox (ОДНА ТРАНЗАКЦИЯ!)
~~~

### 3. Retry Topics + DLT

**Логика:**
~~~text
orders.events → ошибка → orders.retry.1
orders.retry.1 → ошибка → orders.retry.2
orders.retry.2 → ошибка → orders.dlt
~~~

**DLT — не мусорка:**
- Сохраняем причину ошибки (headers)
- Сохраняем в БД для анализа
- Ждём ручного разбора

### 4. Correlation ID

**Проблема:** как найти все логи одного сообщения?

**Решение:**
~~~text
1. Producer генерирует correlationId (UUID)
2. Кладёт в Kafka headers
3. Consumer читает из headers
4. Все логи содержат correlationId

grep "correlationId=d7673a49" → все логи цепочки!
~~~

### 5. Метрики

| Метрика | Описание |
|---------|----------|
| Orders created | Создано заказов |
| Outbox published | Опубликовано событий |
| Events processed | Обработано событий |
| Events duplicated | Пропущено дублей |
| Events retried | Отправлено в retry |
| Events sent to DLT | Отправлено в DLT |
| Processing errors | Ошибок обработки |
| Avg processing time | Среднее время |

---

## 🛠️ Технологии

~~~text
Java                 21
Apache Kafka         4.3.1 (KRaft)
PostgreSQL           16
HikariCP             5.1.0
Jackson              2.17.2
Micrometer           1.12.5
Docker Compose       latest
Maven                3.9.9 (в Docker)
~~~

---

## 📎 Ссылки

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [Idempotent Consumer Pattern](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
- [Dead Letter Topic](https://www.confluent.io/blog/dead-letter-queue-pattern/)
- [Correlation ID Pattern](https://microservices.io/patterns/observability/correlation-id.html)

---

## 👨‍🎓 Автор

**Имя:** Ilyas  
**Курс:** Otus "Администрирование платформы Apache Kafka"  
**Дата:** 2026-09-25

---

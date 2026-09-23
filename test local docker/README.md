# Домашнее задание №9 — Kafka Streams: state store, repartition, restore, exactly-once-v2

## 📌 Описание

Реализация Kafka Streams-приложения с **state store**, **repartition** и **exactly_once_v2**, а также проверка восстановления состояния после перезапуска приложения.

**Цель работы:**  
Реализовать Kafka Streams-приложение с state store, repartition и exactly_once_v2, а также проверить восстановление состояния после перезапуска приложения.

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
├── homework-9-streams/                        # ДЗ №9
│   ├── src/main/java/ru/otus/kafka/hw9/
│   │   ├── Hw9App.java                        # Точка входа
│   │   ├── producer/
│   │   │   └── OrderProducer.java             # Producer для OrderCreated
│   │   └── streams/
│   │       └── OrderAggregationTopology.java  # Kafka Streams Topology
│   ├── Dockerfile
│   └── pom.xml
│
├── docker-compose.yml
├── pom.xml
├── pom-docker-hw9.xml                         # POM для HW9
├── hw9-full.cmd                               # Единый скрипт (Windows)
├── hw9-full.sh                                # Единый скрипт (Linux)
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
hw9-full.cmd
~~~

**Git Bash / Linux:**
~~~bash
./hw9-full.sh
~~~

Скрипт делает всё автоматически:
1. Запускает Kafka
2. Создаёт топики `orders` и `order-totals-output`
3. Отправляет 20 событий `OrderCreated`
4. Запускает Kafka Streams на 30 секунд
5. Показывает internal topics (repartition)

### Пошаговый запуск (для отладки)

~~~bash
# 1. Инфраструктура
docker compose -p kafka-training --profile hw9 up -d kafka

# 2. Создать топики
docker compose -p kafka-training --profile hw9 run --rm --build hw9-app init

# 3. Отправить 20 событий
docker compose -p kafka-training --profile hw9 run --rm hw9-app produce

# 4. Запустить Streams (30 секунд)
docker compose -p kafka-training --profile hw9 run --rm hw9-app streams

# 5. Проверить internal topics
docker exec kafka-training-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --list | grep -E "(orders|order-totals|hw9)"
~~~

### Остановка

~~~bash
docker compose -p kafka-training down --remove-orphans
~~~

---

## 📊 Результаты проверки

### 1. Топология Kafka Streams

~~~text
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [orders])
      --> KSTREAM-MAPVALUES-0000000001
    Processor: KSTREAM-MAPVALUES-0000000001 (stores: [])
      --> KSTREAM-KEY-SELECT-0000000002
    Processor: KSTREAM-KEY-SELECT-0000000002 (stores: [])
      --> order-totals-store-repartition-filter
    Sink: order-totals-store-repartition-sink (topic: order-totals-store-repartition)

  Sub-topology: 1
    Source: order-totals-store-repartition-source (topics: [order-totals-store-repartition])
      --> KSTREAM-AGGREGATE-0000000003
    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [order-totals-store])
      --> KTABLE-TOSTREAM-0000000007
    Sink: KSTREAM-SINK-0000000009 (topic: order-totals-output)
~~~

**Что видно:**
- ✅ **REPARTITION TOPIC** (`order-totals-store-repartition`)
- ✅ **STATE STORE** (`order-totals-store`)
- ✅ **Two sub-topologies** (из-за repartition)

### 2. Отправка событий

~~~text
📤 Sent: orderId=1 userId=43 amount=100 partition=1 offset=0
📤 Sent: orderId=2 userId=44 amount=200 partition=0 offset=0
📤 Sent: orderId=3 userId=42 amount=300 partition=0 offset=1
...
✅ 20 событий отправлено
~~~

### 3. Агрегация

~~~text
🔍 Parsed: userId=42 amount=300
💰 Aggregated: userId=42 amount=300 total=300
📊 TOTAL: 300
🔍 Parsed: userId=43 amount=100
💰 Aggregated: userId=43 amount=100 total=100
📊 TOTAL: 100
~~~

---

## 📝 Ответы на вопросы

### 1. Для чего нужен state store?

**State store** — это **локальное хранилище состояния**, которое Kafka Streams использует для:

| Назначение | Описание |
|------------|----------|
| **Агрегации** | `count()`, `sum()`, `aggregate()` |
| **Join'ы** | `join()`, `leftJoin()`, `outerJoin()` |
| **Оконные операции** | `windowedBy()`, `count()` по окнам |
| **Дедупликация** | Хранение обработанных ID |
| **Кэширование** | Быстрый доступ к последним значениям |

**В нашем примере:**

~~~text
State Store: order-totals-store
┌──────────────────────────────────────┐
│  Key (userId)  │  Value (total)       │
├────────────────┼──────────────────────┤
│  42            │  3600                │
│  43            │  4800                │
│  44            │  5900                │
└──────────────────────────────────────┘
~~~

**Как работает:**

1. Событие приходит → `userId=42, amount=300`
2. Kafka Streams ищет в state store ключ `42`
3. Находит текущее значение `total=300`
4. Прибавляет `300 + 300 = 600`
5. Сохраняет обратно `userId=42, total=600`

**Типы state stores:**
- **KeyValueStore** — для агрегаций (наш случай)
- **WindowStore** — для оконных операций
- **SessionStore** — для сессий

**Почему важен:**
- ✅ **Скорость** — не нужно читать всю историю
- ✅ **Инкрементальность** — обновляем только изменения
- ✅ **Локальность** — данные рядом с обработкой

---

### 2. Откуда Kafka Streams восстанавливает state store после потери локального состояния?

**Kafka Streams восстанавливает state store из двух источников:**

| Источник | Что содержит | Когда используется |
|----------|-------------|-------------------|
| **Changelog topic** | Все изменения state store | Основной механизм restore |
| **Repartition topic** | Перепартиционированные данные | Для пересчёта |

**Changelog topic:**

Для **каждого** state store Kafka Streams создаёт **changelog topic**:

~~~text
State Store: order-totals-store
Changelog Topic: hw9-streams-app-order-totals-store-changelog
~~~

**Как работает:**

~~~text
1. Streams обновляет state store
   → Записывает в changelog topic (Kafka)

2. Streams падает / перезапускается

3. Streams восстанавливает state store:
   → Читает ВСЕ записи из changelog topic
   → Применяет их к локальному state store
   → Состояние восстановлено!
~~~

**Ключевые настройки:**

~~~java
// Changelog topic создаётся автоматически
// Настройки по умолчанию:
StreamsConfig.REPLICATION_FACTOR_CONFIG = 1  // RF для changelog
StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG = 0  // standby replicas
~~~

**Процесс восстановления:**

~~~text
1. Streams стартует
2. Создаёт локальный state store
3. Читает changelog topic с offset=0
4. Применяет каждую запись к state store
5. Когда доходит до конца — state store готов
6. Начинает обработку новых событий
~~~

**Что происходит при перезапуске:**

~~~text
До перезапуска:
  State store: userId=42 → total=3600

Перезапуск:
  State store пуст (in-memory) или удалён

Restore:
  Читаем changelog: userId=42, +300, +600, ...
  Применяем: total=3600
  State store восстановлен!

После перезапуска:
  Новое событие: userId=42, amount=100
  total = 3600 + 100 = 3700  ← продолжает с предыдущего значения!
~~~

**Преимущества:**
- ✅ **Персистентность** — состояние в Kafka
- ✅ **Отказоустойчивость** — Kafka хранит данные
- ✅ **Масштабируемость** — можно восстановить на новом инстансе

**Standby replicas:**

Для **быстрого** restore можно настроить standby replicas:

~~~java
props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
~~~

Тогда:
- Вторая реплика state store живёт на другом инстансе
- При падении основного — standby становится активным
- Restore **мгновенный** (не нужно читать changelog)

---

### 3. Что даёт exactly_once_v2?

**exactly_once_v2** — это **гарантия доставки**, при которой каждое сообщение обрабатывается **ровно один раз**.

| Гарантия | Описание | Дубли | Потери |
|----------|----------|-------|--------|
| **at_most_once** | Максимум 1 раз | ❌ Нет | ✅ Возможны |
| **at_least_once** | Минимум 1 раз | ✅ Возможны | ❌ Нет |
| **exactly_once_v2** | Ровно 1 раз | ❌ Нет | ❌ Нет |

**Как работает:**

**Проблема без EOS:**

~~~text
1. Streams обрабатывает событие
2. Обновляет state store
3. Отправляет результат в output topic
4. ❌ Streams падает ДО commit offset
5. Streams перезапускается
6. Читает то же событие СНОВА
7. Обновляет state store СНОВА (дубль!)
8. Отправляет результат СНОВА (дубль!)
~~~

**Решение с EOS:**

~~~text
1. Streams обрабатывает событие
2. Открывает ТРАНЗАКЦИЮ (Kafka)
3. Обновляет state store
4. Отправляет результат в output topic
5. Коммитит offset + транзакцию
6. ✅ Всё атомарно!

При падении:
→ Транзакция откатывается
→ Offset НЕ коммитится
→ Состояние НЕ изменяется
→ Нет дублей!
~~~

**Ключевые компоненты:**

| Компонент | Роль |
|-----------|------|
| **Transactional Producer** | Атомарная запись в Kafka |
| **Transaction Coordinator** | Управление транзакциями |
| **Idempotent Producer** | Защита от дублей при retry |
| **Read Committed** | Consumer видит только закоммиченные данные |

**Настройка:**

~~~java
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
~~~

**Что даёт:**
- ✅ **Нет дублей** в output topic
- ✅ **Нет дублей** в state store
- ✅ **Атомарность** — state + output + offset
- ✅ **Консистентность** — данные всегда корректны

**Цена:**
- ❌ **Производительность** — транзакции медленнее
- ❌ **Latency** — commit занимает время
- ❌ **Сложность** — больше настроек

**Когда использовать:**
- ✅ **Финансовые операции** (платежи, балансы)
- ✅ **Критичные данные** (заказы, инвентарь)
- ✅ **Exactly-once обязателен**

**Когда НЕ использовать:**
- ❌ **Логи** (дубли не критичны)
- ❌ **Метрики** (можно агрегировать)
- ❌ **Высокая нагрузка** (производительность важнее)

**Отличия v1 vs v2:**

| Аспект | exactly_once (v1) | exactly_once_v2 |
|--------|-------------------|-----------------|
| **Область** | Только Streams | Streams + Producer |
| **Производительность** | Ниже | Выше |
| **Сложность** | Выше | Ниже |
| **Рекомендация** | Устарело | ✅ Используйте v2 |

**Что нового в v2:**
- ✅ Работает с **любыми** Kafka producers
- ✅ Меньше **транзакционных** накладных расходов
- ✅ Лучше **масштабируется**
- ✅ Рекомендуется Confluent для production

---

## 🛠️ Технологии

~~~text
Java                 21
Apache Kafka         4.3.1 (KRaft)
Kafka Streams        4.3.1
Docker Compose       latest
Maven                3.9.9 (в Docker)
~~~

---

## 📎 Ссылки

- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [State Stores](https://kafka.apache.org/documentation/streams/developer-guide/processor-api.html#state-stores)
- [Exactly-Once Semantics](https://kafka.apache.org/documentation/#semantics)
- [KIP-447: Producer Scalability for EOS](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)

---

## 👨‍🎓 Автор

**Имя:** Ilyas  
**Курс:** Otus "Администрирование платформы Apache Kafka"  
**Дата:** 2026-09-23

---

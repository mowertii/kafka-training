# Домашнее задание №7 — Rebalancing, static membership, cooperative assignor, graceful shutdown

## 📌 Описание

Исследование поведения consumer group при **ребалансировке** и настройка:
- **Cooperative assignor** (`CooperativeStickyAssignor`)
- **Static membership** (`group.instance.id`)
- **Graceful shutdown** (`consumer.wakeup()` + `close()`)

**Цель работы:**  
Исследовать поведение consumer group при ребалансировке и настроить cooperative assignor, static membership и graceful shutdown для корректной работы Kafka consumers.

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
├── homework-7-rebalancing/                    # ДЗ №7
│   ├── src/main/java/ru/otus/kafka/hw7/
│   │   ├── Hw7App.java                        # Точка входа
│   │   ├── consumer/
│   │   │   └── RebalanceConsumer.java         # Consumer с rebalance listener
│   │   └── producer/
│   │       └── TopicProducer.java             # Producer
│   ├── Dockerfile
│   └── pom.xml
│
├── docker-compose.yml
├── pom.xml                                    # Parent POM
├── pom-docker.xml                             # POM для HW6
├── pom-docker-hw7.xml                         # POM для HW7
├── hw7.cmd                                    # Пошаговый скрипт (Windows)
├── hw7.sh                                     # Пошаговый скрипт (Linux)
├── hw7-full.cmd                               # Единый скрипт (Windows)
├── hw7-full.sh                                # Единый скрипт (Linux)
└── README.md
~~~

---

## ⚙️ Требования

~~~text
Docker Desktop 20.10+
Docker Compose 2.0+
Java 21
Apache Kafka 4.3.1 (KRaft)
Maven 3.9.9 (в Docker)
~~~

---

## 🚀 Запуск

### Единый скрипт (рекомендуется)

**Windows CMD:**
~~~cmd
hw7-full.cmd
~~~

**Git Bash / Linux:**
~~~bash
./hw7-full.sh
~~~

Скрипт делает всё автоматически:
1. Запускает Kafka
2. Создаёт топик `hw7-rebalance-topic` (3 partitions)
3. Отправляет 30 сообщений
4. **DEMO 1**: Rebalance с EAGER assignor (`RangeAssignor`)
5. **DEMO 2**: Rebalance с COOPERATIVE assignor (`CooperativeStickyAssignor`)
6. **DEMO 3**: Rebalance со STATIC membership (`group.instance.id`)

### Пошаговый запуск (для отладки)

~~~bash
# 1. Инфраструктура
docker compose -p kafka-training --profile hw7 up -d kafka

# 2. Создать топик
docker compose -p kafka-training --profile hw7 run --rm --build hw7-app init

# 3. Отправить сообщения
docker compose -p kafka-training --profile hw7 run --rm hw7-app produce

# 4. DEMO 1: EAGER
docker compose -p kafka-training --profile hw7 run --rm hw7-app demo-eager

# 5. DEMO 2: COOPERATIVE
docker compose -p kafka-training --profile hw7 run --rm hw7-app demo-cooperative

# 6. DEMO 3: STATIC
docker compose -p kafka-training --profile hw7 run --rm hw7-app demo-static
~~~

### Остановка

~~~bash
docker compose -p kafka-training down --remove-orphans
~~~

---

## 📊 Результаты проверки

### DEMO 1: EAGER assignor (RangeAssignor)

~~~text
[consumer-1] ✅ onPartitionsAssigned: [hw7-rebalance-topic-0, hw7-rebalance-topic-1]
[consumer-2] ✅ onPartitionsAssigned: [hw7-rebalance-topic-2]

... (остановка consumer-1) ...

[consumer-1] ⚠️  onPartitionsRevoked: [hw7-rebalance-topic-0, hw7-rebalance-topic-1]
[consumer-2] ⚠️  onPartitionsRevoked: [hw7-rebalance-topic-2]  ← ВСЕ партиции!
[consumer-2] ✅ onPartitionsAssigned: [hw7-rebalance-topic-0, hw7-rebalance-topic-1, hw7-rebalance-topic-2]
~~~

**Вывод:** EAGER assignor отзывает **ВСЕ** партиции у **ВСЕХ** consumer'ов, потом заново распределяет.

---

### DEMO 2: COOPERATIVE assignor

~~~text
[consumer-1] ✅ onPartitionsAssigned: [hw7-rebalance-topic-0, hw7-rebalance-topic-2]
[consumer-2] ✅ onPartitionsAssigned: [hw7-rebalance-topic-1]

... (остановка consumer-1) ...

[consumer-1] ⚠️  onPartitionsRevoked: [hw7-rebalance-topic-0, hw7-rebalance-topic-2]
[consumer-2] ✅ onPartitionsAssigned: [hw7-rebalance-topic-0, hw7-rebalance-topic-2]  ← только MOVED!
~~~

**Вывод:** COOPERATIVE assignor отзывает **ТОЛЬКО те партиции**, которые передаются. Остальные продолжают обрабатываться.

---

### DEMO 3: STATIC membership

~~~text
[consumer-1] 🔧 Static membership: group.instance.id=consumer-1
[consumer-2] 🔧 Static membership: group.instance.id=consumer-2

[consumer-1] ✅ onPartitionsAssigned: [hw7-rebalance-topic-0, hw7-rebalance-topic-2]
[consumer-2] ✅ onPartitionsAssigned: [hw7-rebalance-topic-1]

... (остановка consumer-1) ...

[consumer-1] ⚠️  onPartitionsRevoked: [hw7-rebalance-topic-0, hw7-rebalance-topic-2]
[consumer-1] ✅ Consumer закрыт корректно
~~~

**Вывод:** Static membership **не вызывает rebalance** при перезапуске consumer с тем же `group.instance.id` (в пределах `session.timeout.ms`).

---

## 📝 Ответы на вопросы

### 1. Когда возникает rebalance?

**Rebalance** — это процесс перераспределения partitions между consumer'ами в одной consumer group.

**Когда возникает:**

| Причина | Пример |
|---------|--------|
| **Новый consumer** | Запустили 3-го consumer → partitions перераспределяются |
| **Consumer упал** | Consumer не отправляет heartbeat → исключается из группы |
| **Consumer остановлен** | Graceful shutdown → partitions передаются другим |
| **Изменение подписки** | Consumer подписался на новый топик |
| **Изменение partitions** | Добавили partitions в топик |
| **Превышен session.timeout.ms** | Consumer не отвечает → исключается |

**Проблема rebalance:**
- **Stop-the-world** (eager): все consumer'ы останавливаются
- **Долгая обработка**: пока идёт rebalance — сообщения не читаются
- **Дубли**: если offset не закоммичен — сообщения будут прочитаны повторно

---

### 2. Для чего нужен cooperative assignor?

**CooperativeStickyAssignor** — это assignor, который делает **инкрементальный rebalance**.

**Отличия от eager (RangeAssignor):**

| Аспект | EAGER (RangeAssignor) | COOPERATIVE (CooperativeStickyAssignor) |
|--------|----------------------|----------------------------------------|
| **Отзыв partitions** | ВСЕ партиции у ВСЕХ consumer'ов | ТОЛЬКО те, что передаются |
| **Остановка обработки** | Полная (stop-the-world) | Частичная |
| **Re balance** | Двойной (revoke → assign) | Одинарный (revoke moved → assign moved) |
| **Производительность** | Ниже | Выше |

**Как работает cooperative:**
1. Consumer A отзывает **только те партиции**, которые нужно передать Consumer B
2. Consumer B получает эти партиции
3. Consumer A продолжает обрабатывать **остальные** партиции

**Преимущества:**
- ✅ Меньше downtime
- ✅ Меньше дублей
- ✅ Плавный rebalance

**Когда использовать:**
- ✅ Production (рекомендуется)
- ✅ Большие consumer groups
- ✅ Высокая нагрузка

---

### 3. Какую проблему решает static membership?

**Static membership** (`group.instance.id`) решает проблему **частых rebalance при перезапуске consumer'ов**.

**Проблема без static membership:**

~~~text
1. Consumer запущен (dynamic membership)
2. Consumer падает / перезапускается
3. Kafka видит: "Consumer ушёл" → rebalance
4. Consumer возвращается через 5 секунд
5. Kafka видит: "Новый consumer" → rebalance снова

Результат: 2 rebalance за 5 секунд!
~~~

**С static membership:**

~~~text
1. Consumer запущен (group.instance.id=consumer-1)
2. Consumer падает / перезапускается
3. Kafka видит: "Consumer-1 временно недоступен" → ждёт session.timeout.ms
4. Consumer возвращается через 5 секунд (в пределах timeout)
5. Kafka видит: "Consumer-1 вернулся" → НЕТ rebalance!

Результат: 0 rebalance!
~~~

**Как работает:**
- Каждый consumer получает **уникальный** `group.instance.id`
- При перезапуске consumer с тем же ID **не происходит rebalance**
- Kafka ждёт `session.timeout.ms` перед исключением

**Преимущества:**
- ✅ Меньше rebalance
- ✅ Меньше дублей
- ✅ Стабильность группы

**Когда использовать:**
- ✅ Kubernetes (поды перезапускаются)
- ✅ Stateful приложения
- ✅ Consumer'ы с долгой инициализацией

**Ограничения:**
- ❌ `group.instance.id` должен быть **уникальным**
- ❌ Нужно **вручную** управлять ID (например, через StatefulSet в K8s)
- ❌ При превышении `session.timeout.ms` — rebalance всё равно произойдёт

---

### 4. Зачем consumer нужен graceful shutdown?

**Graceful shutdown** — это корректное завершение работы consumer'а с вызовом `consumer.close()`.

**Проблема без graceful shutdown:**

~~~text
1. Consumer читает сообщения
2. Приложение убито (kill -9)
3. Consumer не вызвал close()
4. Kafka ждёт session.timeout.ms (45 секунд по умолчанию)
5. Только потом — rebalance
6. 45 секунд: partitions не обрабатываются!
~~~

**С graceful shutdown:**

~~~text
1. Consumer читает сообщения
2. Получен сигнал shutdown (SIGTERM)
3. Consumer.wakeup() → прерывает poll()
4. Consumer.close() → отправляет LeaveGroup
5. Kafka сразу делает rebalance
6. 0 секунд: partitions переданы другим
~~~

**Как работает:**

~~~java
Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

public void shutdown() {
    running.set(false);
    consumer.wakeup();  // Прерывает poll()
}

// В цикле:
try {
    while (running.get()) {
        consumer.poll(...);
    }
} catch (WakeupException e) {
    // Ожидаемое исключение
} finally {
    consumer.close();  // Отзывает partitions + LeaveGroup
}
~~~

**Преимущества:**
- ✅ Мгновенный rebalance
- ✅ Нет простоя
- ✅ Корректный commit offset
- ✅ Меньше дублей

**Когда использовать:**
- ✅ Всегда в production
- ✅ Kubernetes (SIGTERM при остановке пода)
- ✅ Docker (docker stop)

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

- [Kafka Consumer Rebalance Protocol](https://kafka.apache.org/documentation/#consumerconfigs)
- [CooperativeStickyAssignor](https://kafka.apache.org/documentation/#upgrade_240_notable)
- [Static Membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances)
- [Graceful Shutdown](https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#wakeup())

---

## 👨‍🎓 Автор

**Имя:** Ilyas  
**Курс:** Otus "Администрирование платформы Apache Kafka"  
**Дата:** 2026-09-22

---

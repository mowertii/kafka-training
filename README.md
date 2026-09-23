# Kafka Training Course

Репозиторий для выполнения домашних заданий по курсу Kafka.

---

## Домашние задания

| # | Название | Ветка | Статус |
|---|----------|-------|--------|
| 1 | Basic Producer/Consumer | [feature/homework-1](https://github.com/mowertii/kafka-training/tree/feature/homework-1) | ✅ Готово |
| 2 | Reliable Delivery | [feature/homework-2](https://github.com/mowertii/kafka-training/tree/feature/homework-2) | ✅ Готово |
| 3 | Error Handling: Retry, DLT, Backoff | [feature/homework-3](https://github.com/mowertii/kafka-training/tree/feature/homework-3) | ✅ Готово |
| 4 | Idempotent consumer and inbox; replay and deduplication | [feature/homework-4](https://github.com/mowertii/kafka-training/tree/feature/homework-4) | ✅ Готово |
| 5 | Transactional Outbox + Failure Testing | [feature/homework-5](https://github.com/mowertii/kafka-training/tree/feature/homework-5) | ✅ Готово |
| 6 | Schema Registry и Avro, Monitoring | [feature/homework-6](https://github.com/mowertii/kafka-training/tree/feature/homework-6) | ✅ Готово |
| 7 | Rebalancing, static membership, cooperative assignor, graceful shutdown | [feature/homework-7](https://github.com/mowertii/kafka-training/tree/feature/homework-7) | ✅ Готово |
| 8 | Metrics, tracing/correlation ID, lag diagnostics | [feature/homework-8](https://github.com/mowertii/kafka-training/tree/feature/homework-8) | ✅ Готово |
| 9 | Kafka Streams: state store, repartition, restore, exactly-once-v2  | [feature/homework-9](https://github.com/mowertii/kafka-training/tree/feature/homework-9) | ✅ Готово |
---

## Как работать

```bash
# Клонируем репозиторий
git clone git@github.com:mowertii/kafka-training.git
cd kafka-training

# Переключаемся на нужное ДЗ
git checkout feature/homework-1

# Читаем README.md внутри ветки
# Запускаем проект
./start.cmd
./demo.cmd init
./demo.cmd producer
или одним файлом (если есть)
hw2.cmd, hw3.cmd etc

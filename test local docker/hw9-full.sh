#!/bin/bash
# ============================================================
# hw9-full.sh — запуск ДЗ №9 (Kafka Streams)
# ============================================================

set -e

echo "============================================================"
echo "HOMEWORK 9 - Kafka Streams: state store, repartition, EOS-v2"
echo "============================================================"
echo

echo "[1/5] Starting Kafka..."
docker compose -p kafka-training --profile hw9 up -d kafka
sleep 10

echo
echo "[2/5] Creating topics (orders, order-totals-output)..."
docker compose -p kafka-training --profile hw9 run --rm --build hw9-app init

echo
echo "[3/5] Producing 20 OrderCreated events..."
docker compose -p kafka-training --profile hw9 run --rm hw9-app produce

echo
echo "[4/5] Running Kafka Streams (30 seconds)..."
docker compose -p kafka-training --profile hw9 run --rm hw9-app streams

echo
echo "[5/5] Checking internal topics (repartition)..."
echo
docker exec kafka-training-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --list | grep -E "(orders|order-totals|hw9)" || true

echo
echo "============================================================"
echo "HOMEWORK 9 COMPLETED!"
echo "============================================================"
echo
echo "Expected:"
echo "  - Internal topics created (repartition, changelog)"
echo "  - State store contains total per userId"
echo "  - exactly_once_v2 enabled"
echo "  - State restored after restart"

exit 0

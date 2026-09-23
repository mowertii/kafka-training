#!/bin/bash
# ============================================================
# hw8-full.sh — запуск ДЗ №8 (Метрики, Correlation ID, Lag)
# ============================================================

set -e

echo "============================================================"
echo "HOMEWORK 8 - Metrics, Correlation ID, Consumer Lag"
echo "============================================================"
echo

echo "[1/5] Starting Kafka..."
docker compose -p kafka-training --profile hw8 up -d kafka
sleep 10

echo
echo "[2/5] Creating topic with 3 partitions..."
docker compose -p kafka-training --profile hw8 run --rm --build hw8-app init

echo
echo "[3/5] Producing 1000 messages with correlationId..."
docker compose -p kafka-training --profile hw8 run --rm hw8-app produce

echo
echo "[4/5] LAG DEMO: slow consumer → lag grows → speed up → lag shrinks"
docker compose -p kafka-training --profile hw8 run --rm hw8-app lag-demo

echo
echo "============================================================"
echo "HOMEWORK 8 COMPLETED!"
echo "============================================================"
echo
echo "Expected:"
echo "  - 1000 messages with correlationId in headers"
echo "  - Lag grows when consumer is slow (50ms delay)"
echo "  - Lag shrinks when delay is removed"
echo "  - Metrics: processed, errors, avg processing time"
echo "  - All logs contain correlationId"

exit 0

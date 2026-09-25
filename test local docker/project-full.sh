#!/bin/bash
# ============================================================
# project-full.sh — запуск финального проекта
# Outbox + Idempotent Consumer + Retry/DLT + Observability
# ============================================================

set -e

echo "============================================================"
echo "FINAL PROJECT - Outbox + Idempotent Consumer + Retry/DLT"
echo "============================================================"
echo

echo "[1/4] Starting infrastructure (Kafka + PostgreSQL)..."
docker compose -p kafka-training --profile project up -d kafka postgres
sleep 15

echo
echo "[2/4] Building project..."
docker compose -p kafka-training --profile project build project-app

echo
echo "[3/4] Initializing topics and DB tables..."
docker compose -p kafka-training --profile project run --rm project-app init

echo
echo "[4/4] Running FULL DEMO (orders + consumer + retry + DLT)..."
docker compose -p kafka-training --profile project run --rm project-app demo

echo
echo "============================================================"
echo "PROJECT COMPLETED!"
echo "============================================================"
echo
echo "Expected:"
echo "  - 5 orders processed successfully"
echo "  - 1 'bad' order sent to retry.1 → retry.2 → DLT"
echo "  - Inbox contains unique eventIds"
echo "  - Payments contains 5 payments"
echo "  - DLT contains 1 message"
echo
echo "Check DB:"
echo "  docker exec -it kafka-training-postgres psql -U demo -d kafkademo"
echo "  SELECT * FROM orders;"
echo "  SELECT * FROM payments;"
echo "  SELECT * FROM dlt_messages;"

exit 0

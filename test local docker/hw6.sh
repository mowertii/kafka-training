#!/bin/bash
# ============================================================
# hw6.sh — запуск ДЗ №6 (Schema Registry + Avro)
# ============================================================

set -e

echo "============================================================"
echo "HOMEWORK 6 - Schema Registry + Avro + Compatibility"
echo "============================================================"
echo

echo "[1/5] Starting infrastructure (Kafka + PostgreSQL + Schema Registry)..."
docker compose -p kafka-training --profile hw6 up -d kafka postgres schema-registry

echo
echo "Waiting for Schema Registry to be ready (may take 1-2 minutes)..."
sleep 90

echo
echo "[2/5] Registering schema V1 (orderId, userId)..."
docker compose -p kafka-training --profile hw6 run --rm --build hw6-app register-v1

echo
echo "[3/5] Registering schema V2 (compatible, +createdAt)..."
docker compose -p kafka-training --profile hw6 run --rm hw6-app register-v2

echo
echo "[4/5] Checking incompatible schema V3 (orderId: int -> string)..."
docker compose -p kafka-training --profile hw6 run --rm hw6-app check-v3

echo
echo "[5/5] Verifying via REST API:"
echo
echo "--- Subjects ---"
curl -s http://localhost:8081/subjects
echo
echo
echo "--- Versions ---"
curl -s http://localhost:8081/subjects/orders-value/versions
echo
echo
echo "--- Latest Schema ---"
curl -s http://localhost:8081/subjects/orders-value/versions/latest
echo
echo
echo "--- Compatibility Mode ---"
curl -s http://localhost:8081/config/orders-value
echo
echo
echo "============================================================"
echo "HOMEWORK 6 COMPLETED!"
echo "============================================================"
echo
echo "Expected:"
echo "  - V1 (schemaId=1) - registered"
echo "  - V2 (schemaId=2) - registered (compatible)"
echo "  - V3 - REJECTED (incompatible)"
echo
echo "Stop infrastructure:"
echo "  docker compose -p kafka-training down --remove-orphans"

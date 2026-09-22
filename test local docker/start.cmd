@echo off
setlocal
echo ============================================================
echo Starting Kafka + PostgreSQL infrastructure...
echo ============================================================
docker compose -p kafka-training up -d kafka postgres
echo.
echo Waiting for services to be healthy...
ping 127.0.0.1 -n 11 >nul
docker compose -p kafka-training ps
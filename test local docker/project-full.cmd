@echo off
setlocal

echo ============================================================
echo FINAL PROJECT - Outbox + Idempotent Consumer + Retry/DLT
echo ============================================================
echo.

echo [1/4] Starting infrastructure (Kafka + PostgreSQL)...
docker compose -p kafka-training --profile project up -d kafka postgres
ping 127.0.0.1 -n 16 >nul

echo.
echo [2/4] Building project...
docker compose -p kafka-training --profile project build project-app
if errorlevel 1 exit /b %errorlevel%

echo.
echo [3/4] Initializing topics and DB tables...
docker compose -p kafka-training --profile project run --rm project-app init
if errorlevel 1 exit /b %errorlevel%

echo.
echo [4/4] Running FULL DEMO (orders + consumer + retry + DLT)...
docker compose -p kafka-training --profile project run --rm project-app demo
if errorlevel 1 exit /b %errorlevel%

echo.
echo ============================================================
echo PROJECT COMPLETED!
echo ============================================================
echo.
echo Expected:
echo   - 5 orders processed successfully
echo   - 1 'bad' order sent to retry.1 -^> retry.2 -^> DLT
echo   - Inbox contains unique eventIds
echo   - Payments contains 5 payments
echo   - DLT contains 1 message
echo.
echo Check DB:
echo   docker exec -it kafka-training-postgres psql -U demo -d kafkademo
echo   SELECT * FROM orders;
echo   SELECT * FROM payments;
echo   SELECT * FROM dlt_messages;

exit /b 0

@echo off
setlocal

echo ============================================================
echo HOMEWORK 8 - Metrics, Correlation ID, Consumer Lag
echo ============================================================
echo.

echo [1/5] Starting Kafka...
docker compose -p kafka-training --profile hw8 up -d kafka
ping 127.0.0.1 -n 11 >nul

echo.
echo [2/5] Creating topic with 3 partitions...
docker compose -p kafka-training --profile hw8 run --rm --build hw8-app init
if errorlevel 1 exit /b %errorlevel%

echo.
echo [3/5] Producing 1000 messages with correlationId...
docker compose -p kafka-training --profile hw8 run --rm hw8-app produce
if errorlevel 1 exit /b %errorlevel%

echo.
echo [4/5] LAG DEMO: slow consumer -^> lag grows -^> speed up -^> lag shrinks
docker compose -p kafka-training --profile hw8 run --rm hw8-app lag-demo
if errorlevel 1 exit /b %errorlevel%

echo.
echo ============================================================
echo HOMEWORK 8 COMPLETED!
echo ============================================================
echo.
echo Expected:
echo   - 1000 messages with correlationId in headers
echo   - Lag grows when consumer is slow (50ms delay)
echo   - Lag shrinks when delay is removed
echo   - Metrics: processed, errors, avg processing time
echo   - All logs contain correlationId

exit /b 0

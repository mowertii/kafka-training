@echo off
setlocal

echo ============================================================
echo HOMEWORK 7 - FULL DEMO
echo Rebalancing + Cooperative Assignor + Static Membership
echo ============================================================
echo.

echo [1/6] Starting infrastructure...
docker compose -p kafka-training --profile hw7 up -d kafka
ping 127.0.0.1 -n 6 >nul

echo.
echo [2/6] Creating topic (3 partitions)...
docker compose -p kafka-training --profile hw7 run --rm --build hw7-app init
if errorlevel 1 exit /b %errorlevel%

echo.
echo [3/6] Producing 30 messages...
docker compose -p kafka-training --profile hw7 run --rm hw7-app produce
if errorlevel 1 exit /b %errorlevel%

echo.
echo [4/6] DEMO 1: Rebalance with EAGER assignor (RangeAssignor)...
echo       Watch for: onPartitionsRevoked + onPartitionsAssigned (ALL partitions)
docker compose -p kafka-training --profile hw7 run --rm hw7-app demo-eager
if errorlevel 1 exit /b %errorlevel%

echo.
echo [5/6] DEMO 2: Rebalance with COOPERATIVE assignor...
echo       Watch for: only MOVED partitions are revoked (incremental)
docker compose -p kafka-training --profile hw7 run --rm hw7-app demo-cooperative
if errorlevel 1 exit /b %errorlevel%

echo.
echo [6/6] DEMO 3: STATIC MEMBERSHIP...
echo       Watch for: no rebalance on restart (same group.instance.id)
docker compose -p kafka-training --profile hw7 run --rm hw7-app demo-static
if errorlevel 1 exit /b %errorlevel%

echo.
echo ============================================================
echo HOMEWORK 7 COMPLETED!
echo ============================================================
echo.
echo Expected:
echo   - EAGER: ALL partitions revoked on rebalance
echo   - COOPERATIVE: only MOVED partitions revoked (incremental)
echo   - STATIC: no rebalance on restart (same group.instance.id)
echo   - GRACEFUL SHUTDOWN: consumer.close() called on shutdown

exit /b 0

@echo off
setlocal

if "%~1"=="" (
    echo Usage: hw7.cmd ^<mode^>
    echo Modes:
    echo   init              - create topic with 3 partitions
    echo   produce           - send 30 messages
    echo   consumer-eager    - run consumer with eager assignor
    echo   consumer-cooperative - run consumer with cooperative assignor
    echo   consumer-static   - run consumer with static membership
    echo   demo-eager        - demo rebalance (eager)
    echo   demo-cooperative  - demo rebalance (cooperative)
    echo   demo-static       - demo rebalance (static membership)
    exit /b 2
)

echo ============================================================
echo HOMEWORK 7 - Rebalancing, Static Membership, Cooperative Assignor
echo Mode: %~1
echo ============================================================
echo.

echo [1/2] Ensuring infrastructure is up...
docker compose -p kafka-training --profile hw7 up -d kafka
ping 127.0.0.1 -n 6 >nul

echo.
echo [2/2] Running: %~1
docker compose -p kafka-training --profile hw7 run --rm --build hw7-app %~1 %2 %3

exit /b %errorlevel%

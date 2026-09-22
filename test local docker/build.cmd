@echo off
setlocal
echo ============================================================
echo Building all modules via Docker...
echo ============================================================
docker compose -p kafka-training --profile hw6 build hw6-app
if errorlevel 1 exit /b %errorlevel%
echo.
echo Build complete!
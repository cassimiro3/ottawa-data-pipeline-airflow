@echo off
setlocal enabledelayedexpansion

echo ===========================================
echo   RESET AIRFLOW ENVIRONMENT
echo ===========================================

:: 1. Stop and remove containers
echo Stopping and removing existing containers...
docker-compose down --volumes --remove-orphans

:: 2. Clean local folders
echo Removing logs and temporary folders...
if exist logs rmdir /s /q logs
if exist airflow_home rmdir /s /q airflow_home
if exist .docker\localstack rmdir /s /q .docker\localstack

echo Recreating required directories...
mkdir logs
mkdir .docker\localstack

:: 3. Clean Docker environment
echo Cleaning unused Docker resources...
docker system prune -f >nul
docker volume prune -f >nul

:: 4. Start containers
echo Starting containers...
docker-compose up -d

:: 5. Wait for Postgres to become healthy
echo Waiting for Postgres to become healthy...
set "PG_CONTAINER=ottawa-data-pipeline-airflow-postgres-1"

:wait_pg
for /f "tokens=*" %%i in ('docker inspect --format "{{.State.Health.Status}}" %PG_CONTAINER% 2^>nul') do (
    set "STATUS=%%i"
)
if "%STATUS%"=="healthy" (
    echo Postgres is ready.
) else (
    echo Still waiting for Postgres...
    timeout /t 3 >nul
    goto wait_pg
)

:: 6. Initialize Airflow database
echo ===========================================
echo   Initializing Airflow database
echo ===========================================
docker-compose run airflow-webserver airflow db migrate

:: 7. Create Airflow admin user
echo ===========================================
echo   Creating default Airflow admin user
echo ===========================================
docker-compose run airflow-webserver airflow users create ^
    --username airflow ^
    --password airflow ^
    --firstname Admin ^
    --lastname User ^
    --role Admin ^
    --email admin@example.com

:: 8. Restart containers cleanly
echo Restarting Airflow containers...
docker-compose down
docker-compose up -d

:: 9. Wait for Airflow webserver to become healthy
echo Waiting for Airflow webserver to become healthy...
set "WEB_CONTAINER=ottawa-data-pipeline-airflow-airflow-webserver-1"

:wait_web
for /f "tokens=*" %%i in ('docker inspect --format "{{.State.Health.Status}}" %WEB_CONTAINER% 2^>nul') do (
    set "STATUS=%%i"
)
if "%STATUS%"=="healthy" (
    echo Airflow webserver is ready.
) else (
    echo Still waiting for Airflow webserver...
    timeout /t 5 >nul
    goto wait_web
)

:: 10. Final confirmation
echo ===========================================
echo   RESET COMPLETE
echo Airflow web UI available at: http://localhost:8080
echo Username: airflow / Password: airflow
echo ===========================================

pause

#!/bin/bash

set -e

echo "==========================================="
echo "  RESET AIRFLOW ENVIRONMENT (Linux/macOS)"
echo "==========================================="

# 1. Stop and remove containers
echo "Stopping and removing existing containers..."
docker compose down --volumes --remove-orphans

# 2. Clean local folders
echo "Removing logs and temporary folders..."
rm -rf logs airflow_home .docker/localstack

echo "Recreating required directories..."
mkdir -p logs .docker/localstack

# 3. Clean Docker environment
echo "Cleaning unused Docker resources..."
docker system prune -f >/dev/null
docker volume prune -f >/dev/null

# 4. Start containers
echo "Starting containers..."
docker compose up -d

# 5. Wait for Postgres to become healthy
PG_CONTAINER="ottawa-data-pipeline-airflow-postgres-1"
echo "Waiting for Postgres to become healthy..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' "$PG_CONTAINER" 2>/dev/null)" == "healthy" ]; do
  echo "Still waiting for Postgres..."
  sleep 3
done
echo "Postgres is ready."

# 6. Initialize Airflow database
echo "==========================================="
echo "  Initializing Airflow database"
echo "==========================================="
docker compose run airflow-webserver airflow db migrate

# 7. Create Airflow admin user
echo "==========================================="
echo "  Creating default Airflow admin user"
echo "==========================================="
docker compose run airflow-webserver airflow users create \
  --username airflow \
  --password airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# 8. Restart containers cleanly
echo "Restarting Airflow containers..."
docker compose down
docker compose up -d

# 9. Wait for Airflow webserver to become healthy
WEB_CONTAINER="ottawa-data-pipeline-airflow-airflow-webserver-1"
echo "Waiting for Airflow webserver to become healthy..."
until [ "$(docker inspect --format='{{.State.Health.Status}}' "$WEB_CONTAINER" 2>/dev/null)" == "healthy" ]; do
  echo "Still waiting for Airflow webserver..."
  sleep 5
done
echo "Airflow webserver is ready."

# 10. Final confirmation
echo "==========================================="
echo "  RESET COMPLETE"
echo "Airflow web UI available at: http://localhost:8080"
echo "Username: airflow / Password: airflow"
echo "==========================================="

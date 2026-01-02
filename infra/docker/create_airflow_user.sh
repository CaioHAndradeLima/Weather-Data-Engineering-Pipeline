#!/bin/bash
set -e

echo "Initializing Airflow metadata database"
echo "Creating admin user (admin / admin)"

docker compose up --abort-on-container-exit airflow-init

echo "Airflow initialization completed successfully"

#!/bin/bash
set -e

# Initialise the Airflow metadata database.
airflow db migrate

# Create the admin user with a fixed password if it doesn't exist yet.
# This runs on every startup but is idempotent — it skips creation if the
# user already exists.
airflow users create \
  --username admin \
  --password test \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com 2>/dev/null || true

# Start Airflow (webserver + scheduler + triggerer in one process).
exec airflow standalone

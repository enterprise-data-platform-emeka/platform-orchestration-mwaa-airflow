#!/bin/bash
set -e

# Initialise the Airflow metadata database.
airflow db migrate

# Create the admin user with a fixed password.
# '|| true' makes this a no-op if the user already exists.
airflow users create \
  --username admin \
  --password test \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com 2>/dev/null || true

# Start scheduler and triggerer in the background, then run the webserver
# in the foreground so Docker tracks its process for health checks.
airflow scheduler &
airflow triggerer &
exec airflow webserver --port 8080

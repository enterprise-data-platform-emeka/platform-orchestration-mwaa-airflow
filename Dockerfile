FROM apache/airflow:2.9.2-python3.11

# Install project dependencies using the official MWAA 2.9.2 constraints file.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt"

# entrypoint.sh initialises the DB, creates the admin user with a fixed
# password, then starts Airflow standalone.
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

FROM apache/airflow:2.9.2-python3.11

# Install project dependencies using the official MWAA 2.9.2 constraints file.
# The constraints file pins every transitive dependency to versions that MWAA
# uses in production, so the local environment matches production as closely
# as possible without needing the full aws-mwaa-local-runner build.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt"

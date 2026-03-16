# plugins/__init__.py
#
# MWAA (Amazon Managed Workflows for Apache Airflow) requires the plugins/
# directory to exist and be importable. This file makes it a valid Python
# package. Custom Airflow plugins (hooks, operators, sensors) live here when
# needed — for now the EDP pipeline only uses providers from requirements.txt.

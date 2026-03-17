"""
edp_pipeline.py — Enterprise Data Platform (EDP) main orchestration DAG.

This DAG runs the full Silver and Gold pipeline on a daily schedule:

  1. Silver layer (parallel): Six AWS Glue PySpark jobs transform raw Bronze
     Parquet files from S3 (Simple Storage Service) into cleaned star-schema
     tables in Silver S3. All six jobs run in parallel because they read from
     independent Bronze partitions.

  2. Silver complete (join): An EmptyOperator acts as a synchronisation point
     that waits for all six Silver jobs to succeed before continuing.

  3. Gold layer (sequential): dbt (data build tool) runs against Athena to
     produce aggregate Gold tables, then dbt test validates them. These run
     sequentially because dbt test depends on the models dbt run produces.

  4. Pipeline complete: A final EmptyOperator marks the successful end of
     the pipeline so downstream sensors can attach to it cleanly.

Airflow Variables required (set via Admin → Variables in the Airflow UI):
  mwaa_env       — one of: dev, staging, prod
  aws_account_id — 12-digit AWS account ID, e.g. 158311564771

Schedule: 06:00 UTC daily (Bronze CDC data lands overnight from DMS (Database
Migration Service), so 06:00 gives DMS time to finish the nightly batch).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
# retries=1 with a 5-minute delay handles the most common transient failures
# (throttling, momentary AWS API hiccups) without piling up retries.
# email_on_failure=False keeps the MWAA environment clean — alerting is
# handled by the AI Operations Agent via CloudWatch EventBridge instead.

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="edp_pipeline",
    description="EDP daily Silver + Gold pipeline (Glue → dbt/Athena)",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["edp", "silver", "gold"],
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # Read runtime config from Airflow Variables.
    # Variables are resolved at parse time here so that the Glue job names
    # and dbt target are baked into each task definition — this is the
    # standard pattern for MWAA 2.9.2 (Airflow 2.9.2).
    # -----------------------------------------------------------------------

    # mwaa_env drives the naming convention throughout the platform:
    #   Glue job names: edp-{mwaa_env}-{job_name}
    #   S3 bucket names: edp-{mwaa_env}-{account_id}-{type}
    #   dbt target:      matches mwaa_env (dev / staging / prod profile in
    #                    profiles.yml inside the dbt project)
    mwaa_env = Variable.get("mwaa_env", default_var="dev")

    # The six Silver Glue jobs. Order here does not matter — they run in
    # parallel. The names must match what is deployed by platform-glue-jobs.
    silver_job_names = [
        "dim_customer",
        "dim_product",
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_shipments",
    ]

    # dbt project is mounted at this path inside the MWAA / local runner
    # container. In production MWAA the dbt project is bundled into the
    # plugins.zip that is uploaded to S3 (Simple Storage Service).
    dbt_project_path = "/opt/airflow/dbt/platform-dbt-analytics"

    # -----------------------------------------------------------------------
    # Group 1 — Silver (parallel Glue jobs)
    # -----------------------------------------------------------------------
    # GlueJobOperator submits an existing Glue job and, with
    # wait_for_completion=True, polls until the job reaches a terminal state
    # (SUCCEEDED, FAILED, STOPPED). If the job fails, Airflow marks the task
    # as failed and the retry logic in default_args kicks in.
    #
    # num_of_dpus=2 is the minimum for G.1X workers (2 DPU (Data Processing
    # Unit) = 2 workers). Adjust per-job in staging/prod if needed.
    #
    # aws_conn_id defaults to "aws_default" — configure the MWAA connection
    # in Admin → Connections to point at the correct IAM (Identity and Access
    # Management) role for your environment.

    silver_tasks = []
    for job_name in silver_job_names:
        task = GlueJobOperator(
            task_id=f"silver_{job_name}",
            job_name=f"edp-{mwaa_env}-{job_name}",
            wait_for_completion=True,
            verbose=True,
            num_of_dpus=2,
            # aws_conn_id uses the MWAA execution role when running in
            # production — no explicit connection ID needed if the MWAA
            # environment IAM role has glue:StartJobRun permission.
            aws_conn_id="aws_default",
        )
        silver_tasks.append(task)

    # -----------------------------------------------------------------------
    # Synchronisation point — silver_complete
    # -----------------------------------------------------------------------
    # All six Silver tasks must succeed before dbt runs. Without this join
    # point, gold_dbt_run would have six upstream dependencies listed inline,
    # which makes the graph harder to read and harder to extend later.

    silver_complete = EmptyOperator(task_id="silver_complete")

    # -----------------------------------------------------------------------
    # Group 2 — Gold (dbt run then dbt test, sequential)
    # -----------------------------------------------------------------------
    # dbt runs inside the Airflow worker container via BashOperator. In the
    # local runner, dbt is installed in the worker image. In production MWAA,
    # dbt is available because it is included in requirements.txt and MWAA
    # installs it into the worker environment on startup.
    #
    # DBT_TARGET maps directly to a dbt profile target so the same DAG can
    # deploy to dev, staging, and prod without code changes — just change
    # the mwaa_env Airflow Variable.

    # dbt is installed at /home/airflow/.local/bin/dbt but that path is not
    # in the PATH used by BashOperator. Use the full path explicitly.
    dbt_bin = "/home/airflow/.local/bin/dbt"

    aws_account_id = Variable.get("aws_account_id", default_var="158311564771")
    athena_results_bucket = f"edp-{mwaa_env}-{aws_account_id}-athena-results"

    dbt_env = {
        "DBT_TARGET": mwaa_env,
        "ATHENA_RESULTS_BUCKET": athena_results_bucket,
        "ATHENA_WORKGROUP": f"edp-{mwaa_env}-workgroup",
        "DBT_ATHENA_SCHEMA": f"edp_{mwaa_env}_gold",
    }

    gold_dbt_run = BashOperator(
        task_id="gold_dbt_run",
        bash_command=(
            f"cd {dbt_project_path} && "
            f"{dbt_bin} run --target {mwaa_env} --profiles-dir {dbt_project_path}/profiles --no-use-colors"
        ),
        env=dbt_env,
    )

    gold_dbt_test = BashOperator(
        task_id="gold_dbt_test",
        bash_command=(
            f"cd {dbt_project_path} && "
            f"{dbt_bin} test --target {mwaa_env} --profiles-dir {dbt_project_path}/profiles --no-use-colors"
        ),
        env=dbt_env,
    )

    # -----------------------------------------------------------------------
    # Pipeline end marker
    # -----------------------------------------------------------------------
    # Downstream sensors or notification tasks can depend on pipeline_complete
    # rather than on gold_dbt_test directly. This keeps the dependency graph
    # clean if we later add parallel post-processing tasks before the marker.

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    # -----------------------------------------------------------------------
    # Dependency chain
    # -----------------------------------------------------------------------
    # [silver_dim_customer, silver_dim_product, ...] >> silver_complete
    #   >> gold_dbt_run >> gold_dbt_test >> pipeline_complete

    silver_tasks >> silver_complete
    silver_complete >> gold_dbt_run >> gold_dbt_test >> pipeline_complete

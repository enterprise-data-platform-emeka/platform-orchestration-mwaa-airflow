"""
edp_pipeline.py: Enterprise Data Platform (EDP) main orchestration DAG.

This DAG runs the full Silver and Gold pipeline on a daily schedule:

  1. Silver layer (parallel): Six AWS Glue PySpark jobs transform raw Bronze
     Parquet files from S3 (Simple Storage Service) into cleaned star-schema
     tables in Silver S3. All six jobs run in parallel because they read from
     independent Bronze partitions.

  2. Silver complete (join): An EmptyOperator acts as a synchronisation point
     that waits for all six Silver jobs to succeed before continuing.

  3. Glue Crawler: After all Silver jobs succeed, the Glue Crawler scans the
     Silver S3 bucket and registers (or updates) table schemas in the Glue
     Data Catalog. dbt cannot query Silver tables via Athena until the catalog
     knows they exist.

  4. Gold layer (sequential): dbt (data build tool) runs against Athena to
     produce aggregate Gold tables, then dbt test validates them. These run
     sequentially because dbt test depends on the models dbt run produces.

     In MWAA, the dbt project is downloaded from S3 at the start of gold_dbt_run.
     The platform-dbt-analytics deploy workflow syncs the project to
     s3://{mwaa-bucket}/dbt/platform-dbt-analytics/ on every push. This means
     dbt model changes take effect on the next DAG run with no MWAA environment
     update needed.

  5. Pipeline complete: A final EmptyOperator marks the successful end of
     the pipeline so downstream sensors can attach to it cleanly.

Airflow Variables required (set via Admin → Variables in the Airflow UI):
  mwaa_env: one of dev, staging, prod
  aws_account_id: 12-digit AWS account ID from your AWS account

Schedule: 06:00 UTC daily (Bronze CDC data lands overnight from DMS (Database
Migration Service), so 06:00 gives DMS time to finish the nightly batch).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
# retries=1 with a 5-minute delay handles the most common transient failures
# (throttling, momentary AWS API hiccups) without piling up retries.
# email_on_failure=False keeps the MWAA environment clean, alerting is
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
    # and dbt target are baked into each task definition. This is the
    # standard pattern for MWAA 2.9.2 (Airflow 2.9.2).
    # -----------------------------------------------------------------------

    # mwaa_env drives the naming convention throughout the platform:
    #   Glue job names: edp-{mwaa_env}-{job_name}
    #   S3 bucket names: edp-{mwaa_env}-{account_id}-{type}
    #   dbt target:      matches mwaa_env (dev / staging / prod profile in
    #                    profiles.yml inside the dbt project)
    mwaa_env = Variable.get("mwaa_env", default_var="dev")

    # The six Silver Glue jobs. Order here does not matter, they run in
    # parallel. The names must match what is deployed by platform-glue-jobs.
    silver_job_names = [
        "dim_customer",
        "dim_product",
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_shipments",
    ]

    # Detect whether we are running in MWAA or the local Docker runner by
    # checking AIRFLOW_HOME. MWAA sets it to /usr/local/airflow; the local
    # Docker image sets it to /opt/airflow.
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    is_mwaa = airflow_home == "/usr/local/airflow"

    # -----------------------------------------------------------------------
    # Group 1: Silver (parallel Glue jobs)
    # -----------------------------------------------------------------------
    # GlueJobOperator submits an existing Glue job and, with
    # wait_for_completion=True, polls until the job reaches a terminal state
    # (SUCCEEDED, FAILED, STOPPED). If the job fails, Airflow marks the task
    # as failed and the retry logic in default_args kicks in.
    #
    # aws_conn_id defaults to "aws_default". Configure the MWAA connection
    # in Admin → Connections to point at the correct IAM (Identity and Access
    # Management) role for your environment.

    silver_tasks = []
    for job_name in silver_job_names:
        task = GlueJobOperator(
            task_id=f"silver_{job_name}",
            job_name=f"edp-{mwaa_env}-{job_name}",
            wait_for_completion=True,
            # verbose=False: verbose=True streams Glue logs from /aws-glue/jobs via
            # CloudWatch, which requires logs:GetLogEvents on that log group. The MWAA
            # execution role only has CloudWatch access for airflow-edp-dev-mwaa-* log
            # groups, so verbose=True causes an immediate AccessDenied that kills the task.
            verbose=False,
            aws_conn_id="aws_default",
        )
        silver_tasks.append(task)

    # -----------------------------------------------------------------------
    # Synchronisation point: silver_complete
    # -----------------------------------------------------------------------
    # All six Silver tasks must succeed before dbt runs. Without this join
    # point, gold_dbt_run would have six upstream dependencies listed inline,
    # which makes the graph harder to read and harder to extend later.

    silver_complete = EmptyOperator(task_id="silver_complete")

    # -----------------------------------------------------------------------
    # Group 1b: Silver Crawler
    # -----------------------------------------------------------------------
    # GlueCrawlerOperator starts the crawler and waits for it to finish.
    # The crawler scans the Silver S3 bucket and registers each Parquet
    # partition as a table in the Glue Data Catalog (edp_{env}_silver
    # database). Without this step, dbt cannot see the Silver tables via
    # Athena even though the Parquet files exist in S3.

    run_silver_crawler = GlueCrawlerOperator(
        task_id="run_silver_crawler",
        config={"Name": f"edp-{mwaa_env}-silver-crawler"},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # -----------------------------------------------------------------------
    # Group 2: Gold (dbt run then dbt test, sequential)
    # -----------------------------------------------------------------------
    # dbt runs inside the Airflow worker container via BashOperator. In the
    # local runner, dbt is installed in the worker image. In production MWAA,
    # dbt is available because it is included in requirements.txt and MWAA
    # installs it into the worker environment on startup.
    #
    # DBT_TARGET maps directly to a dbt profile target so the same DAG can
    # deploy to dev, staging, and prod without code changes. Just change
    # the mwaa_env Airflow Variable.

    aws_account_id = Variable.get("aws_account_id")
    athena_results_bucket = f"edp-{mwaa_env}-{aws_account_id}-athena-results"

    # append_env=True merges these vars into the worker's full environment rather
    # than replacing it. Without this, PATH, HOME, and PYTHONPATH are stripped,
    # and dbt's Python entry point cannot find its installed packages.
    # AWS credentials are not included here. The MWAA execution role provides
    # them automatically via the instance metadata service.
    dbt_env = {
        "DBT_TARGET": mwaa_env,
        "ATHENA_RESULTS_BUCKET": athena_results_bucket,
        "ATHENA_WORKGROUP": f"edp-{mwaa_env}-workgroup",
        "DBT_ATHENA_SCHEMA": f"edp_{mwaa_env}_gold",
        "AWS_DEFAULT_REGION": "eu-central-1",
    }

    dbt_bin = f"{os.environ.get('HOME', '/home/airflow')}/.local/bin/dbt"
    dbt_run_path = "/tmp/dbt_workspace"  # nosec B108 - /tmp is the only writable path in MWAA
    dbt_profiles_path = f"{dbt_run_path}/profiles"

    # How the dbt project reaches the worker differs between MWAA and the local runner.
    #
    # In MWAA: the platform-dbt-analytics deploy workflow syncs the project to
    #   s3://{mwaa_bucket}/dbt/platform-dbt-analytics/ on every push. The worker
    #   downloads it at task runtime via aws s3 sync. No plugins.zip involved, no
    #   MWAA environment update needed when dbt models change.
    #
    # Locally: the project is mounted as a Docker volume at
    #   {airflow_home}/dbt/platform-dbt-analytics. Copied to a writable path because
    #   the volume mount may be read-only and dbt needs to write target/ and logs/.
    mwaa_bucket = f"edp-{mwaa_env}-{aws_account_id}-mwaa-dags"

    if is_mwaa:
        dbt_setup_cmd = (
            f"rm -rf {dbt_run_path} && "
            f"aws s3 sync s3://{mwaa_bucket}/dbt/platform-dbt-analytics/ {dbt_run_path}/"
        )
    else:
        dbt_local_path = f"{airflow_home}/dbt/platform-dbt-analytics"
        dbt_setup_cmd = (
            f"rm -rf {dbt_run_path} && "
            f"cp -r {dbt_local_path} {dbt_run_path}"
        )

    gold_dbt_run = BashOperator(
        task_id="gold_dbt_run",
        bash_command=(
            f"{dbt_setup_cmd} && "
            f"cd {dbt_run_path} && "
            f"{dbt_bin} deps --target {mwaa_env} --profiles-dir {dbt_profiles_path} --no-use-colors && "
            f"{dbt_bin} run --target {mwaa_env} --profiles-dir {dbt_profiles_path} --no-use-colors"
        ),
        env=dbt_env,
        append_env=True,
    )

    gold_dbt_test = BashOperator(
        task_id="gold_dbt_test",
        bash_command=(
            f"cd {dbt_run_path} && "
            f"{dbt_bin} test --target {mwaa_env} --profiles-dir {dbt_profiles_path} --no-use-colors"
        ),
        env=dbt_env,
        append_env=True,
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
    #   >> run_silver_crawler >> gold_dbt_run >> gold_dbt_test >> pipeline_complete

    silver_tasks >> silver_complete
    silver_complete >> run_silver_crawler >> gold_dbt_run >> gold_dbt_test >> pipeline_complete

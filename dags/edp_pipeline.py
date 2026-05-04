"""
edp_pipeline.py: Enterprise Data Platform (EDP) main orchestration DAG.

This DAG runs the full Silver and Gold pipeline on a daily schedule:

  1. Silver layer (parallel): Six AWS Glue PySpark jobs transform raw Bronze
     Parquet files from S3 (Simple Storage Service) into cleaned star-schema
     tables in Silver S3. All six jobs run in parallel because they read from
     independent Bronze partitions.

  2. Silver complete (join): An EmptyOperator acts as a synchronisation point
     that waits for all six Silver jobs to succeed before continuing.

  3. Silver row count validation: A PythonOperator reads the SilverRowCount
     CloudWatch metric published by each Glue job and fails the DAG if any
     table wrote zero rows. This catches silent data loss without scanning S3.

  4. Glue Crawler: After validation, the Glue Crawler scans the Silver S3
     bucket and registers (or updates) table schemas in the Glue Data Catalog.
     dbt cannot query Silver tables via Athena until the catalog knows they
     exist.

  5. dbt source freshness gate: Runs dbt test --select source:silver to check
     that each Silver source table contains data within the expected freshness
     window. If Silver is stale, the DAG fails here rather than producing stale
     Gold data.

  6. Gold layer (sequential): dbt (data build tool) runs against Athena to
     produce aggregate Gold tables.

     In MWAA, the dbt project is downloaded from S3 at the start of each dbt
     task. The platform-dbt-analytics deploy workflow syncs the project to
     s3://{mwaa-bucket}/dbt/platform-dbt-analytics/ on every push.

  7. Gold row count validation: After dbt run completes, a PythonOperator
     reads staging model row counts from run_results.json and compares them to
     the Silver CloudWatch row count metrics. Any staging model that diverges
     from its Silver source by more than 5% fails the DAG before dbt test runs.
     Uses CloudWatch and the dbt artifact file, not Athena COUNT(*) scans.

  8. dbt test: Validates Gold model quality (unique keys, accepted values,
     custom assertions). Runs after the row count gate passes.

  9. Pipeline complete: A final EmptyOperator marks the successful end of the
     pipeline so downstream sensors can attach to it cleanly.

Airflow Variables required (set via Admin → Variables in the Airflow UI):
  mwaa_env: one of dev, staging, prod
  aws_account_id: 12-digit AWS account ID from your AWS account

Schedule: 06:00 UTC daily (Bronze CDC data lands overnight from DMS (Database
Migration Service), so 06:00 gives DMS time to finish the nightly batch).
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
# retries=1 with a 5-minute delay handles the most common transient failures
# (throttling, momentary AWS API hiccups) without piling up retries.
# email_on_failure=False keeps the MWAA environment clean; alerting is
# handled by CloudWatch alarms in the monitoring module.

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Module-level constant so callable functions below can reference it without
# being passed it explicitly (it never varies between environments).
_DBT_RUN_PATH = "/tmp/dbt_workspace"  # nosec B108 - /tmp is the only writable path in MWAA

# Silver table names and their corresponding dbt staging model names.
# Used by both validate tasks so defined once here.
_SILVER_TO_STAGING = {
    "dim_customer": "stg_customers",
    "dim_product": "stg_products",
    "fact_orders": "stg_orders",
    "fact_order_items": "stg_order_items",
    "fact_payments": "stg_payments",
    "fact_shipments": "stg_shipments",
}

# ---------------------------------------------------------------------------
# Validation callables (module-level so Airflow can serialise them)
# ---------------------------------------------------------------------------


def _validate_silver_row_counts(env: str, **context) -> None:
    """
    Read the SilverRowCount CloudWatch metric published by each Glue job in
    this run and fail if any table wrote zero rows.

    Uses a 2-hour lookback window so the metric is guaranteed to be present
    even if CloudWatch propagation is slightly delayed. Does not scan S3 or
    Athena — the count comes from the Glue job's in-memory DataFrame.count()
    call during the write phase.

    Sleeps 90 seconds before querying. CloudWatch custom metrics published via
    put_metric_data take approximately 60-90 seconds to become queryable via
    GetMetricStatistics. Airflow's task scheduling adds some natural delay but
    not reliably enough to skip this wait.
    """
    import boto3

    time.sleep(90)

    cw = boto3.client("cloudwatch", region_name="eu-central-1")
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=2)

    missing = []
    zero_rows = []

    for table in _SILVER_TO_STAGING:
        resp = cw.get_metric_statistics(
            Namespace="EDP/DataQuality",
            MetricName="SilverRowCount",
            Dimensions=[
                {"Name": "Table", "Value": table},
                {"Name": "Environment", "Value": env},
            ],
            StartTime=start,
            EndTime=now,
            Period=7200,
            Statistics=["Maximum"],
        )
        datapoints = resp.get("Datapoints", [])
        if not datapoints:
            missing.append(table)
            continue
        row_count = int(datapoints[0]["Maximum"])
        print(f"[silver-row-count] {table}: {row_count:,} rows")
        if row_count == 0:
            zero_rows.append(table)

    failures = (
        [f"{t}: no metric published (Glue job may not have completed)" for t in missing]
        + [f"{t}: 0 rows written to Silver" for t in zero_rows]
    )
    if failures:
        raise ValueError(f"Silver row count validation failed — {'; '.join(failures)}")


def _validate_gold_row_counts(env: str, **context) -> None:
    """
    Compare Gold staging model row counts (from dbt's run_results.json) against
    Silver row counts (from CloudWatch metrics published by Glue jobs). Fails
    if any staging model diverges from its Silver source by more than 5%.

    Why 5%: the Glue validation step quarantines rows that fail business rules
    (nulls, type mismatches). A small number of Silver rows being quarantined
    before dbt sees them is expected. More than 5% loss indicates a join
    condition problem or a dbt model bug.

    Falls back gracefully if dbt-athena reports rows_affected=-1 for all models
    (some adapter versions do not populate this field for CTAS queries). In that
    case the check is skipped and a warning is logged rather than failing the DAG.
    """
    import boto3

    # ── Silver counts from CloudWatch ─────────────────────────────────────────
    cw = boto3.client("cloudwatch", region_name="eu-central-1")
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=2)

    silver_counts: dict[str, int | None] = {}
    for table in _SILVER_TO_STAGING:
        resp = cw.get_metric_statistics(
            Namespace="EDP/DataQuality",
            MetricName="SilverRowCount",
            Dimensions=[
                {"Name": "Table", "Value": table},
                {"Name": "Environment", "Value": env},
            ],
            StartTime=start,
            EndTime=now,
            Period=7200,
            Statistics=["Maximum"],
        )
        dps = resp.get("Datapoints", [])
        silver_counts[table] = int(dps[0]["Maximum"]) if dps else None

    # ── Gold staging counts from run_results.json ─────────────────────────────
    results_path = Path(_DBT_RUN_PATH) / "target" / "run_results.json"
    if not results_path.exists():
        print("[gold-row-count] run_results.json not found — skipping Gold vs Silver comparison")
        return

    with open(results_path) as f:
        run_results = json.load(f)

    stg_counts: dict[str, int] = {}
    for result in run_results.get("results", []):
        uid = result.get("unique_id", "")
        if not uid.startswith("model.") or "stg_" not in uid:
            continue
        model_name = uid.split(".")[-1]
        rows = result.get("adapter_response", {}).get("rows_affected", -1)
        if rows >= 0:
            stg_counts[model_name] = rows

    if not stg_counts:
        print(
            "[gold-row-count] rows_affected=-1 for all staging models "
            "(adapter does not populate this field for CTAS). Skipping comparison."
        )
        return

    # ── Compare ───────────────────────────────────────────────────────────────
    mismatches = []
    for silver_table, stg_model in _SILVER_TO_STAGING.items():
        silver_count = silver_counts.get(silver_table)
        gold_count = stg_counts.get(stg_model)

        if silver_count is None or gold_count is None:
            print(
                f"[gold-row-count] {stg_model}: missing count "
                f"(silver={silver_count}, gold={gold_count}) — skipping"
            )
            continue

        print(f"[gold-row-count] {stg_model}: silver={silver_count:,}, gold={gold_count:,}")

        if silver_count > 0:
            divergence = abs(silver_count - gold_count) / silver_count
            if divergence > 0.05:
                mismatches.append(
                    f"{stg_model}: silver={silver_count:,}, gold={gold_count:,}, "
                    f"divergence={divergence:.1%} (threshold 5%)"
                )

    if mismatches:
        raise ValueError(f"Gold vs Silver row count divergence exceeded — {'; '.join(mismatches)}")


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

    mwaa_env = Variable.get("mwaa_env", default_var="dev")

    silver_job_names = [
        "dim_customer",
        "dim_product",
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "fact_shipments",
    ]

    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    is_mwaa = airflow_home == "/usr/local/airflow"

    # -----------------------------------------------------------------------
    # Group 1: Silver (parallel Glue jobs)
    # -----------------------------------------------------------------------
    # GlueJobOperator submits an existing Glue job and polls until it reaches
    # a terminal state. verbose=False avoids the AccessDenied error that occurs
    # when MWAA tries to stream /aws-glue/jobs CloudWatch logs.

    silver_tasks = []
    for job_name in silver_job_names:
        task = GlueJobOperator(
            task_id=f"silver_{job_name}",
            job_name=f"edp-{mwaa_env}-{job_name}",
            wait_for_completion=True,
            verbose=False,
            aws_conn_id="aws_default",
        )
        silver_tasks.append(task)

    # -----------------------------------------------------------------------
    # Synchronisation point: silver_complete
    # -----------------------------------------------------------------------

    silver_complete = EmptyOperator(task_id="silver_complete")

    # -----------------------------------------------------------------------
    # O1: Silver row count validation
    # -----------------------------------------------------------------------
    # Reads the SilverRowCount CloudWatch metric published by each Glue job.
    # Fails the DAG early if any table has zero rows, catching silent data loss
    # before the Crawler or dbt run spend time on bad data.

    validate_silver_row_counts = PythonOperator(
        task_id="validate_silver_row_counts",
        python_callable=_validate_silver_row_counts,
        op_kwargs={"env": mwaa_env},
    )

    # -----------------------------------------------------------------------
    # Group 1b: Silver Crawler
    # -----------------------------------------------------------------------
    # Scans the Silver S3 bucket and registers table schemas in the Glue Data
    # Catalog. dbt cannot query Silver via Athena until this runs.

    run_silver_crawler = GlueCrawlerOperator(
        task_id="run_silver_crawler",
        config={"Name": f"edp-{mwaa_env}-silver-crawler"},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # -----------------------------------------------------------------------
    # Shared dbt configuration
    # -----------------------------------------------------------------------

    aws_account_id = Variable.get("aws_account_id")
    athena_results_bucket = f"edp-{mwaa_env}-{aws_account_id}-athena-results"

    dbt_env = {
        "DBT_TARGET": mwaa_env,
        "ATHENA_RESULTS_BUCKET": athena_results_bucket,
        "ATHENA_WORKGROUP": f"edp-{mwaa_env}-workgroup",
        "DBT_ATHENA_SCHEMA": f"edp_{mwaa_env}_gold",
        "DBT_SILVER_SCHEMA": f"edp_{mwaa_env}_silver",
        "AWS_DEFAULT_REGION": "eu-central-1",
    }

    dbt_bin = f"{os.environ.get('HOME', '/home/airflow')}/.local/bin/dbt"
    dbt_run_path = _DBT_RUN_PATH
    dbt_profiles_path = f"{dbt_run_path}/profiles"

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

    # -----------------------------------------------------------------------
    # D2: dbt source freshness gate
    # -----------------------------------------------------------------------
    # Runs dbt test --select source:silver to execute the freshness_relative_to_reference
    # column tests defined in _sources.yml. If Silver data is stale (max timestamp
    # more than 24 hours behind the reference date), the DAG stops here and does
    # not produce stale Gold output.
    #
    # Requires workspace setup because it runs before gold_dbt_run. The S3 sync
    # is idempotent — gold_dbt_run's own setup will re-sync cleanly.

    gold_dbt_source_freshness = BashOperator(
        task_id="gold_dbt_source_freshness",
        bash_command=(
            f"{dbt_setup_cmd} && "
            f"cd {dbt_run_path} && "
            f"{dbt_bin} deps --target {mwaa_env} --profiles-dir {dbt_profiles_path} --no-use-colors && "
            f"{dbt_bin} test --select 'source:silver' --target {mwaa_env} --profiles-dir {dbt_profiles_path} --no-use-colors"
        ),
        env=dbt_env,
        append_env=True,
    )

    # -----------------------------------------------------------------------
    # Group 2: Gold (dbt run then dbt test, sequential)
    # -----------------------------------------------------------------------
    # dbt runs inside the Airflow worker container via BashOperator. In MWAA,
    # dbt is available because it is included in requirements.txt. append_env=True
    # merges vars into the worker's full environment rather than replacing it.

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

    # -----------------------------------------------------------------------
    # D3: Gold vs Silver row count validation
    # -----------------------------------------------------------------------
    # Reads staging model row counts from dbt's run_results.json (written by
    # gold_dbt_run) and compares them to the Silver CloudWatch row count metrics.
    # A >5% divergence indicates a join problem or incorrect filter in a dbt
    # staging model. Runs before gold_dbt_test so test failures don't obscure
    # a row count issue.

    validate_gold_row_counts = PythonOperator(
        task_id="validate_gold_row_counts",
        python_callable=_validate_gold_row_counts,
        op_kwargs={"env": mwaa_env},
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

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    # -----------------------------------------------------------------------
    # Dependency chain
    # -----------------------------------------------------------------------
    # silver_tasks >> silver_complete >> validate_silver_row_counts
    #   >> run_silver_crawler >> gold_dbt_source_freshness
    #   >> gold_dbt_run >> validate_gold_row_counts
    #   >> gold_dbt_test >> pipeline_complete

    silver_tasks >> silver_complete
    (
        silver_complete
        >> validate_silver_row_counts
        >> run_silver_crawler
        >> gold_dbt_source_freshness
        >> gold_dbt_run
        >> validate_gold_row_counts
        >> gold_dbt_test
        >> pipeline_complete
    )

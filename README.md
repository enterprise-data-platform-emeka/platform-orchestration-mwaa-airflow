# platform-orchestration-mwaa-airflow

This repository is part of the [Enterprise Data Platform](https://github.com/enterprise-data-platform-emeka/platform-docs). For the full project overview, architecture diagram, and build order, start there.

---

This repo holds the Airflow DAG (Directed Acyclic Graph) that orchestrates the Enterprise Data Platform (EDP) production data pipeline. It runs on MWAA (Amazon Managed Workflows for Apache Airflow) version 2.9.2 and drives the daily Silver and Gold data transformations.

The pipeline follows Medallion Architecture. Raw CDC (Change Data Capture) events land in Bronze S3 (Simple Storage Service) overnight from DMS (Database Migration Service). At 06:00 UTC this DAG kicks off six parallel Glue PySpark jobs that clean and reshape the data into Silver, then dbt (data build tool) runs against Athena to produce Gold aggregates that Redshift Serverless exposes to the BI dashboard.

## Repository layout

```
platform-orchestration-mwaa-airflow/
├── dags/
│   └── edp_pipeline.py        # The main orchestration DAG
├── plugins/
│   └── __init__.py            # Required by MWAA — empty for now
├── dbt/                       # Mount point for platform-dbt-analytics (gitignored)
├── docker-compose.yml         # Local MWAA runner setup
├── requirements.txt           # Pinned packages for MWAA 2.9.2
├── Makefile                   # Shortcuts for local development
├── .env.example               # Template for local credentials
└── .github/
    └── workflows/
        ├── ci.yml             # Validate DAG on every PR and push
        └── deploy.yml         # Sync to MWAA S3 bucket on merge to main
```

## Local development

I use the official AWS MWAA local runner to develop and test DAG changes before pushing to MWAA. The local runner runs the same `amazon/mwaa-local:2_9` Docker image that MWAA uses in production, so import errors and provider compatibility problems surface locally rather than after a 20-minute MWAA environment update.

### Prerequisites

- Docker Desktop (running)
- AWS credentials for the `dev-admin` profile (or any profile with access to the dev environment)
- `platform-dbt-analytics` repo available locally (for Gold task testing)

### Step 1 — Clone this repo

```bash
git clone <repo-url> platform-orchestration-mwaa-airflow
cd platform-orchestration-mwaa-airflow
```

### Step 2 — Set up AWS credentials

```bash
cp .env.example .env
```

Open `.env` and fill in your AWS credentials. For temporary SSO (Single Sign-On) credentials:

```bash
aws sso login --profile dev-admin
# Then copy the credentials from:
aws configure export-credentials --profile dev-admin --format env
```

Never commit `.env` — it's in `.gitignore`.

### Step 3 — Set up the dbt mount point

The DAG's Gold tasks run dbt inside the container from `/usr/local/airflow/dbt/platform-dbt-analytics`. I mount the `dbt/` directory into that path. The simplest setup is a symlink:

```bash
# From inside platform-orchestration-mwaa-airflow/
ln -s ../../platform-dbt-analytics dbt/platform-dbt-analytics
```

Or clone it directly:

```bash
git clone <dbt-repo-url> dbt/platform-dbt-analytics
```

The `dbt/` directory is gitignored so it doesn't accidentally get committed.

### Step 4 — Start the local runner

```bash
make up
```

The webserver starts on `http://localhost:8080`. Default credentials are `admin` / `test`. The first startup takes a minute or two while Docker pulls the image and installs `requirements.txt`.

```bash
make logs      # watch startup output
make webserver # open http://localhost:8080 in your browser (macOS)
make down      # stop the container
```

### Step 5 — Set Airflow Variables locally

In the Airflow UI go to Admin → Variables and create:

| Key             | Value           | Description                         |
|-----------------|-----------------|-------------------------------------|
| `mwaa_env`      | `dev`           | Target environment                  |
| `aws_account_id`| `158311564771`  | Your AWS account ID                 |

Or set them via the Airflow CLI inside the container:

```bash
docker compose exec local-runner airflow variables set mwaa_env dev
docker compose exec local-runner airflow variables set aws_account_id 158311564771
```

### DAG hot-reload

The `dags/` directory is mounted into the container. Save a change to `edp_pipeline.py` and the scheduler picks it up within ~30 seconds. No restart needed.

If you change `requirements.txt`, restart the container so the new packages install:

```bash
make down && make up
```

## DAG overview

**DAG ID:** `edp_pipeline`
**Schedule:** `0 6 * * *` (06:00 UTC daily)
**Catchup:** disabled (no backfill on first deploy)
**Max active runs:** 1 (prevents overlapping pipeline runs)

### Task breakdown

```
silver_dim_customer ─┐
silver_dim_product  ─┤
silver_fact_orders  ─┤
                      ├─► silver_complete ─► gold_dbt_run ─► gold_dbt_test ─► pipeline_complete
silver_fact_order_items─┤
silver_fact_payments ──┤
silver_fact_shipments──┘
```

**Silver tasks (parallel):** Six `GlueJobOperator` tasks trigger the corresponding Glue jobs. They run in parallel because each job reads from an independent Bronze partition (one per DMS table). `wait_for_completion=True` means Airflow polls the Glue API until the job finishes. If a Glue job fails, the task retries once after 5 minutes.

**silver_complete:** An `EmptyOperator` join point. All six Silver tasks must succeed before dbt starts.

**gold_dbt_run:** A `BashOperator` that runs `dbt run --target {mwaa_env}` inside the worker. This builds all Gold models in the dbt project against Athena.

**gold_dbt_test:** A `BashOperator` that runs `dbt test --target {mwaa_env}` to validate data quality on the Gold models. Runs after `gold_dbt_run`.

**pipeline_complete:** A final `EmptyOperator` that marks successful pipeline completion. Downstream sensors or notification tasks attach here.

### Airflow Variables

The DAG reads two Airflow Variables at parse time:

| Variable        | Required | Default | Description                                               |
|-----------------|----------|---------|-----------------------------------------------------------|
| `mwaa_env`      | Yes      | `dev`   | Sets Glue job names and dbt target (`dev`/`staging`/`prod`) |
| `aws_account_id`| No       | —       | Used for constructing S3 bucket names in logs/alerts      |

Set these in Admin → Variables in the Airflow UI, or via the CLI:

```bash
airflow variables set mwaa_env dev
```

## How to deploy to MWAA

The CI/CD pipeline handles deployment automatically. Here's what happens:

1. Push to `main` → CI runs, validates the DAG import.
2. CI passes → Deploy workflow triggers automatically, syncs to dev MWAA.
3. For staging/prod → trigger the Deploy workflow manually via GitHub Actions → choose the environment.

GitHub Environments (`staging`, `prod`) require reviewer approval before the deployment runs. Set this up in Settings → Environments.

### Manual deployment (if needed)

If you need to deploy outside of CI:

```bash
# Authenticate
aws sso login --profile dev-admin

ACCOUNT_ID=$(aws sts get-caller-identity --profile dev-admin --query Account --output text)
ENV=dev
BUCKET="edp-${ENV}-${ACCOUNT_ID}-mwaa-dags"

# Sync DAGs
aws s3 sync dags/ s3://${BUCKET}/dags/ --delete --profile dev-admin

# Upload requirements
aws s3 cp requirements.txt s3://${BUCKET}/requirements.txt --profile dev-admin

# Sync plugins
aws s3 sync plugins/ s3://${BUCKET}/plugins/ --delete --profile dev-admin
```

MWAA picks up new DAG files within ~30 seconds. A changed `requirements.txt` triggers a MWAA environment update that takes ~20 minutes.

## Updating requirements

Before adding a new package:

1. Check it against the MWAA 2.9.2 constraints file:
   `https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt`
2. Test the install locally with `make down && make up`.
3. Verify the DAG still imports cleanly.
4. Only then push — a bad `requirements.txt` can cause a MWAA environment update failure that takes 20+ minutes to detect and roll back.

## CI

The CI workflow (`.github/workflows/ci.yml`) runs on every pull request and push to main:

1. Installs `apache-airflow==2.9.2` + the Amazon provider into a clean Python environment.
2. Imports `dags/edp_pipeline.py` directly to confirm it loads without errors.
3. Runs `airflow dags list` to check for import errors.

No real AWS calls happen in CI. The DAG uses `Variable.get("mwaa_env", default_var="dev")` so it doesn't need a live Airflow database or AWS connection to parse successfully.

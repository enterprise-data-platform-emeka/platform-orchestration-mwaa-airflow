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
│   └── __init__.py            # Required by MWAA, empty for now
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

### Step 1: Clone this repo

```bash
git clone <repo-url> platform-orchestration-mwaa-airflow
cd platform-orchestration-mwaa-airflow
```

### Step 2: Set up AWS credentials

```bash
cp .env.example .env
```

Open `.env` and fill in your AWS credentials. For temporary SSO (Single Sign-On) credentials:

```bash
aws sso login --profile dev-admin
# Then copy the credentials from:
aws configure export-credentials --profile dev-admin --format env
```

Never commit `.env`. It's in `.gitignore`.

### Step 3: Set up the dbt mount point

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

### Step 4: Start the local runner

```bash
make up
```

The webserver starts on `http://localhost:8080`. Default credentials are `admin` / `test`. The first startup takes a minute or two while Docker pulls the image and installs `requirements.txt`.

```bash
make logs      # watch startup output
make webserver # open http://localhost:8080 in your browser (macOS)
make down      # stop the container
```

### Step 5: Set Airflow Variables locally

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
                      ├─► silver_complete ─► run_silver_crawler ─► gold_dbt_run ─► gold_dbt_test ─► upload_dbt_artifacts ─► pipeline_complete
silver_fact_order_items─┤
silver_fact_payments ──┤
silver_fact_shipments──┘
```

**Silver tasks (parallel):** Six `GlueJobOperator` tasks trigger the corresponding Glue jobs. They run in parallel because each job reads from an independent Bronze partition (one per DMS table). `wait_for_completion=True` means Airflow polls the Glue API until the job finishes. If a Glue job fails, the task retries once after 5 minutes.

**silver_complete:** An `EmptyOperator` join point. All six Silver tasks must succeed before anything downstream starts.

**run_silver_crawler:** A `GlueCrawlerOperator` that runs the Silver Glue Crawler after all Silver jobs complete. This updates the Glue Catalog with any new partitions written to Silver, so Athena sees the latest data when dbt runs.

**gold_dbt_run:** A `BashOperator` that runs `dbt run --target {mwaa_env}` inside the worker. This builds all Gold models in the dbt project against Athena.

**gold_dbt_test:** A `BashOperator` that runs `dbt test --target {mwaa_env}` to validate data quality on the Gold models. Runs after `gold_dbt_run`.

**upload_dbt_artifacts:** A `BashOperator` that copies `target/manifest.json` and `target/catalog.json` from the dbt workspace to `s3://{bronze_bucket}/metadata/dbt/`. The Analytics Agent reads these artifacts at query time to understand the business meaning behind every Gold column. Runs after `gold_dbt_test` so only artifacts from a clean, tested run are published.

**pipeline_complete:** A final `EmptyOperator` that marks successful pipeline completion. Downstream sensors or notification tasks attach here.

### Airflow Variables

The DAG reads two Airflow Variables at parse time:

| Variable        | Required | Default | Description                                               |
|-----------------|----------|---------|-----------------------------------------------------------|
| `mwaa_env`      | Yes      | `dev`   | Sets Glue job names and dbt target (`dev`/`staging`/`prod`) |
| `aws_account_id`| No       | none    | Used for constructing S3 bucket names in logs/alerts      |

Set these in Admin → Variables in the Airflow UI, or via the CLI:

```bash
airflow variables set mwaa_env dev
```

## MWAA environment and DAG

The `edp-dev-mwaa` environment runs Airflow 2.9.2 on MWAA. After the DAG deploys, the full pipeline runs end-to-end with all 11 tasks green.

![MWAA environment edp-dev-mwaa showing Available status running Airflow 2.9.2](images/MWAA-Airflow-AWS-Environment.png)

![Airflow UI showing the edp_pipeline DAG graph with all 11 tasks green after a successful run](images/MWAA-Airflow-UI.png)

---

## How to deploy to MWAA

The CI/CD pipeline handles all deployment automatically. Here's how it works:

### What each repo owns

| Artifact | Owner | Update cost |
|---|---|---|
| DAGs (`dags/`) | This repo | ~30 seconds (S3 sync) |
| `requirements.txt` | This repo | ~35 minutes (MWAA environment update) |
| `plugins.zip` (dbt project) | `platform-dbt-analytics` repo | ~35 minutes (MWAA environment update) |

This separation means you can update DAGs dozens of times a day with no downtime. Changing packages or the dbt project triggers a controlled environment update.

### On push to main

1. CI validates the DAG (lint + import check).
2. CI passes → Deploy workflow triggers automatically.
3. DAGs sync to S3 (MWAA picks them up within ~30 seconds).
4. `requirements.txt` is uploaded. If its content changed, the workflow calls `aws mwaa update-environment` to apply the new packages (~35 min). If content is unchanged, the update is skipped.
5. `plugins.zip` is NOT managed by this repo. Push to `platform-dbt-analytics` to update the dbt project on MWAA workers.

### Promotion to staging and prod

Trigger the Deploy workflow manually from GitHub Actions and choose the target environment. GitHub Environment protection rules require reviewer approval for staging and prod.

### Manual deployment (if needed)

```bash
aws sso login --profile dev-admin

ACCOUNT_ID=$(aws sts get-caller-identity --profile dev-admin --query Account --output text)
ENV=dev
BUCKET="edp-${ENV}-${ACCOUNT_ID}-mwaa-dags"

# Sync DAGs (picked up by MWAA within ~30 seconds)
aws s3 sync dags/ s3://${BUCKET}/dags/ --delete --profile dev-admin

# Upload requirements.txt (triggers MWAA update if changed)
aws s3 cp requirements.txt s3://${BUCKET}/requirements.txt --profile dev-admin
```

To update `plugins.zip` manually, run `make package` in `platform-dbt-analytics` and deploy from that repo.

### First deploy after a fresh infrastructure apply

After `terraform apply` creates a new MWAA environment, it contains an empty placeholder `plugins.zip`. To load the real dbt project:

1. Trigger the `platform-dbt-analytics` deploy workflow manually (via GitHub Actions → workflow_dispatch).
2. Wait ~35 minutes for the MWAA environment update to complete.
3. The pipeline is then ready to run end-to-end.

## Updating requirements

Before adding a new package:

1. Check it against the MWAA 2.9.2 constraints file:
   `https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt`
2. Test the install locally with `make down && make up`.
3. Verify the DAG still imports cleanly.
4. Only then push. A bad `requirements.txt` can cause a MWAA environment update failure that takes 20+ minutes to detect and roll back.

## CI/CD

CI skips runs triggered by README, `.env.example`, or `plugins.zip` changes. Only DAG code, plugins, requirements, and workflow file changes trigger the pipeline.

### On every pull request and push to main

Two jobs run in parallel:

| Job | What it checks |
|---|---|
| Lint and security scan | ruff checks `dags/` and `plugins/` for style. bandit scans the same paths for MEDIUM and HIGH severity security issues. |
| Validate DAG | Installs `apache-airflow==2.9.2` + Amazon provider, imports `edp_pipeline.py` directly, then runs `airflow dags list` to confirm zero import errors. |

No real AWS calls happen in CI. The DAG uses `Variable.get("mwaa_env", default_var="dev")` so it parses without a live Airflow database or AWS connection.

### On merge to main

The deploy workflow triggers automatically after CI passes. It syncs `dags/` and `requirements.txt` to the MWAA S3 (Simple Storage Service) bucket in dev. MWAA picks up new DAG files within about 30 seconds. A changed `requirements.txt` triggers a MWAA environment update that takes around 35 minutes. Authentication uses OIDC (OpenID Connect), no long-lived AWS credentials are stored anywhere. `plugins.zip` is not managed by this repo — push to `platform-dbt-analytics` to update the dbt project on MWAA workers.

### Promotion to staging and prod

Trigger the Deploy workflow manually from GitHub Actions, choose the target environment. GitHub Environment protection rules require reviewer approval for staging and prod before the job runs.

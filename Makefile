# Makefile — platform-orchestration-mwaa-airflow
#
# Shortcuts for working with the aws-mwaa-local-runner.
# Run 'make help' to see all available commands.

.PHONY: help up down logs webserver env package deploy

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------

help:
	@echo ""
	@echo "EDP MWAA (Amazon Managed Workflows for Apache Airflow) local runner"
	@echo ""
	@echo "Prerequisites:"
	@echo "  1. Run 'make env' to log in via SSO and populate .env automatically"
	@echo "  2. Symlink dbt/platform-dbt-analytics (see README)"
	@echo "  3. Run 'make up' to start"
	@echo ""
	@echo "Available commands:"
	@echo "  make env            Log in via SSO and write credentials to .env"
	@echo "  make up             Start the local Airflow runner"
	@echo "  make down           Stop the local runner"
	@echo "  make logs           Follow container logs"
	@echo "  make webserver      Open the Airflow UI (macOS)"
	@echo "  make package        Build plugins.zip from dbt project (run before make apply dev)"
	@echo "  make deploy ENV=dev Upload DAG to the MWAA S3 bucket (run after make apply dev)"
	@echo ""

# ---------------------------------------------------------------------------
# Refresh .env with current SSO credentials
# ---------------------------------------------------------------------------
# Run this every time your SSO session expires (roughly every 8 hours).
# It writes the three credential values from your active dev-admin SSO session
# directly into .env, leaving all other lines (region, account ID, etc.) alone.
# Usage: aws sso login --profile dev-admin && make env

env:
	@aws sso login --profile dev-admin
	$(eval CREDS := $(shell aws configure export-credentials --profile dev-admin --format env))
	@KEY=$$(aws configure export-credentials --profile dev-admin --format env | grep AWS_ACCESS_KEY_ID | cut -d= -f2); \
	SECRET=$$(aws configure export-credentials --profile dev-admin --format env | grep AWS_SECRET_ACCESS_KEY | cut -d= -f2); \
	TOKEN=$$(aws configure export-credentials --profile dev-admin --format env | grep AWS_SESSION_TOKEN | cut -d= -f2); \
	if [ ! -f .env ]; then cp .env.example .env; fi; \
	sed -i '' "s|^AWS_ACCESS_KEY_ID=.*|AWS_ACCESS_KEY_ID=$$KEY|" .env; \
	sed -i '' "s|^AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=$$SECRET|" .env; \
	sed -i '' "s|^#* *AWS_SESSION_TOKEN=.*|AWS_SESSION_TOKEN=$$TOKEN|" .env; \
	echo "✓ .env updated with fresh dev-admin credentials"

# ---------------------------------------------------------------------------
# Start the local MWAA runner
# ---------------------------------------------------------------------------
# Reads credentials and config from .env (copied from .env.example).
# The webserver starts on http://localhost:8080 (admin / test).
# DAG changes are picked up automatically within ~30 seconds.
# requirements.txt changes require a container restart (make down && make up).

up:
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found."; \
		echo "Run: make env  to log in via SSO and populate .env automatically."; \
		exit 1; \
	fi
	docker compose up -d
	@echo ""
	@echo "Local runner starting. Webserver will be ready at http://localhost:8080"
	@echo "Default credentials: admin / test"
	@echo "Run 'make logs' to watch startup progress."

# ---------------------------------------------------------------------------
# Stop the local MWAA runner
# ---------------------------------------------------------------------------

down:
	docker compose down

# ---------------------------------------------------------------------------
# Follow container logs
# ---------------------------------------------------------------------------
# Shows combined output from the scheduler and webserver.
# Press Ctrl+C to stop following (the container keeps running).

logs:
	docker compose logs -f

# ---------------------------------------------------------------------------
# Open the Airflow webserver UI (macOS only)
# ---------------------------------------------------------------------------

webserver:
	open http://localhost:8080

# ---------------------------------------------------------------------------
# Build plugins.zip for MWAA deployment
# ---------------------------------------------------------------------------
# MWAA extracts plugins.zip to /usr/local/airflow/plugins/ on every worker.
# The dbt project lives there so the BashOperator can find it at a known path.
#
# Run this BEFORE 'make apply dev' in terraform-platform-infra-live.
# Terraform uploads the resulting plugins.zip to the MWAA DAGs S3 bucket.

package:
	@echo "Building plugins.zip from ../platform-dbt-analytics ..."
	@if [ ! -d "../platform-dbt-analytics" ]; then \
		echo "Error: ../platform-dbt-analytics not found. Clone the repo first."; \
		exit 1; \
	fi
	@rm -f plugins.zip
	@cd .. && zip -r platform-orchestration-mwaa-airflow/plugins.zip platform-dbt-analytics \
		--exclude "platform-dbt-analytics/.git/*" \
		--exclude "platform-dbt-analytics/__pycache__/*" \
		--exclude "platform-dbt-analytics/.venv/*" \
		--exclude "platform-dbt-analytics/target/*" \
		--exclude "platform-dbt-analytics/dbt_packages/*" \
		--exclude "platform-dbt-analytics/scripts/*" \
		--exclude "platform-dbt-analytics/data/*" \
		--exclude "platform-dbt-analytics/logs/*"
	@echo "plugins.zip created ($(shell du -sh plugins.zip | cut -f1))"

# ---------------------------------------------------------------------------
# Upload DAG to the MWAA S3 bucket
# ---------------------------------------------------------------------------
# Run this AFTER 'make apply dev' has created the MWAA environment.
# ENV controls which environment to deploy to (dev, staging, prod).
# Usage: make deploy ENV=dev

ENV ?= dev

deploy:
	@if [ ! -f .env ]; then \
		echo "Error: .env not found. Run 'make env' first."; \
		exit 1; \
	fi
	$(eval ACCOUNT_ID := $(shell aws sts get-caller-identity --profile $(ENV)-admin --query Account --output text))
	@BUCKET="edp-$(ENV)-$(ACCOUNT_ID)-mwaa-dags"; \
	echo "Uploading DAG to s3://$$BUCKET/dags/ ..."; \
	aws s3 cp dags/edp_pipeline.py s3://$$BUCKET/dags/edp_pipeline.py --profile $(ENV)-admin; \
	echo "DAG uploaded. MWAA picks up changes within 30 seconds."

# Makefile — platform-orchestration-mwaa-airflow
#
# Shortcuts for working with the aws-mwaa-local-runner.
# Run 'make help' to see all available commands.

.PHONY: help up down logs webserver env

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
	@echo "  make env        Log in via SSO and write credentials to .env automatically"
	@echo "  make up         Start the local MWAA runner (Airflow webserver + scheduler)"
	@echo "  make down       Stop and remove the local MWAA runner container"
	@echo "  make logs       Follow container logs (Ctrl+C to stop)"
	@echo "  make webserver  Open the Airflow UI in your default browser (macOS)"
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

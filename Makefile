# Makefile — platform-orchestration-mwaa-airflow
#
# Shortcuts for working with the aws-mwaa-local-runner.
# Run 'make help' to see all available commands.

.PHONY: help up down logs webserver

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------

help:
	@echo ""
	@echo "EDP MWAA (Amazon Managed Workflows for Apache Airflow) local runner"
	@echo ""
	@echo "Prerequisites:"
	@echo "  1. Copy .env.example to .env and fill in your AWS credentials"
	@echo "  2. Symlink dbt/platform-dbt-analytics (see README)"
	@echo "  3. Run 'make up' to start"
	@echo ""
	@echo "Available commands:"
	@echo "  make up         Start the local MWAA runner (Airflow webserver + scheduler)"
	@echo "  make down       Stop and remove the local MWAA runner container"
	@echo "  make logs       Follow container logs (Ctrl+C to stop)"
	@echo "  make webserver  Open the Airflow UI in your default browser (macOS)"
	@echo ""

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
		echo "Run: cp .env.example .env  then fill in your AWS credentials."; \
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

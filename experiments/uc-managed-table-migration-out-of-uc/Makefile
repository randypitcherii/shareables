.DEFAULT_GOAL := help

-include .env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
EXPERIMENT_WAREHOUSE_ID ?=
EXPERIMENT_EXTERNAL_ROOT ?=
EXPERIMENT_MANAGED_CATALOG ?= uc_managed_exit_experiment
AWS_PROFILE ?=
AWS_REGION ?= us-east-1

.PHONY: help sync auth-check verify setup matrix cleanup test lint check

help: ## Show targets and active non-secret configuration
	@awk 'BEGIN {FS = ":.*## "; printf "Targets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nConfiguration:\n  DATABRICKS_CONFIG_PROFILE=%s\n  EXPERIMENT_WAREHOUSE_ID=%s\n  EXPERIMENT_EXTERNAL_ROOT=%s\n  EXPERIMENT_MANAGED_CATALOG=%s\n  AWS_PROFILE=%s\n  AWS_REGION=%s\n' "$(DATABRICKS_CONFIG_PROFILE)" "$(EXPERIMENT_WAREHOUSE_ID)" "$(EXPERIMENT_EXTERNAL_ROOT)" "$(EXPERIMENT_MANAGED_CATALOG)" "$(AWS_PROFILE)" "$(AWS_REGION)"

sync: ## Install locked Python dependencies
	uv sync

auth-check: ## Verify the configured Databricks OAuth/SSO profile
	@test -n "$(EXPERIMENT_WAREHOUSE_ID)" || (echo "EXPERIMENT_WAREHOUSE_ID is required" && exit 1)
	@test -n "$(EXPERIMENT_EXTERNAL_ROOT)" || (echo "EXPERIMENT_EXTERNAL_ROOT is required" && exit 1)
	@test -n "$(AWS_PROFILE)" || (echo "AWS_PROFILE is required" && exit 1)
	@databricks auth describe -p "$(DATABRICKS_CONFIG_PROFILE)" >/dev/null
	@aws sts get-caller-identity --profile "$(AWS_PROFILE)" >/dev/null

verify: auth-check ## Prove identity, SQL warehouse, external location, and direct S3 access
	uv run python scripts/verify.py

setup: verify ## Create the S3-rooted catalog with managed Delta (UniForm) and managed Iceberg sources
	uv run python scripts/setup.py

matrix: setup ## Run the 2 sources x 3 exits zero-copy matrix and write results JSON
	uv run python scripts/exit_01_zero_catalog_delta.py
	uv run python scripts/exit_03_iceberg_new_catalog.py
	uv run python scripts/exit_02_uc_external_reregister.py

cleanup: auth-check ## Drop the experiment catalog (object-storage files are left in place)
	uv run python scripts/cleanup.py

test: ## Run no-infrastructure tests
	uv run pytest -m "not infrastructure"

lint: ## Run Ruff checks
	uv run ruff check scripts tests

check: test lint ## Run all local quality gates

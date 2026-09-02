.DEFAULT_GOAL := help

-include .env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
EXPERIMENT_CATALOG ?= my_catalog
EXPERIMENT_SCHEMA ?= uc_managed_exit_experiment
EXPERIMENT_WAREHOUSE_ID ?=
EXPERIMENT_EXTERNAL_ROOT ?=
AWS_PROFILE ?=
AWS_REGION ?= us-east-1
EXPERIMENT_GLUE_DATABASE ?= uc_managed_exit_experiment
EXPERIMENT_ATHENA_OUTPUT ?=

.PHONY: help sync auth-check verify setup matrix cleanup test lint check

help: ## Show targets and active non-secret configuration
	@awk 'BEGIN {FS = ":.*## "; printf "Targets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nConfiguration:\n  DATABRICKS_CONFIG_PROFILE=%s\n  EXPERIMENT_CATALOG=%s\n  EXPERIMENT_SCHEMA=%s\n  EXPERIMENT_WAREHOUSE_ID=%s\n  EXPERIMENT_EXTERNAL_ROOT=%s\n  AWS_PROFILE=%s\n  AWS_REGION=%s\n  EXPERIMENT_GLUE_DATABASE=%s\n  EXPERIMENT_ATHENA_OUTPUT=%s\n' "$(DATABRICKS_CONFIG_PROFILE)" "$(EXPERIMENT_CATALOG)" "$(EXPERIMENT_SCHEMA)" "$(EXPERIMENT_WAREHOUSE_ID)" "$(EXPERIMENT_EXTERNAL_ROOT)" "$(AWS_PROFILE)" "$(AWS_REGION)" "$(EXPERIMENT_GLUE_DATABASE)" "$(EXPERIMENT_ATHENA_OUTPUT)"

sync: ## Install locked Python dependencies
	uv sync

auth-check: ## Verify the configured Databricks OAuth/SSO profile
	@test -n "$(EXPERIMENT_WAREHOUSE_ID)" || (echo "EXPERIMENT_WAREHOUSE_ID is required" && exit 1)
	@test -n "$(EXPERIMENT_EXTERNAL_ROOT)" || (echo "EXPERIMENT_EXTERNAL_ROOT is required" && exit 1)
	@test -n "$(AWS_PROFILE)" || (echo "AWS_PROFILE is required" && exit 1)
	@test -n "$(EXPERIMENT_ATHENA_OUTPUT)" || (echo "EXPERIMENT_ATHENA_OUTPUT is required" && exit 1)
	@databricks auth describe -p "$(DATABRICKS_CONFIG_PROFILE)" >/dev/null
	@aws sts get-caller-identity --profile "$(AWS_PROFILE)" >/dev/null

verify: auth-check ## Prove identity, SQL warehouse, catalog, and external root with live calls
	uv run python scripts/verify.py

setup: verify ## Create isolated managed Delta and managed Iceberg source tables
	uv run python scripts/setup.py

matrix: setup ## Run every numbered migration probe and write results JSON
	uv run python scripts/migrations/01_delta_to_uncataloged_path.py
	uv run python scripts/migrations/02_delta_to_uc_external.py
	uv run python scripts/migrations/03_delta_unregister_reregister.py
	uv run python scripts/migrations/04_managed_iceberg_export.py
	uv run python scripts/migrations/05_delta_to_glue_athena.py

cleanup: auth-check ## Drop the experiment schema and external destination tables
	uv run python scripts/cleanup.py

test: ## Run no-infrastructure tests
	uv run pytest -m "not infrastructure"

lint: ## Run Ruff checks
	uv run ruff check scripts tests

check: test lint ## Run all local quality gates

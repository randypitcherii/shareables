.DEFAULT_GOAL := help

-include .env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
EXPERIMENT_WAREHOUSE_ID ?=
EXPERIMENT_STORAGE_ROOT ?=
EXPERIMENT_CATALOG ?= uc_delta_vs_iceberg_experiment
AWS_REGION ?= us-east-1
EXPERIMENT_SPARK_VERSION ?= 19.x-scala2.13
EXPERIMENT_NODE_TYPE ?= m5d.xlarge

.PHONY: help sync auth-check verify setup duckdb spark cdf streaming writers pyiceberg mitigation matrix cleanup test lint check

help: ## Show targets and active non-secret configuration
	@awk 'BEGIN {FS = ":.*## "; printf "Targets:\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nConfiguration:\n  DATABRICKS_CONFIG_PROFILE=%s\n  EXPERIMENT_WAREHOUSE_ID=%s\n  EXPERIMENT_STORAGE_ROOT=%s\n  EXPERIMENT_CATALOG=%s\n  AWS_REGION=%s\n  EXPERIMENT_SPARK_VERSION=%s\n  EXPERIMENT_NODE_TYPE=%s\n' "$(DATABRICKS_CONFIG_PROFILE)" "$(EXPERIMENT_WAREHOUSE_ID)" "$(EXPERIMENT_STORAGE_ROOT)" "$(EXPERIMENT_CATALOG)" "$(AWS_REGION)" "$(EXPERIMENT_SPARK_VERSION)" "$(EXPERIMENT_NODE_TYPE)"

sync: ## Install locked Python dependencies
	uv sync

auth-check: ## Verify required config and the OAuth/SSO Databricks profile
	@test -n "$(EXPERIMENT_WAREHOUSE_ID)" || (echo "EXPERIMENT_WAREHOUSE_ID is required" && exit 1)
	@test -n "$(EXPERIMENT_STORAGE_ROOT)" || (echo "EXPERIMENT_STORAGE_ROOT is required" && exit 1)
	@databricks auth describe -p "$(DATABRICKS_CONFIG_PROFILE)" >/dev/null

verify: auth-check ## Prove identity, warehouse, external data access, and Iceberg REST reachability
	uv run python scripts/verify.py

setup: verify ## Create the catalog, evidence volume, and seeded managed Delta / Iceberg tables
	uv run python scripts/setup.py

duckdb: ## Row 01: DuckDB read + INSERT/UPDATE/DELETE/MERGE on both formats
	uv run python scripts/01_duckdb_dml.py

spark: ## Run the DBR job that gathers raw evidence for rows 02-04 (~10 min)
	uv run python scripts/spark_run.py

cdf: ## Row 02: automatic change data feed (DBR job + DBSQL)
	uv run python scripts/02_change_data_feed.py

streaming: ## Row 03: Structured Streaming source and sink
	uv run python scripts/03_structured_streaming.py

writers: ## Row 04: DataFrame V1/V2 writers and DeltaTable.merge
	uv run python scripts/04_dataframe_writers.py

pyiceberg: ## Row 05: PyIceberg writes through the UC Iceberg REST catalog
	uv run python scripts/05_pyiceberg_writes.py

mitigation: ## Row 06: Liquid Iceberg v3 with deletion vectors off
	uv run python scripts/06_iceberg_dv_off_mitigation.py

matrix: setup duckdb spark cdf streaming writers pyiceberg mitigation ## Full run, in order (row 02 depends on row 01's commits)

cleanup: auth-check ## Drop the experiment catalog (CASCADE)
	uv run python scripts/cleanup.py

test: ## Run no-infrastructure tests
	uv run pytest -m "not infrastructure"

lint: ## Run Ruff checks
	uv run ruff check scripts tests

check: test lint ## Run all local quality gates

.DEFAULT_GOAL := help

# Config via env vars (or a gitignored .env) — sane defaults, echoed in help.
export DATABRICKS_CONFIG_PROFILE ?= DEFAULT
export EXPERIMENT_CATALOG ?= my_catalog
export EXPERIMENT_SCHEMA ?= dbt_v2_compat
export EXPERIMENT_WAREHOUSE_ID ?=
export DBT_V2_VERSION ?= 2.0.11
export DBT_V2_DIST ?= dbt_oss

-include .env
export

PY := uv run python

.PHONY: help auth-check install-dbt setup matrix jobs readme all teardown test lint

help: ## List targets and the environment they read
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'
	@echo ""
	@echo "  DATABRICKS_CONFIG_PROFILE  CLI profile          ($(DATABRICKS_CONFIG_PROFILE))"
	@echo "  EXPERIMENT_CATALOG         UC catalog           ($(EXPERIMENT_CATALOG))"
	@echo "  EXPERIMENT_SCHEMA          schema prefix        ($(EXPERIMENT_SCHEMA))"
	@echo "  EXPERIMENT_WAREHOUSE_ID    SQL warehouse        (default: first serverless)"
	@echo "  DBT_V2_VERSION / DBT_V2_DIST  release under test ($(DBT_V2_DIST) $(DBT_V2_VERSION))"

auth-check: ## Verify Databricks auth before anything live
	databricks auth describe --profile $(DATABRICKS_CONFIG_PROFILE) > /dev/null

install-dbt: ## Install the pinned dbt v2 wheel into .dbt-v2/
	$(PY) scripts/install_dbt_v2.py

setup: auth-check ## Schema, landing volume, first landing file, freshness source
	cd scripts && $(PY) setup.py

matrix: install-dbt setup ## Local rows r01-r26 (laptop -> warehouse), ~4 min
	cd scripts && $(PY) run_matrix.py

jobs: setup ## Jobs rows j01-j03: deploy the bundle and run both orchestration paths
	cd scripts && $(PY) run_jobs.py

readme: ## Regenerate the README findings matrix from results/matrix_results.json
	cd scripts && $(PY) render_readme.py

all: matrix jobs readme ## Everything, then the README

teardown: auth-check ## Drop the experiment schemas and destroy the bundle
	cd scripts && $(PY) teardown.py

test: ## No-infra unit tests
	uv run pytest -m "not infrastructure"

lint: ## Ruff
	uv run ruff check scripts tests

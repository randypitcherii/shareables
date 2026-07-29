.DEFAULT_GOAL := help

# Config via env vars — sane defaults, overridable, echoed in help.
export DATABRICKS_CONFIG_PROFILE ?= DEFAULT
export EXPERIMENT_CATALOG ?= my_catalog
export EXPERIMENT_SCHEMA ?= udf_privilege_delegation
export EXPERIMENT_WAREHOUSE_ID ?=
export EXPERIMENT_LOWPRIV_DISPLAY_NAME ?= udf-delegation-lowpriv

# Credentials for the low-privilege caller are written to .env by `make setup`.
-include .env
export

PY := uv run python

.PHONY: help auth-check verify setup matrix row-1 row-2 row-3 row-4 row-5 row-6 row-7 teardown test test-infrastructure lint

help: ## List available targets
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Environment variables:"
	@echo "  DATABRICKS_CONFIG_PROFILE      CLI profile (default: DEFAULT)"
	@echo "  EXPERIMENT_CATALOG             Unity Catalog name (default: my_catalog)"
	@echo "  EXPERIMENT_SCHEMA              UC schema (default: udf_privilege_delegation)"
	@echo "  EXPERIMENT_WAREHOUSE_ID        SQL warehouse id (default: first serverless warehouse)"
	@echo "  EXPERIMENT_LOWPRIV_DISPLAY_NAME  Caller service principal (default: udf-delegation-lowpriv)"

auth-check: ## Verify Databricks authentication before anything live
	databricks auth describe --profile $(DATABRICKS_CONFIG_PROFILE)

verify: auth-check ## Prove auth + connectivity end-to-end with one real request
	$(PY) scripts/verify.py

setup: verify ## Create the low-privilege caller, the admin-owned objects, and the grants
	$(PY) scripts/setup/00_create_lowpriv_principal.py
	$(PY) scripts/setup/01_provision_objects.py

matrix: row-1 row-2 row-3 row-4 row-5 row-6 row-7 ## Run every matrix row and rewrite results/matrix_results.json

row-1: ## SQL UDF — definer vs invoker rights
	$(PY) scripts/delegation/01_sql_udf_definer_vs_invoker.py

row-2: ## Python UDF — outbound network egress
	$(PY) scripts/delegation/02_python_udf_network_egress.py

row-3: ## Python UDF — ambient credential context
	$(PY) scripts/delegation/03_python_udf_credential_context.py

row-4: ## End to end — low-privilege caller creates a service principal
	$(PY) scripts/delegation/04_end_to_end_create_service_principal.py

row-5: ## SQL surface for identity administration
	$(PY) scripts/delegation/05_sql_surface_for_identity_admin.py

row-6: ## Function body readability — can a caller with only EXECUTE read the source?
	$(PY) scripts/delegation/06_function_body_readable_by_caller.py

row-7: ## Author-embedded credential — does the delegated action work then?
	$(PY) scripts/delegation/07_embedded_credential_end_to_end.py

teardown: ## Remove the schema, the functions, and both service principals
	$(PY) scripts/setup/99_teardown.py

test: ## Run no-infra tests only (unit + composition)
	uv run pytest -m "not infrastructure"

test-infrastructure: auth-check ## Run live tests (requires Databricks auth)
	uv run pytest -m infrastructure

lint: ## Ruff check
	uv run ruff check scripts tests

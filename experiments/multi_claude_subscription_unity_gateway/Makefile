.DEFAULT_GOAL := help
SHELL := /bin/bash

-include .env
DATABRICKS_CONFIG_PROFILE ?= DEFAULT
CATALOG ?= main
TARGET ?= dev
export DATABRICKS_CONFIG_PROFILE CATALOG TARGET WAREHOUSE_ID

PY := uv run python
TEST_CALLER = $(shell jq -r .application_id .state/test_caller.json 2>/dev/null)
BUNDLE = databricks bundle --profile $(DATABRICKS_CONFIG_PROFILE) --target $(TARGET) --var catalog=$(CATALOG) --var sub_a_principal=$(TEST_CALLER)

help: ## Show targets and current config
	@awk 'BEGIN{FS=":.*## "} /^[a-z0-9_%-]+:.*## /{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}' $(MAKEFILE_LIST)
	@echo; echo "  DATABRICKS_CONFIG_PROFILE=$(DATABRICKS_CONFIG_PROFILE)  CATALOG=$(CATALOG)  TARGET=$(TARGET)"

auth-check: ## Prove the Databricks profile works
	@databricks current-user me --profile $(DATABRICKS_CONFIG_PROFILE) -o json | jq -r '"databricks: " + .userName'

setup: auth-check ## dev: create the throwaway test-caller SP (non-owner the matrix acts as)
	$(PY) scripts/setup/00_test_caller.py

validate: ## databricks bundle validate
	$(BUNDLE) validate

deploy: auth-check ## Deploy the schema and the three relayed provider services
	@test -n "$(TEST_CALLER)" || { echo "run make setup first"; exit 1; }
	$(BUNDLE) deploy

up: setup deploy ## setup + deploy

matrix: ## Run every row that needs no real subscription
	@for s in scripts/matrix/[0-9][0-9]_*.py; do $(PY) $$s || true; done

row-%: ## Run one row, e.g. make row-04
	$(PY) $$(ls scripts/matrix/$*_*.py)

test: ## No-infra unit tests (harness + Omnigent launcher plugin)
	uv run --group dev pytest -q
	cd omnigent_ug_launcher && uv run --quiet --with-editable . --with pytest pytest -q

teardown: ## Destroy the bundle, then delete the test-caller SP
	$(BUNDLE) destroy --auto-approve
	$(PY) scripts/setup/99_teardown_test_caller.py

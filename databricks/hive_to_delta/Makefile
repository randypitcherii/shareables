.PHONY: help install test test-unit test-composition test-infrastructure \
	test-standard test-cross-bucket test-cross-region \
	test-verbose test-parallel test-collect lint format clean setup-tables \
	auth-check tf-init tf-plan tf-apply tf-destroy

# Default environment variables for tests
export AWS_PROFILE ?= aws-sandbox-field-eng_databricks-sandbox-admin
export AWS_REGION ?= us-east-1
export HIVE_TO_DELTA_TEST_GLUE_DATABASE ?= hive_to_delta_test
export HIVE_TO_DELTA_TEST_CATALOG ?= fe_randy_pitcher_workspace_catalog
export HIVE_TO_DELTA_TEST_SCHEMA ?= hive_to_delta_tests

# Default target
help:
	@echo "Hive to Delta - Makefile Commands"
	@echo "=================================="
	@echo ""
	@echo "Testing (by category):"
	@echo "  make test-unit           - Run unit tests only (no infra required)"
	@echo "  make test-composition    - Run composition tests only (no infra required)"
	@echo "  make test-infrastructure - Run infrastructure tests (requires Databricks + AWS)"
	@echo ""
	@echo "Testing (by scenario):"
	@echo "  make test                - Run ALL tests (unit + composition + infrastructure)"
	@echo "  make test-standard       - Run standard partition layout tests"
	@echo "  make test-cross-bucket   - Run cross-bucket partition tests"
	@echo "  make test-cross-region   - Run cross-region partition tests"
	@echo ""
	@echo "Testing (utilities):"
	@echo "  make test-verbose        - Run all tests with extra verbose output"
	@echo "  make test-parallel       - Run tests in parallel (4 workers)"
	@echo "  make test-collect        - Show test collection without running"
	@echo ""
	@echo "Development:"
	@echo "  make install             - Install package in editable mode with dev dependencies"
	@echo "  make lint                - Run ruff linter"
	@echo "  make format              - Format code with ruff"
	@echo "  make clean               - Remove build artifacts and caches"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make auth-check          - Verify Databricks + AWS authentication"
	@echo "  make setup-tables        - Create Glue test tables in AWS"
	@echo "  make tf-init             - Initialize Terraform"
	@echo "  make tf-plan             - Plan Terraform changes"
	@echo "  make tf-apply            - Apply Terraform changes"
	@echo "  make tf-destroy          - Destroy Terraform resources"
	@echo ""
	@echo "Environment Variables:"
	@echo "  AWS_PROFILE                      - AWS CLI profile (default: aws-sandbox-field-eng_databricks-sandbox-admin)"
	@echo "  AWS_REGION                       - AWS region (default: us-east-1)"
	@echo "  HIVE_TO_DELTA_TEST_GLUE_DATABASE - Glue database name (default: hive_to_delta_test)"
	@echo "  HIVE_TO_DELTA_TEST_CATALOG       - Unity Catalog name (default: fe_randy_pitcher_workspace_catalog)"
	@echo "  HIVE_TO_DELTA_TEST_SCHEMA        - Unity Catalog schema (default: hive_to_delta_tests)"
	@echo "  HIVE_TO_DELTA_TEST_WAREHOUSE_ID  - SQL Warehouse ID (auto-detected if not set)"
	@echo ""

# Install package in editable mode with dev dependencies
install:
	pip install -e .[dev]

# =====================================================================
# Test targets by category
# =====================================================================

# Run unit tests only (no infrastructure required)
# These are the fast, isolated tests in test_models, test_schema, test_discovery, etc.
test-unit:
	@echo "=== Running unit tests (no infrastructure required) ==="
	uv run pytest tests/ -v --ignore=tests/test_infrastructure.py --ignore=tests/test_composition.py --ignore=tests/test_convert.py --ignore=tests/test_operations.py --ignore=tests/test_shallow_clone.py

# Run composition tests only (no infrastructure required)
# These test multi-module wiring with mocks
test-composition:
	@echo "=== Running composition tests (no infrastructure required) ==="
	uv run pytest tests/test_composition.py -v

# Run infrastructure tests for the composable pipeline (requires Databricks + AWS)
test-infrastructure:
	@echo "=== Running composable pipeline infrastructure tests ==="
	@echo "AWS Profile: $(AWS_PROFILE)"
	@echo "AWS Region: $(AWS_REGION)"
	@echo "Glue Database: $(HIVE_TO_DELTA_TEST_GLUE_DATABASE)"
	@echo "Target Catalog: $(HIVE_TO_DELTA_TEST_CATALOG)"
	@echo "Target Schema: $(HIVE_TO_DELTA_TEST_SCHEMA)"
	@echo ""
	uv run pytest tests/test_infrastructure.py -v -m infrastructure

# =====================================================================
# Test targets by scenario (legacy infra tests)
# =====================================================================

# Run all tests with verbose output
test:
	@echo "=== Running ALL hive_to_delta tests ==="
	@echo "AWS Profile: $(AWS_PROFILE)"
	@echo "AWS Region: $(AWS_REGION)"
	@echo "Glue Database: $(HIVE_TO_DELTA_TEST_GLUE_DATABASE)"
	@echo "Target Catalog: $(HIVE_TO_DELTA_TEST_CATALOG)"
	@echo "Target Schema: $(HIVE_TO_DELTA_TEST_SCHEMA)"
	@echo ""
	uv run pytest tests/ -v

# Run standard partition layout tests
test-standard:
	uv run pytest tests/ -v -m standard

# Run cross-bucket partition tests
test-cross-bucket:
	uv run pytest tests/ -v -m cross_bucket

# Run cross-region partition tests
test-cross-region:
	uv run pytest tests/ -v -m cross_region

# Run tests with extra verbose output
test-verbose:
	uv run pytest tests/ -vv

# Run tests in parallel (4 workers)
test-parallel:
	uv run pytest tests/ -v -n 4

# Show test collection without running
test-collect:
	uv run pytest tests/ --collect-only

# =====================================================================
# Development tools
# =====================================================================

# Run ruff linter
lint:
	ruff check hive_to_delta/

# Format code with ruff
format:
	ruff format hive_to_delta/

# =====================================================================
# Infrastructure management
# =====================================================================

# Verify Databricks and AWS authentication
auth-check:
	@echo "=== Checking authentication ==="
	@echo ""
	@echo "--- Databricks ---"
	@databricks auth env 2>&1 && echo "Databricks: OK" || echo "Databricks: FAILED - run: databricks auth login --host https://fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com"
	@echo ""
	@echo "--- AWS ---"
	@aws sts get-caller-identity --profile $(AWS_PROFILE) 2>&1 && echo "AWS: OK" || echo "AWS: FAILED - run: aws sso login --profile $(AWS_PROFILE)"
	@echo ""

# Setup Glue test tables in AWS
# Requires: HTD_BUCKET_EAST_1A, HTD_BUCKET_EAST_1B, HTD_BUCKET_WEST_2, HTD_GLUE_DATABASE env vars
setup-tables:
	AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) uv run python scripts/setup_fresh_hive_tables.py

# Clean build artifacts and caches
clean:
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf *.egg-info
	rm -rf hive_to_delta.egg-info
	rm -rf dist
	rm -rf build
	rm -rf .coverage
	rm -rf htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true

# Initialize Terraform
tf-init:
	cd terraform && terraform init

# Plan Terraform changes
tf-plan:
	cd terraform && terraform plan

# Apply Terraform changes
tf-apply:
	cd terraform && terraform apply

# Destroy Terraform resources
tf-destroy:
	cd terraform && terraform destroy

.PHONY: help install test test-standard test-cross-bucket test-cross-region \
	lint format clean setup-tables tf-init tf-plan tf-apply tf-destroy

# Default target
help:
	@echo "Hive to Delta - Makefile Commands"
	@echo "=================================="
	@echo ""
	@echo "Development:"
	@echo "  make install          - Install package in editable mode with dev dependencies"
	@echo "  make test             - Run all pytest tests"
	@echo "  make test-standard    - Run standard partition layout tests"
	@echo "  make test-cross-bucket - Run cross-bucket partition tests"
	@echo "  make test-cross-region - Run cross-region partition tests"
	@echo "  make lint             - Run ruff linter"
	@echo "  make format           - Format code with ruff"
	@echo "  make clean            - Remove build artifacts and caches"
	@echo "  make setup-tables     - Create Glue test tables in AWS"
	@echo ""
	@echo "Terraform:"
	@echo "  make tf-init          - Initialize Terraform"
	@echo "  make tf-plan          - Plan Terraform changes"
	@echo "  make tf-apply         - Apply Terraform changes"
	@echo "  make tf-destroy       - Destroy Terraform resources"
	@echo ""
	@echo "Environment Variables (for Terraform):"
	@echo "  AWS_PROFILE           - AWS CLI profile to use"
	@echo "  AWS_REGION            - AWS region (defaults to us-east-1)"
	@echo ""

# Install package in editable mode with dev dependencies
install:
	pip install -e .[dev]

# Run all tests
# AWS_PROFILE and AWS_REGION are passed through for S3/Glue access
test:
	AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) uv run pytest tests/

# Run standard partition layout tests
test-standard:
	AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) uv run pytest tests/ -m standard

# Run cross-bucket partition tests
test-cross-bucket:
	AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) uv run pytest tests/ -m cross_bucket

# Run cross-region partition tests
test-cross-region:
	AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) uv run pytest tests/ -m cross_region

# Run ruff linter
lint:
	ruff check hive_to_delta/

# Format code with ruff
format:
	ruff format hive_to_delta/

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

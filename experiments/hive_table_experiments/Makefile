.PHONY: help init deploy-aws deploy-databricks deploy \
	setup-basic-layouts setup-external-partitions setup-multi-location \
	setup-glue-hive setup-glue-fed setup-uc-ext setup-uc-delta setup-all-methods \
	setup-conversion-sources \
	test test-basic-layouts test-external-partitions test-multi-location \
	test-glue-hive test-glue-fed test-uc-external test-delta-operations \
	test-conversion test-convert-to-delta test-shallow-clone test-manual-delta-log \
	test-converted-tables test-scenario \
	generate-results generate-conversion-matrix \
	destroy-databricks destroy-aws destroy clean

# Default target
help:
	@echo "Hive Table Migration Experiments - Makefile Commands"
	@echo "===================================================="
	@echo ""
	@echo "Setup & Deployment:"
	@echo "  make init                     - Initialize Terraform and create Databricks secrets scope"
	@echo "  make deploy-aws               - Deploy AWS resources (Terraform)"
	@echo "  make deploy-databricks        - Deploy Databricks resources (DABs)"
	@echo "  make deploy                   - Full deployment (AWS + Databricks)"
	@echo ""
	@echo "Fixture Setup (by access method):"
	@echo "  make setup-glue-hive          - Create glue_as_hive_metastore fixtures"
	@echo "  make setup-glue-fed           - Create uc_glue_federation fixtures"
	@echo "  make setup-uc-ext             - Create uc_external_tables (Parquet) fixtures"
	@echo "  make setup-uc-delta           - Create uc_external_tables (Delta) fixtures"
	@echo "  make setup-all-methods        - Create all fixtures for all access methods"
	@echo ""
	@echo "Fixture Setup (legacy by capability):"
	@echo "  make setup-basic-layouts      - Create Basic Layout test fixtures"
	@echo "  make setup-external-partitions - Create External Partition fixtures"
	@echo "  make setup-multi-location     - Create Multi-Location fixtures"
	@echo ""
	@echo "Testing (by access method):"
	@echo "  make test                     - Run all pytest tests"
	@echo "  make test-glue-hive           - Run glue_as_hive_metastore tests"
	@echo "  make test-glue-fed            - Run uc_glue_federation tests"
	@echo "  make test-uc-external         - Run uc_external_tables tests"
	@echo "  make test-delta-operations    - Run OPTIMIZE/VACUUM tests"
	@echo ""
	@echo "Testing (legacy by capability):"
	@echo "  make test-basic-layouts       - Run Basic Layout tests"
	@echo "  make test-external-partitions - Run External Partition tests"
	@echo "  make test-multi-location      - Run Multi-Location tests"
	@echo "  make test-scenario SCENARIO=standard  - Run specific scenario"
	@echo ""
	@echo "Conversion Evaluation:"
	@echo "  make setup-conversion-sources - Create 21 Glue source tables for conversion testing"
	@echo "  make test-conversion          - Run all conversion evaluation tests"
	@echo "  make test-convert-to-delta    - Test CONVERT TO DELTA method"
	@echo "  make test-shallow-clone       - Test SHALLOW CLONE method"
	@echo "  make test-manual-delta-log    - Test Manual Delta Log method"
	@echo "  make test-converted-tables    - Test post-conversion UC capabilities"
	@echo ""
	@echo "Results:"
	@echo "  make generate-results         - Generate RESULTS_MATRIX.md from test runs"
	@echo "  make generate-conversion-matrix - Generate CONVERSION_MATRIX.md"
	@echo ""
	@echo "Teardown:"
	@echo "  make destroy-databricks - Destroy Databricks resources"
	@echo "  make destroy-aws       - Destroy AWS resources"
	@echo "  make destroy           - Full teardown (Databricks + AWS)"
	@echo "  make clean             - Remove local state and caches"
	@echo ""

# Initialize Terraform
init:
	@echo "==> Initializing Terraform..."
	cd terraform && terraform init
	@echo ""
	@echo "==> Creating Databricks secrets scope (if not exists)..."
	@databricks secrets create-scope your-project-scope 2>/dev/null || echo "Scope already exists"
	@echo ""
	@echo "✓ Initialization complete"

# Deploy AWS resources
deploy-aws:
	@echo "==> Deploying AWS resources with Terraform..."
	cd terraform && terraform apply -auto-approve
	@echo ""
	@echo "✓ AWS resources deployed"

# Deploy Databricks resources
deploy-databricks:
	@echo "==> Extracting Terraform outputs..."
	$(eval ROLE_ARN := $(shell cd terraform && terraform output -raw role_arn))
	$(eval BUCKET_EAST_1A := $(shell cd terraform && terraform output -raw bucket_east_1a))
	$(eval BUCKET_EAST_1B := $(shell cd terraform && terraform output -raw bucket_east_1b))
	$(eval BUCKET_WEST_2 := $(shell cd terraform && terraform output -raw bucket_west_2))
	$(eval AWS_ACCESS_KEY := $(shell cd terraform && terraform output -raw aws_access_key_id))
	$(eval AWS_SECRET_KEY := $(shell cd terraform && terraform output -raw aws_secret_access_key))
	@echo ""
	@echo "==> Storing AWS credentials in Databricks secrets..."
	@databricks secrets put-secret your-project-scope aws_access_key_id --string-value "$(AWS_ACCESS_KEY)"
	@databricks secrets put-secret your-project-scope aws_secret_access_key --string-value "$(AWS_SECRET_KEY)"
	@echo ""
	@echo "==> Deploying Databricks resources with DABs..."
	databricks bundle deploy \
		--var="role_arn=$(ROLE_ARN)" \
		--var="bucket_east_1a=$(BUCKET_EAST_1A)" \
		--var="bucket_east_1b=$(BUCKET_EAST_1B)" \
		--var="bucket_west_2=$(BUCKET_WEST_2)"
	@echo ""
	@echo "✓ Databricks resources deployed"

# Full deployment
deploy: deploy-aws deploy-databricks
	@echo ""
	@echo "========================================="
	@echo "✓ Full stack deployment complete!"
	@echo "========================================="
	@echo ""
	@echo "Next step: make setup-basic-layouts"

# Set up Basic Layout test fixtures
setup-basic-layouts:
	@echo "==> Setting up Basic Layout test fixtures..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments.fixtures import setup_basic_layout_fixtures; setup_basic_layout_fixtures()"
	@echo ""
	@echo "✓ Basic Layout fixtures created"

# Set up External Partition and Multi-Location test fixtures
setup-external-partitions setup-multi-location:
	@echo "==> Setting up External Partition and Multi-Location test fixtures..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments.fixtures import setup_external_and_multi_location_fixtures; setup_external_and_multi_location_fixtures()"
	@echo ""
	@echo "✓ External Partition and Multi-Location fixtures created"

# Backward-compatible aliases (deprecated)
setup-glue-tables: setup-basic-layouts
	@echo "(Note: setup-glue-tables is deprecated, use setup-basic-layouts)"

setup-phase2-fixtures: setup-external-partitions
	@echo "(Note: setup-phase2-fixtures is deprecated, use setup-external-partitions or setup-multi-location)"

# =============================================================================
# Access Method Fixture Setup
# =============================================================================

# Set up glue_as_hive_metastore fixtures
setup-glue-hive:
	@echo "==> Setting up glue_as_hive_metastore fixtures..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments import setup_fixtures_for_access_method, GLUE_AS_HIVE; setup_fixtures_for_access_method(GLUE_AS_HIVE)"
	@echo ""
	@echo "✓ glue_as_hive_metastore fixtures created"

# Set up uc_glue_federation fixtures
setup-glue-fed:
	@echo "==> Setting up uc_glue_federation fixtures..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments import setup_fixtures_for_access_method, UC_GLUE_FEDERATION; setup_fixtures_for_access_method(UC_GLUE_FEDERATION)"
	@echo ""
	@echo "✓ uc_glue_federation fixtures created"

# Set up uc_external_tables (Parquet) fixtures
setup-uc-ext:
	@echo "==> Setting up uc_external_tables (Parquet) fixtures..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments import setup_fixtures_for_access_method, UC_EXTERNAL_HIVE; setup_fixtures_for_access_method(UC_EXTERNAL_HIVE)"
	@echo ""
	@echo "✓ uc_external_tables (Parquet) fixtures created"

# Set up uc_external_tables (Delta) fixtures
setup-uc-delta:
	@echo "==> Setting up uc_external_tables (Delta) fixtures..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments import setup_fixtures_for_access_method, UC_EXTERNAL_DELTA; setup_fixtures_for_access_method(UC_EXTERNAL_DELTA)"
	@echo ""
	@echo "✓ uc_external_tables (Delta) fixtures created"

# Set up all access methods
setup-all-methods:
	@echo "==> Setting up fixtures for ALL access methods..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments import setup_all_access_methods; setup_all_access_methods()"
	@echo ""
	@echo "✓ All access method fixtures created"

# =============================================================================
# Test Targets
# =============================================================================

# Run all tests
test:
	@echo "==> Running all tests..."
	uv run pytest tests/ -v
	@echo ""
	@echo "✓ Tests complete"

# Run Basic Layout tests only
test-basic-layouts:
	@echo "==> Running Basic Layout tests..."
	uv run pytest tests/test_partitions_under_table_root.py tests/test_partitions_in_nested_subdirectories.py -v

# Run External Partition tests only
test-external-partitions:
	@echo "==> Running External Partition tests..."
	uv run pytest tests/test_partitions_outside_table_root.py -v

# Run Multi-Location tests only
test-multi-location:
	@echo "==> Running Multi-Location tests..."
	uv run pytest tests/test_partitions_across_s3_buckets.py tests/test_partitions_across_aws_regions.py tests/test_partitions_shared_between_tables.py -v

# Backward-compatible alias (deprecated)
test-phase2: test-external-partitions test-multi-location
	@echo "(Note: test-phase2 is deprecated, use test-external-partitions or test-multi-location)"

# Run tests for glue_as_hive_metastore access method
test-glue-hive:
	@echo "==> Running glue_as_hive_metastore tests..."
	uv run pytest tests/glue_as_hive_metastore/ -v

# Run tests for uc_glue_federation access method
test-glue-fed:
	@echo "==> Running uc_glue_federation tests..."
	uv run pytest tests/uc_glue_federation/ -v

# Run tests for uc_external_tables access method
test-uc-external:
	@echo "==> Running uc_external_tables tests..."
	uv run pytest tests/uc_external_tables/ -v

# Run OPTIMIZE and VACUUM validation tests
test-delta-operations:
	@echo "==> Running Delta operations tests..."
	uv run pytest tests/delta_operations/ -v

# Generate results matrix from test runs
generate-results:
	@echo "==> Generating results matrix..."
	uv run python -c "from hive_table_experiments import get_results_matrix; m = get_results_matrix(); m.save_markdown('RESULTS_MATRIX.md') if m.results else print('No results to export')"
	@echo ""
	@echo "✓ Results matrix generated (RESULTS_MATRIX.md)"

# Run specific scenario test
test-scenario:
	@if [ -z "$(SCENARIO)" ]; then \
		echo "Error: SCENARIO variable required"; \
		echo "Usage: make test-scenario SCENARIO=standard"; \
		exit 1; \
	fi
	@echo "==> Running $(SCENARIO) scenario tests..."
	uv run pytest tests/test_$(SCENARIO).py -v

# =============================================================================
# Conversion Evaluation Targets
# =============================================================================

# Set up conversion source tables (21 Glue tables)
setup-conversion-sources:
	@echo "==> Setting up conversion source tables (21 tables)..."
	AWS_ACCESS_KEY_ID=$(shell cd terraform && terraform output -raw aws_access_key_id) \
	AWS_SECRET_ACCESS_KEY=$(shell cd terraform && terraform output -raw aws_secret_access_key) \
	AWS_DEFAULT_REGION=us-east-1 \
	HIVE_EVAL_BUCKET_EAST_1A=$(shell cd terraform && terraform output -raw bucket_east_1a) \
	HIVE_EVAL_BUCKET_EAST_1B=$(shell cd terraform && terraform output -raw bucket_east_1b) \
	HIVE_EVAL_BUCKET_WEST_2=$(shell cd terraform && terraform output -raw bucket_west_2) \
	HIVE_EVAL_GLUE_DATABASE=$(shell cd terraform && terraform output -raw glue_database_name) \
	uv run python -c "from hive_table_experiments import setup_conversion_source_tables; setup_conversion_source_tables()"
	@echo ""
	@echo "✓ Conversion source tables created"

# Run CONVERT TO DELTA tests
test-convert-to-delta:
	@echo "==> Running CONVERT TO DELTA tests..."
	uv run pytest tests/conversion_evaluation/test_convert_to_delta.py -v

# Run SHALLOW CLONE tests
test-shallow-clone:
	@echo "==> Running SHALLOW CLONE tests..."
	uv run pytest tests/conversion_evaluation/test_shallow_clone.py -v

# Run Manual Delta Log tests
test-manual-delta-log:
	@echo "==> Running Manual Delta Log tests..."
	uv run pytest tests/conversion_evaluation/test_manual_delta_log.py -v

# Run all conversion tests
test-conversion:
	@echo "==> Running all conversion evaluation tests..."
	uv run pytest tests/conversion_evaluation/ -v

# Run post-conversion UC capability tests
test-converted-tables:
	@echo "==> Running post-conversion UC capability tests..."
	uv run pytest tests/conversion_evaluation/test_converted_tables.py -v

# Generate conversion matrix results
generate-conversion-matrix:
	@echo "==> Generating conversion matrix..."
	uv run python -c "from hive_table_experiments import get_conversion_matrix; m = get_conversion_matrix(); m.save_markdown('CONVERSION_MATRIX.md')"
	@echo ""
	@echo "✓ Conversion matrix generated (CONVERSION_MATRIX.md)"

# Destroy Databricks resources
destroy-databricks:
	@echo "==> Destroying Databricks resources..."
	-databricks bundle destroy
	@echo "==> Removing secrets scope..."
	-databricks secrets delete-scope your-project-scope
	@echo ""
	@echo "✓ Databricks resources destroyed"

# Destroy AWS resources
destroy-aws:
	@echo "==> Destroying AWS resources..."
	@echo "WARNING: This will delete all S3 buckets and their contents!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		cd terraform && terraform destroy -auto-approve; \
		echo ""; \
		echo "✓ AWS resources destroyed"; \
	else \
		echo "Cancelled."; \
	fi

# Full teardown
destroy: destroy-databricks destroy-aws
	@echo ""
	@echo "========================================="
	@echo "✓ Full stack teardown complete"
	@echo "========================================="

# Clean local state
clean:
	@echo "==> Cleaning local state and caches..."
	rm -rf terraform/.terraform
	rm -rf terraform/.terraform.lock.hcl
	rm -rf .databricks
	rm -rf src/hive_table_experiments.egg-info
	rm -rf .pytest_cache
	rm -rf __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo ""
	@echo "✓ Cleaned"

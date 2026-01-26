#!/bin/bash
#
# Test runner script for hive_to_delta integration tests
#
# Prerequisites:
# 1. AWS credentials configured (profile or environment)
# 2. Databricks workspace with Unity Catalog
# 3. Test infrastructure deployed via terraform
# 4. Environment variables set (see below)

set -e

# AWS Configuration
export AWS_PROFILE="${AWS_PROFILE:-aws-sandbox-field-eng_databricks-sandbox-admin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# Databricks Configuration
# Uses ~/.databrickscfg DEFAULT profile by default
# Override with DATABRICKS_CONFIG_PROFILE if needed

# Test Configuration
export HIVE_TO_DELTA_TEST_GLUE_DATABASE="${HIVE_TO_DELTA_TEST_GLUE_DATABASE:-hive_to_delta_test}"
export HIVE_TO_DELTA_TEST_CATALOG="${HIVE_TO_DELTA_TEST_CATALOG:-fe_randy_pitcher_workspace_catalog}"
export HIVE_TO_DELTA_TEST_SCHEMA="${HIVE_TO_DELTA_TEST_SCHEMA:-hive_to_delta_tests}"

# SQL Warehouse ID for cross-bucket/cross-region query verification
# Will auto-detect if not set
# export HIVE_TO_DELTA_TEST_WAREHOUSE_ID="your-warehouse-id"

echo "=== Running hive_to_delta integration tests ==="
echo "AWS Profile: $AWS_PROFILE"
echo "AWS Region: $AWS_REGION"
echo "Glue Database: $HIVE_TO_DELTA_TEST_GLUE_DATABASE"
echo "Target Catalog: $HIVE_TO_DELTA_TEST_CATALOG"
echo "Target Schema: $HIVE_TO_DELTA_TEST_SCHEMA"
echo ""

# Run tests
uv run pytest tests/ -v "$@"

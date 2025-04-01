#!/usr/bin/env python
"""
Shared constants for the project.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Databricks catalogs
UC_SERVERLESS_STORAGE_CATALOG = 'iceberg_overlay_workspace'
UC_STANDARD_STORAGE_CATALOG = 'randy_pitcher_overlay_workspace'

# Schema name for Databricks
SCHEMA_NAME = 'iceberg_test_snowflake_consumption'

# Table names
TABLE_NAME_DELTA = 'severance_delta'
TABLE_NAME_ICEBERG = 'severance_iceberg'

# Snowflake constants
SNOWFLAKE_DATABASE = "unity_catalog_iceberg_db"
SNOWFLAKE_WAREHOUSE = "INTERACTIVE_WH"
SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION = "unity_catalog_serverless_storage"
SNOWFLAKE_STANDARD_CATALOG_INTEGRATION = "unity_catalog_standard_storage"

# Environment variables (no defaults - will fail if not set)
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']

DATABRICKS_WORKSPACE_URL = os.environ['DATABRICKS_WORKSPACE_URL']
DATABRICKS_SP_CLIENT_ID = os.environ['DATABRICKS_SP_CLIENT_ID']
DATABRICKS_SP_CLIENT_SECRET = os.environ['DATABRICKS_SP_CLIENT_SECRET'] 
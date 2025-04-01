#!/usr/bin/env python
"""
Main script for creating Iceberg-compatible tables in Databricks.
"""
import os
from typing import Dict, List, Tuple
from dotenv import load_dotenv
from databricks_ops import (
    get_spark_session,
    validate_connectivity as validate_databricks_connectivity,
    create_schema as create_databricks_schema,
    create_iceberg_compatible_tables,
    verify_table_properties
)
from pyiceberg_ops import (
    validate_connectivity as validate_pyiceberg_connectivity,
    create_files_iceberg_table
)
from snowflake_ops import (
    validate_connectivity as validate_snowflake_connectivity,
    setup_database,
    create_catalog_integrations,
    verify_catalog_integration,
    list_namespaces,
    list_tables,
    create_iceberg_tables
)
from constants import (
    UC_SERVERLESS_STORAGE_CATALOG,
    UC_STANDARD_STORAGE_CATALOG,
    SCHEMA_NAME,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION,
    SNOWFLAKE_STANDARD_CATALOG_INTEGRATION,
)

# Load environment variables
load_dotenv()

def create_tables_in_catalog(catalog_name: str) -> bool:
    """Create tables in the specified catalog.
    
    Returns:
        bool: True if native Iceberg table creation succeeded
    """
    # Get Spark session
    spark, error = get_spark_session()
    if error:
        print(f"❌ Failed to get Spark session: {error}")
        return False
    
    # Create schema
    success, error = create_databricks_schema(spark, catalog_name, SCHEMA_NAME)
    if not success:
        print(f"❌ Failed to create schema: {error}")
        return False
    
    # Create Delta table with Iceberg properties
    success, error = create_iceberg_compatible_tables(spark, catalog_name, SCHEMA_NAME)
    if not success:
        print(f"❌ Failed to create Delta table: {error}")
        return False
    print(f"✅ Created {catalog_name}.{SCHEMA_NAME}.severance_delta")
    
    # Create native Iceberg table
    success, error = create_files_iceberg_table(catalog_name, SCHEMA_NAME)
    if not success:
        print(f"❌ Failed to create native Iceberg table: {error}")
        return False
    print(f"✅ Created {catalog_name}.{SCHEMA_NAME}.severance_iceberg")
    
    return True

def create_tables() -> None:
    """Create tables in Databricks."""
    print("\n===== DATABRICKS OPERATIONS =====\n")
    
    # Validate Databricks connectivity
    success, error = validate_databricks_connectivity()
    if not success:
        print(f"❌ Databricks connectivity check failed: {error}")
        return
    
    # Create tables in serverless storage
    print(f"\n[Creating in serverless catalog ({UC_SERVERLESS_STORAGE_CATALOG})]")
    serverless_iceberg_success = create_tables_in_catalog(UC_SERVERLESS_STORAGE_CATALOG)
    
    # Create tables in standard storage
    print(f"\n[Creating in standard catalog ({UC_STANDARD_STORAGE_CATALOG})]")
    standard_iceberg_success = create_tables_in_catalog(UC_STANDARD_STORAGE_CATALOG)

def setup_snowflake() -> None:
    """Set up Snowflake database and catalog integrations."""
    print("\n===== SNOWFLAKE OPERATIONS =====\n")
    
    # Create database
    success, error = setup_database()
    if not success:
        print(f"❌ Failed to setup database: {error}")
        return
    
    # Create catalog integrations
    success, error = create_catalog_integrations()
    if not success:
        print(f"❌ Failed to create catalog integrations: {error}")
        return
    
    # Verify integrations
    print("\n[Verifying catalog integrations]")
    for integration_name in [SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION, SNOWFLAKE_STANDARD_CATALOG_INTEGRATION]:
        success, error = verify_catalog_integration(integration_name)
        if not success or error:
            print(f"❌ {integration_name} verification failed")
            continue
        print(f"✅ {integration_name} verified")
    
    # Create Iceberg tables
    print("\n[Creating Snowflake tables from Databricks catalogs]")
    create_iceberg_tables()

def test_snowflake_only() -> None:
    """Run only Snowflake-related setup and tests."""
    print("\n===== SNOWFLAKE OPERATIONS ONLY =====\n")
    setup_snowflake()

if __name__ == "__main__":
    # If you want to test only Databricks table creation
    # create_tables()
    
    # If you want to test only Snowflake integration
    # test_snowflake_only()
    
    # If you want to run the full workflow
    create_tables()
    setup_snowflake() 
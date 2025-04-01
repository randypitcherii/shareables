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

# Load environment variables
load_dotenv()

# Constants
UC_SERVERLESS_STORAGE_CATALOG = 'iceberg_overlay_workspace'
UC_STANDARD_STORAGE_CATALOG = 'randy_pitcher_overlay_workspace'
SCHEMA_NAME = 'iceberg_test_snowflake_consumption'

def create_tables() -> None:
    """Create all tables needed for the demo."""
    print("\n[Creating Tables]")
    
    # Get Spark session
    spark, error = get_spark_session()
    if error:
        print(f"❌ Failed to get Spark session: {error}")
        return
    
    # Create tables in each catalog
    for catalog in [UC_SERVERLESS_STORAGE_CATALOG, UC_STANDARD_STORAGE_CATALOG]:
        catalog_type = "serverless storage" if catalog == UC_SERVERLESS_STORAGE_CATALOG else "standard storage"
        print(f"\n[{catalog} ({catalog_type})]")
        
        # Create schema
        success, error = create_databricks_schema(spark, catalog, SCHEMA_NAME)
        if not success:
            print(f"❌ Failed to create schema: {error}")
            continue
        print(f"✅ Created schema {catalog}.{SCHEMA_NAME}")
        
        # Create Delta table with Iceberg properties
        success, error = create_iceberg_compatible_tables(spark, catalog, SCHEMA_NAME)
        if not success:
            print(f"❌ Failed to create Delta table: {error}")
            continue
        print(f"✅ Created Delta table {catalog}.{SCHEMA_NAME}.severance_delta")
        
        # Create native Iceberg table
        success, error = create_files_iceberg_table(catalog, SCHEMA_NAME)
        if not success:
            print(f"❌ Failed to create native Iceberg table: {error}")
            continue
        print(f"✅ Created native Iceberg table {catalog}.{SCHEMA_NAME}.severance_iceberg")

if __name__ == "__main__":
    create_tables() 
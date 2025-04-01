#!/usr/bin/env python
"""
Databricks operations module.
"""
import os
from typing import Dict, Tuple, Optional
from databricks.connect import DatabricksSession
from severance_data import SEVERANCE_CHARACTERS

def get_spark_session() -> Tuple[Optional[object], Optional[str]]:
    """
    Get a Databricks Spark session.
    
    Returns:
        Tuple of (spark_session, error_message)
    """
    try:
        spark = DatabricksSession.builder.getOrCreate()
        return spark, None
    except Exception as e:
        return None, str(e)

def validate_connectivity() -> Tuple[bool, Optional[str]]:
    """
    Validate connectivity to Databricks.
    
    Returns:
        Tuple of (success, error_message)
    """
    spark, error = get_spark_session()
    if error:
        return False, error
    
    try:
        # Run a simple query to validate connection
        spark.sql("SELECT 1").collect()
        return True, None
    except Exception as e:
        return False, str(e)

def create_schema(spark, catalog_name: str, schema_name: str) -> Tuple[bool, Optional[str]]:
    """
    Create a schema in the specified catalog.
    
    Returns:
        Tuple of (success, error_message)
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
        return True, None
    except Exception as e:
        return False, str(e)

def create_iceberg_compatible_tables(spark, catalog_name: str, schema_name: str) -> Tuple[bool, Optional[str]]:
    """
    Create Iceberg-compatible Delta tables in Databricks.
    
    Returns:
        Tuple of (success, error_message)
    """
    try:
        # Drop existing table if it exists (create or replace semantics)
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.severance_delta")
        
        # Create the main characters table as Delta with Iceberg compatibility
        spark.sql(f"""
        CREATE TABLE {catalog_name}.{schema_name}.severance_delta (
            name STRING,
            department STRING,
            position STRING,
            is_management BOOLEAN
        ) 
        USING DELTA
        TBLPROPERTIES (
            'delta.enableIcebergCompatV2' = 'true'
        )
        """)
        
        # Insert sample data into characters table
        values = ", ".join([
            f"('{c[0]}', '{c[1]}', '{c[2]}', {str(c[3]).lower()})"
            for c in SEVERANCE_CHARACTERS
        ])
        
        spark.sql(f"""
        INSERT INTO {catalog_name}.{schema_name}.severance_delta
        VALUES {values}
        """)
        
        return True, None
    except Exception as e:
        return False, str(e)

def verify_table_properties(spark, catalog_name: str, schema_name: str, table_name: str) -> Tuple[bool, Optional[Dict]]:
    """
    Verify table properties to confirm Iceberg compatibility.
    
    Returns:
        Tuple of (success, properties_dict)
    """
    try:
        # Get table properties
        result = spark.sql(f"SHOW TBLPROPERTIES {catalog_name}.{schema_name}.{table_name}")
        
        # Convert to dictionary
        props = {row['key']: row['value'] for row in result.collect()}
        
        return True, props
    except Exception as e:
        return False, str(e) 
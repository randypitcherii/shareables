#!/usr/bin/env python
"""
Snowflake connectivity and operations module.
"""
import os
import snowflake.connector
from contextlib import contextmanager
from typing import Tuple, Optional, List
from constants import (
    UC_SERVERLESS_STORAGE_CATALOG,
    UC_STANDARD_STORAGE_CATALOG,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION,
    SNOWFLAKE_STANDARD_CATALOG_INTEGRATION,
    SCHEMA_NAME,
    TABLE_NAME_DELTA,
    TABLE_NAME_ICEBERG,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    DATABRICKS_WORKSPACE_URL,
    DATABRICKS_SP_CLIENT_ID,
    DATABRICKS_SP_CLIENT_SECRET,
)

@contextmanager
def get_snowflake_connection():
    """Get a connection to Snowflake."""
    conn = None
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE
        )
        yield conn
    except Exception as e:
        raise e
    finally:
        if conn:
            conn.close()

def validate_connectivity() -> Tuple[bool, Optional[str]]:
    """Validate connectivity to Snowflake."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True, None
    except Exception as e:
        return False, str(e)

def setup_database() -> Tuple[bool, Optional[str]]:
    """Create or replace the database and schemas."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            # Set warehouse
            cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            
            # Create or replace database
            cursor.execute(f"CREATE OR REPLACE DATABASE {SNOWFLAKE_DATABASE}")
            
            # Drop public schema
            cursor.execute(f"DROP SCHEMA IF EXISTS {SNOWFLAKE_DATABASE}.public")
            
            # Create schemas based on catalog integration names
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STANDARD_CATALOG_INTEGRATION}")
            
            return True, None
    except Exception as e:
        return False, str(e)

def create_catalog_integrations() -> Tuple[bool, Optional[str]]:
    """Create catalog integrations for both Databricks catalogs."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            # Create serverless storage catalog integration
            cursor.execute(f"""
            CREATE OR REPLACE CATALOG INTEGRATION {SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION}
                CATALOG_SOURCE = ICEBERG_REST
                TABLE_FORMAT = ICEBERG
                REST_CONFIG = (
                    CATALOG_URI = '{DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest'
                    CATALOG_NAME = '{UC_SERVERLESS_STORAGE_CATALOG}'
                    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
                )
                REST_AUTHENTICATION = (
                    TYPE = OAUTH
                    OAUTH_TOKEN_URI = '{DATABRICKS_WORKSPACE_URL}/oidc/v1/token'
                    OAUTH_CLIENT_ID = '{DATABRICKS_SP_CLIENT_ID}'
                    OAUTH_CLIENT_SECRET = '{DATABRICKS_SP_CLIENT_SECRET}'
                    OAUTH_ALLOWED_SCOPES = ('all-apis')
                )
                ENABLED = TRUE
            """)
            
            # Create standard storage catalog integration
            cursor.execute(f"""
            CREATE OR REPLACE CATALOG INTEGRATION {SNOWFLAKE_STANDARD_CATALOG_INTEGRATION}
                CATALOG_SOURCE = ICEBERG_REST
                TABLE_FORMAT = ICEBERG
                REST_CONFIG = (
                    CATALOG_URI = '{DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest'
                    CATALOG_NAME = '{UC_STANDARD_STORAGE_CATALOG}'
                    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
                )
                REST_AUTHENTICATION = (
                    TYPE = OAUTH
                    OAUTH_TOKEN_URI = '{DATABRICKS_WORKSPACE_URL}/oidc/v1/token'
                    OAUTH_CLIENT_ID = '{DATABRICKS_SP_CLIENT_ID}'
                    OAUTH_CLIENT_SECRET = '{DATABRICKS_SP_CLIENT_SECRET}'
                    OAUTH_ALLOWED_SCOPES = ('all-apis')
                )
                ENABLED = TRUE
            """)
            
            return True, None
    except Exception as e:
        return False, str(e)

def verify_catalog_integration(integration_name: str) -> Tuple[bool, Optional[str]]:
    """Verify a catalog integration is working."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('{integration_name}')")
            result = cursor.fetchone()
            
            # Parse the JSON response
            response = result[0]
            if '"success" : true' in response:
                return True, None
            return True, response
    except Exception as e:
        return False, str(e)

def list_namespaces(integration_name: str) -> Tuple[bool, Optional[List[str]], Optional[str]]:
    """List namespaces available in the catalog integration."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT SYSTEM$LIST_NAMESPACES_FROM_CATALOG('{integration_name}')")
            result = cursor.fetchall()
            namespaces = [row[0] for row in result]
            return True, namespaces, None
    except Exception as e:
        return False, None, str(e)

def list_tables(integration_name: str, namespace: str) -> Tuple[bool, Optional[List[str]], Optional[str]]:
    """List tables in the specified namespace."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            # If namespace is a JSON array string, extract just the schema name we want
            if namespace.startswith('[') and namespace.endswith(']'):
                # Parse the JSON string and find our schema
                import json
                namespaces = json.loads(namespace)
                # Use our SCHEMA_NAME constant if it exists in the list
                if SCHEMA_NAME in namespaces:
                    namespace = SCHEMA_NAME
                else:
                    # Use the first valid schema name
                    for ns in namespaces:
                        if not ns.startswith('"') and ns != "information_schema":
                            namespace = ns
                            break
            
            cursor.execute(f"SELECT SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG('{integration_name}', '{namespace}', 0)")
            result = cursor.fetchall()
            tables = [row[0] for row in result]
            return True, tables, None
    except Exception as e:
        return False, None, str(e)

def create_iceberg_tables() -> Tuple[bool, Optional[str]]:
    """Create Iceberg tables in Snowflake using catalog integrations."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            # Tables to create - each tuple is (integration_name, snowflake_schema_name, source_table)
            tables_to_create = [
                # Serverless catalog tables
                (SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION, SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION, TABLE_NAME_ICEBERG),
                (SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION, SNOWFLAKE_SERVERLESS_CATALOG_INTEGRATION, TABLE_NAME_DELTA),
                
                # Standard catalog tables
                (SNOWFLAKE_STANDARD_CATALOG_INTEGRATION, SNOWFLAKE_STANDARD_CATALOG_INTEGRATION, TABLE_NAME_ICEBERG),
                (SNOWFLAKE_STANDARD_CATALOG_INTEGRATION, SNOWFLAKE_STANDARD_CATALOG_INTEGRATION, TABLE_NAME_DELTA)
            ]
            
            for integration_name, snowflake_schema_name, source_table in tables_to_create:
                try:
                    cursor.execute(f"""
                    CREATE ICEBERG TABLE {SNOWFLAKE_DATABASE}.{snowflake_schema_name}.{source_table}
                    CATALOG            = {integration_name}
                    CATALOG_NAMESPACE  = '{SCHEMA_NAME}'
                    CATALOG_TABLE_NAME = '{source_table}'
                    """)
                except Exception as table_error:
                    # Log error but continue with other tables
                    print(f"❌ Failed to create {source_table} in {snowflake_schema_name}: {str(table_error)}")
                    continue
                
                print(f"✅ Created {snowflake_schema_name}.{source_table}")
            
            return True, None
    except Exception as e:
        return False, str(e) 
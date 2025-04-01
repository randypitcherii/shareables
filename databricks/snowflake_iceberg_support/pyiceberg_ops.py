#!/usr/bin/env python
"""
PyIceberg connectivity and operations module.
"""
import os
from typing import Dict, Tuple, Optional
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from severance_data import SEVERANCE_CHARACTERS

def get_pyiceberg_catalog(catalog_name: str) -> Tuple[Optional[object], Optional[str]]:
    """
    Get a PyIceberg catalog instance using proper authentication.
    
    Returns:
        Tuple of (catalog, error_message)
    """
    try:
        workspace_url = os.environ.get('DATABRICKS_WORKSPACE_URL', '')
        client_id = os.environ.get('DATABRICKS_SP_CLIENT_ID', '')
        client_secret = os.environ.get('DATABRICKS_SP_CLIENT_SECRET', '')
        
        if not all([workspace_url, client_id, client_secret]):
            return None, "Missing required environment variables for Databricks authentication"
        
        # Configure PyIceberg catalog using REST catalog specification with OAuth
        catalog_uri = f'{workspace_url}/api/2.1/unity-catalog/iceberg-rest'
        oauth_uri = f'{workspace_url}/oidc/v1/token'
        oauth_credential = f'{client_id}:{client_secret}'  # standard Iceberg REST API formatting
        
        catalog_props = {
            'type': 'rest',
            'uri': catalog_uri,
            'oauth2-server-uri': oauth_uri,
            'credential': oauth_credential,
            'warehouse': catalog_name,
            'scope': 'all-apis sql'
        }
        
        # Load the catalog
        catalog = load_catalog(**catalog_props)
        return catalog, None
    except Exception as e:
        return None, str(e)

def validate_connectivity(catalog_name: str) -> Tuple[bool, Optional[str]]:
    """
    Validate connectivity to PyIceberg.
    
    Returns:
        Tuple of (success, error_message)
    """
    catalog, error = get_pyiceberg_catalog(catalog_name)
    if error:
        return False, error
    
    try:
        # List namespaces to validate connection
        catalog.list_namespaces()
        return True, None
    except Exception as e:
        return False, str(e)

def create_files_iceberg_table(catalog_name: str, schema_name: str) -> Tuple[bool, Optional[str]]:
    """
    Create a native Iceberg table using PyIceberg and PyArrow.
    
    Returns:
        Tuple of (success, error_message)
    """
    catalog, error = get_pyiceberg_catalog(catalog_name)
    if error:
        return False, error
    
    try:
        # Create a PyArrow dataframe with our sample data
        names = [c[0] for c in SEVERANCE_CHARACTERS]
        departments = [c[1] for c in SEVERANCE_CHARACTERS]
        positions = [c[2] for c in SEVERANCE_CHARACTERS]
        is_management = [c[3] for c in SEVERANCE_CHARACTERS]
        
        # Create PyArrow table with proper column names
        pa_table = pa.Table.from_arrays(
            [pa.array(names), pa.array(departments), pa.array(positions), pa.array(is_management)],
            names=["name", "department", "position", "is_management"]
        )
        
        # Create namespace if it doesn't exist
        try:
            catalog.create_namespace(schema_name)
        except Exception:
            pass  # Namespace likely exists
        
        # Create the table
        identifier = f"{schema_name}.severance_iceberg"
        
        # Drop existing table if it exists
        try:
            catalog.drop_table(identifier)
        except Exception:
            pass  # Table likely doesn't exist
            
        # Create and populate the table
        table = catalog.create_table(
            identifier=identifier,
            schema=pa_table.schema
        )
        table.append(pa_table)
        
        return True, None
    except Exception as e:
        return False, str(e) 
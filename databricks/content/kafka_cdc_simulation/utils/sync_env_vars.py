#!/usr/bin/env python3
"""
Sync environment variables from ./env file to Databricks secrets scope.

This script reads environment variables from a local ./env file and stores them
in a Databricks secrets scope. It drops and recreates the scope each time to 
avoid zombie secrets.

Usage:
    uv run python utils/sync_env_vars.py

Requirements:
    - databricks-sdk package
    - Databricks authentication configured (via env vars, config profile, etc.)
    - ./env file with key=value pairs
"""

import sys
from pathlib import Path
from typing import Dict
import logging

from databricks.sdk import WorkspaceClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SCOPE_NAME = "randy_pitcher_workspace_kafka_cdc"
ENV_FILE_PATH = "./.env"


def read_env_file(file_path: str) -> Dict[str, str]:
    """
    Read environment variables from a file.
    
    Args:
        file_path: Path to the environment file
        
    Returns:
        Dictionary of key-value pairs from the env file
        
    Raises:
        FileNotFoundError: If the env file doesn't exist
        ValueError: If the env file format is invalid
    """
    env_vars = {}
    env_file = Path(file_path)
    
    if not env_file.exists():
        raise FileNotFoundError(f"Environment file not found: {file_path}")
    
    logger.info(f"Reading environment variables from {file_path}")
    
    with open(env_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
                
            # Parse key=value pairs
            if '=' not in line:
                logger.warning(f"Skipping invalid line {line_num}: {line}")
                continue
                
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Remove quotes if present
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
                
            if key:
                env_vars[key] = value
                logger.debug(f"Found variable: {key}")
            else:
                logger.warning(f"Skipping line {line_num} with empty key")
    
    logger.info(f"Found {len(env_vars)} environment variables")
    return env_vars


def scope_exists(client: WorkspaceClient, scope_name: str) -> bool:
    """
    Check if a secrets scope exists.
    
    Args:
        client: Databricks WorkspaceClient
        scope_name: Name of the scope to check
        
    Returns:
        True if scope exists, False otherwise
    """
    try:
        scopes = client.secrets.list_scopes()
        return any(scope.name == scope_name for scope in scopes)
    except Exception as e:
        logger.error(f"Error checking if scope exists: {e}")
        return False


def delete_scope_if_exists(client: WorkspaceClient, scope_name: str) -> None:
    """
    Delete a secrets scope if it exists.
    
    Args:
        client: Databricks WorkspaceClient
        scope_name: Name of the scope to delete
    """
    try:
        if scope_exists(client, scope_name):
            logger.info(f"Deleting existing scope: {scope_name}")
            client.secrets.delete_scope(scope=scope_name)
            logger.info(f"Successfully deleted scope: {scope_name}")
        else:
            logger.info(f"Scope {scope_name} does not exist, nothing to delete")
    except Exception as e:
        logger.error(f"Error deleting scope {scope_name}: {e}")
        raise


def create_scope(client: WorkspaceClient, scope_name: str) -> None:
    """
    Create a new secrets scope.
    
    Args:
        client: Databricks WorkspaceClient
        scope_name: Name of the scope to create
    """
    try:
        logger.info(f"Creating new scope: {scope_name}")
        client.secrets.create_scope(scope=scope_name)
        logger.info(f"Successfully created scope: {scope_name}")
    except Exception as e:
        logger.error(f"Error creating scope {scope_name}: {e}")
        raise


def store_secrets(client: WorkspaceClient, scope_name: str, env_vars: Dict[str, str]) -> None:
    """
    Store environment variables as secrets in the scope.
    
    Args:
        client: Databricks WorkspaceClient
        scope_name: Name of the scope to store secrets in
        env_vars: Dictionary of environment variables to store
    """
    logger.info(f"Storing {len(env_vars)} secrets in scope: {scope_name}")
    
    for key, value in env_vars.items():
        try:
            logger.debug(f"Storing secret: {key}")
            client.secrets.put_secret(
                scope=scope_name,
                key=key,
                string_value=value
            )
        except Exception as e:
            logger.error(f"Error storing secret {key}: {e}")
            raise
    
    logger.info(f"Successfully stored all {len(env_vars)} secrets")


def sync_env_vars_to_databricks(env_file_path: str = ENV_FILE_PATH, 
                               scope_name: str = SCOPE_NAME) -> None:
    """
    Main function to sync environment variables to Databricks secrets.
    
    Args:
        env_file_path: Path to the environment file
        scope_name: Name of the Databricks secrets scope
    """
    try:
        # Read environment variables from file
        env_vars = read_env_file(env_file_path)
        
        if not env_vars:
            logger.warning("No environment variables found to sync")
            return
        
        # Initialize Databricks client
        logger.info("Initializing Databricks WorkspaceClient")
        client = WorkspaceClient()
        
        # Delete existing scope to avoid zombie secrets
        delete_scope_if_exists(client, scope_name)
        
        # Create new scope
        create_scope(client, scope_name)
        
        # Store all secrets
        store_secrets(client, scope_name, env_vars)
        
        logger.info("✅ Successfully synced environment variables to Databricks secrets")
        logger.info(f"Scope: {scope_name}")
        logger.info(f"Secrets stored: {len(env_vars)}")
        
    except FileNotFoundError as e:
        logger.error(f"❌ Environment file error: {e}")
        logger.info(f"Please create an environment file at {env_file_path} with key=value pairs")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error syncing environment variables: {e}")
        sys.exit(1)


def main():
    """Entry point for the script."""
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print(__doc__)
            return
        elif sys.argv[1] in ['-v', '--verbose']:
            logging.getLogger().setLevel(logging.DEBUG)
    
    sync_env_vars_to_databricks()


if __name__ == "__main__":
    main() 
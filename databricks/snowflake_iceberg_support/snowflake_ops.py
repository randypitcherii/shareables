#!/usr/bin/env python
"""
Snowflake connectivity and operations module.
"""
import os
import snowflake.connector
from contextlib import contextmanager

@contextmanager
def get_snowflake_connection():
    """Get a connection to Snowflake."""
    conn = None
    try:
        conn = snowflake.connector.connect(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        )
        yield conn
    except Exception as e:
        raise e
    finally:
        if conn:
            conn.close()

def validate_connectivity():
    """Validate connectivity to Snowflake."""
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            return True, None
    except Exception as e:
        return False, f"Failed to connect to Snowflake: {str(e)}" 
-- ============================================================================
-- SNOWFLAKE READS FROM UNITY CATALOG WITH VENDED CREDENTIAL SUPPORT DEMO
-- Date: April 2, 2025
-- ============================================================================

-- -----------------------------------------------------------------------------
-- STEP 1: CREATE AND SETUP THE DATABASE
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Set the warehouse to use
USE WAREHOUSE INTERACTIVE_WH;

-- Create the database and drop the public schema
CREATE OR REPLACE DATABASE unity_catalog_iceberg_db;
DROP SCHEMA IF EXISTS unity_catalog_iceberg_db.public;

-- Create a schema for the standard catalog integration
CREATE SCHEMA IF NOT EXISTS unity_catalog_iceberg_db.databricks_unity_catalog;

-- -----------------------------------------------------------------------------
-- STEP 2: CREATE CATALOG INTEGRATION FOR UNITY CATALOG
-- -----------------------------------------------------------------------------

-- Create standard storage catalog integration
CREATE OR REPLACE CATALOG INTEGRATION databricks_unity_catalog
    CATALOG_SOURCE = ICEBERG_REST
    TABLE_FORMAT = ICEBERG
    REST_CONFIG = (
        CATALOG_URI = 'https://your-workspace.databricks.com/api/2.1/unity-catalog/iceberg-rest'
        CATALOG_NAME = 'randy_pitcher_overlay_workspace'
        ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
    )
    REST_AUTHENTICATION = (
        TYPE = OAUTH
        OAUTH_TOKEN_URI = 'https://your-workspace.databricks.com/oidc/v1/token'
        OAUTH_CLIENT_ID = '🤫 shhhh'
        OAUTH_CLIENT_SECRET = '🤫 shhhh'
        OAUTH_ALLOWED_SCOPES = ('all-apis')
    )
    ENABLED = TRUE;

-- -----------------------------------------------------------------------------
-- STEP 3: VERIFY CATALOG INTEGRATION
-- -----------------------------------------------------------------------------

-- Verify the catalog integration is working
SELECT 
    parse_json(SYSTEM$VERIFY_CATALOG_INTEGRATION('databricks_unity_catalog')) as verification, 
    iff(verification:"errorCode" = '', '✅ Success!', '❌ Error: \n' || verification:"errorMessage") as status_message;

-- -----------------------------------------------------------------------------
-- STEP 4: EXPLORE AVAILABLE SCHEMAS AND TABLES
-- -----------------------------------------------------------------------------

-- List namespaces available in the catalog
SELECT SYSTEM$LIST_NAMESPACES_FROM_CATALOG('databricks_unity_catalog');

-- List tables in the schema
SELECT SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG(
    'databricks_unity_catalog', 
    'iceberg_test_snowflake_consumption', 
    0
);

-- -----------------------------------------------------------------------------
-- STEP 5: CREATE ICEBERG TABLES IN SNOWFLAKE
-- -----------------------------------------------------------------------------

-- Create the severance_iceberg table
CREATE ICEBERG TABLE 
    unity_catalog_iceberg_db.databricks_unity_catalog.severance_iceberg
    CATALOG            = databricks_unity_catalog
    CATALOG_NAMESPACE  = 'iceberg_test_snowflake_consumption'
    CATALOG_TABLE_NAME = 'severance_iceberg';

-- Create the severance_delta table 
CREATE ICEBERG TABLE 
    unity_catalog_iceberg_db.databricks_unity_catalog.severance_delta
    CATALOG            = databricks_unity_catalog
    CATALOG_NAMESPACE  = 'iceberg_test_snowflake_consumption'
    CATALOG_TABLE_NAME = 'severance_delta';

-- -----------------------------------------------------------------------------
-- STEP 6: QUERY THE TABLES
-- -----------------------------------------------------------------------------

-- Query the severance_iceberg table
SELECT * FROM unity_catalog_iceberg_db.databricks_unity_catalog.severance_iceberg
LIMIT 10;

-- Query the severance_delta table
SELECT * FROM unity_catalog_iceberg_db.databricks_unity_catalog.severance_delta
LIMIT 10;

-- -----------------------------------------------------------------------------
-- STEP 7: CLEANUP (OPTIONAL)
-- -----------------------------------------------------------------------------

-- Drop the database (this will cascade to all schemas and tables)
DROP DATABASE IF EXISTS unity_catalog_iceberg_db;

-- Drop the catalog integration
DROP CATALOG INTEGRATION IF EXISTS databricks_unity_catalog; 
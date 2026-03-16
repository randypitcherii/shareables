# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Iceberg Vended Credentials Demo
# MAGIC **Date: April 2, 2025**
# MAGIC 
# MAGIC This notebook demonstrates how to create Iceberg tables in Unity Catalog that can be accessed by Snowflake using vended credentials.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Install necessary packages and configure environment

# COMMAND ----------

# MAGIC %pip install pyiceberg==0.9.0
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema and Delta Table
# MAGIC Create the schema and Delta table in Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the schema
# MAGIC DROP SCHEMA IF EXISTS randy_pitcher_overlay_workspace.iceberg_test_snowflake_consumption CASCADE;
# MAGIC CREATE SCHEMA IF NOT EXISTS randy_pitcher_overlay_workspace.iceberg_test_snowflake_consumption;
# MAGIC 
# MAGIC -- Create the severance_delta table
# MAGIC CREATE OR REPLACE TABLE 
# MAGIC   randy_pitcher_overlay_workspace.iceberg_test_snowflake_consumption.severance_delta 
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   department STRING,
# MAGIC   salary DOUBLE,
# MAGIC   hire_date DATE
# MAGIC )
# MAGIC 
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC );
# MAGIC 
# MAGIC 
# MAGIC -- Insert some sample data
# MAGIC INSERT INTO randy_pitcher_overlay_workspace.iceberg_test_snowflake_consumption.severance_delta
# MAGIC VALUES 
# MAGIC   (1, 'Mark Scout', 'Macro Data Refinement', 120000.00, '2020-02-01'),
# MAGIC   (2, 'Helly Riggs', 'Macro Data Refinement', 85000.00, '2022-01-15'),
# MAGIC   (3, 'Dylan George', 'Macro Data Refinement', 95000.00, '2019-11-12'),
# MAGIC   (4, 'Irving Bailiff', 'Macro Data Refinement', 110000.00, '2017-05-22'),
# MAGIC   (5, 'Harmony Cobel', 'Management', 180000.00, '2015-08-30'),
# MAGIC   (6, 'Seth Milchick', 'Management', 160000.00, '2016-07-03'),
# MAGIC   (7, 'Burt Goodman', 'Optics and Design', 145000.00, '2018-04-17'),
# MAGIC   (8, 'Felicia Judd', 'Security', 125000.00, '2019-09-05'),
# MAGIC   (9, 'Natalie Ellis', 'Wellness', 135000.00, '2020-03-22'),
# MAGIC   (10, 'Devon Hockett', 'Optics and Design', 115000.00, '2021-06-14');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Iceberg Table Using PyIceberg
# MAGIC Now we'll create an Iceberg table using the PyIceberg library with PyArrow integration

# COMMAND ----------

# Import necessary libraries
import os
import pyarrow as pa
import datetime
from pyiceberg.catalog import load_catalog

# Get PAT dynamically in the Databricks notebook
databricks_pat = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

# Set up catalog configuration using PAT instead of OAuth
catalog_config = {
    'type': 'rest',
    "uri": f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/api/2.1/unity-catalog/iceberg-rest",
    "warehouse": "randy_pitcher_overlay_workspace",
    "token": databricks_pat
}

# Load the catalog
catalog = load_catalog("rest", **catalog_config)

# COMMAND ----------

# Create sample data with PyArrow
# Create arrays for each column
ids = pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
names = pa.array([
    'Mark Scout', 'Helly Riggs', 'Dylan George', 'Irving Bailiff', 
    'Harmony Cobel', 'Seth Milchick', 'Burt Goodman', 'Felicia Judd',
    'Natalie Ellis', 'Devon Hockett'
])
departments = pa.array([
    'Macro Data Refinement', 'Macro Data Refinement', 'Macro Data Refinement', 'Macro Data Refinement',
    'Management', 'Management', 'Optics and Design', 'Security',
    'Wellness', 'Optics and Design'
])
salaries = pa.array([
    120000.00, 85000.00, 95000.00, 110000.00,
    180000.00, 160000.00, 145000.00, 125000.00,
    135000.00, 115000.00
])
hire_dates = pa.array([
    datetime.date(2020, 2, 1), datetime.date(2022, 1, 15), datetime.date(2019, 11, 12), datetime.date(2017, 5, 22),
    datetime.date(2015, 8, 30), datetime.date(2016, 7, 3), datetime.date(2018, 4, 17), datetime.date(2019, 9, 5),
    datetime.date(2020, 3, 22), datetime.date(2021, 6, 14)
])

# Create PyArrow table
arrow_table = pa.Table.from_arrays(
    [ids, names, departments, salaries, hire_dates], 
    names=['id', 'name', 'department', 'salary', 'hire_date']
)

# Create the Iceberg table
iceberg_table_name = "severance_iceberg"
namespace = "iceberg_test_snowflake_consumption"

# Check if table exists and replace if it does
try:
    catalog.drop_table(f"{namespace}.{iceberg_table_name}")
except Exception as e:
    if "NoSuchTableException" not in str(e):
        print(f"❌ Error removing existing table: {str(e)}")
        raise e

# Create the table using the PyArrow table schema directly
try:
    table = catalog.create_table(
        identifier=f"{namespace}.{iceberg_table_name}",
        schema=arrow_table.schema
    )
    
    # Write data to the Iceberg table
    table.append(arrow_table)
    
    print(f"✅ Successfully created Iceberg table and added {len(arrow_table)} records")
except Exception as e:
    print(f"❌ Error creating or writing to Iceberg table: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables
# MAGIC Let's query both tables to verify they were created successfully

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Delta table
# MAGIC SELECT * FROM randy_pitcher_overlay_workspace.iceberg_test_snowflake_consumption.severance_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Iceberg table
# MAGIC SELECT * FROM randy_pitcher_overlay_workspace.iceberg_test_snowflake_consumption.severance_iceberg 
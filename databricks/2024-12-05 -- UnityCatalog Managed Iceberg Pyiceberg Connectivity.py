# Databricks notebook source
# MAGIC %pip install pyiceberg 
# MAGIC %pip install --upgrade pydantic 
# MAGIC %restart_python 

# COMMAND ----------

from pyiceberg.catalog import load_catalog

# get UC iceberg catalog details
UC_CATALOG = 'randy_pitcher_workspace'
UC_CREDENTIAL  = (
  dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)
UC_DATABRICKS_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
UC_CATALOG_URI = f'https://{UC_DATABRICKS_URL}/api/2.1/unity-catalog/iceberg-rest'

# connect
uc_catalog_properties = {
  'type':      'rest',
  'uri':       UC_CATALOG_URI,
  'token':     UC_CREDENTIAL,
  'warehouse': UC_CATALOG
}
uc_catalog = load_catalog(**uc_catalog_properties)


# COMMAND ----------

import time

for ns in uc_catalog.list_namespaces():
  print(f'Found schema: {ns[0]}')
  # time.sleep(1)
  for table in uc_catalog.list_tables(ns[0]):
    print(f'\t{table[0]}.{table[1]}')
  print('\n')
  time.sleep(1)


# COMMAND ----------

uc_catalog.create_namespace_if_not_exists('iceberg_3p_writes')

import pyarrow as pa
import pyarrow.dataset as ds

data = {
    "planet": ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"],
    "diameter_km": [4879, 12104, 12742, 6779, 139820, 116460, 50724, 49244],
    "has_rings": [False, False, False, False, True, True, True, True]
}

pa_table = pa.Table.from_pydict(data)

uc_table = uc_catalog.create_table_if_not_exists(
  identifier="iceberg_3p_writes.planets",
  schema=pa_table.schema
)

uc_table.append(pa_table)

# COMMAND ----------

df = spark.table(f'{UC_CATALOG}.iceberg_3p_writes_macbook.planets')
display(df)

# COMMAND ----------

# MAGIC %%sql
# MAGIC drop schema randy_pitcher_overlay_workspace.iceberg_3p_writes_macbook cascade;

# COMMAND ----------

# DBTITLE 1,Modify a managed iceberg table that dbsql created
import pyarrow as pa
import pyarrow.dataset as ds

uc_table = uc_catalog.load_table("iceberg_test_snowflake_consumption.space_stuff")


# COMMAND ----------

tbl = uc_table.scan().to_arrow()

# COMMAND ----------

uc_table.append(tbl)

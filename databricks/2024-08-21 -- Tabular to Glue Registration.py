# Databricks notebook source
# MAGIC %pip install pyiceberg[glue,s3] pyarrow
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

import os, time
from pprint import pprint
from pyiceberg.catalog import load_catalog

# get tabular catalog details
TABULAR_CREDENTIAL       = dbutils.secrets.get(scope="randy_pitcher_workspace_tabular", key="tabular_credential")
TABULAR_TARGET_WAREHOUSE = 'rpw_aws_us_east_1' # replace this with your tabular warehouse name
TABULAR_CATALOG_URI      = 'https://api.tabular.io/ws' # unless you're a single tenant user, you don't need to change this

# get glue catalog details
AWS_REGION           = 'us-east-1'
GLUE_USER_SECRET_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_sandbox_glue_user_secret_key")
GLUE_USER_ACCESS_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_sandbox_glue_user_access_key")

# load tabular catalog
tabular_catalog_properties = {
    'uri':        TABULAR_CATALOG_URI,
    'credential': TABULAR_CREDENTIAL,
    'warehouse':  TABULAR_TARGET_WAREHOUSE
}
tabular_catalog = load_catalog(**tabular_catalog_properties)

# load tabular catalog
glue_catalog_properties = {
    "type": "glue",
    "client.access-key-id": GLUE_USER_ACCESS_KEY,
    "client.secret-access-key": GLUE_USER_SECRET_KEY,
    "client.region": AWS_REGION
}
glue_catalog = load_catalog(**glue_catalog_properties)


# COMMAND ----------

namespaces_to_register = [namespace[0] for namespace in tabular_catalog.list_namespaces() if namespace[0] not in ['system', 'examples']]

if namespaces_to_register:
  glue_catalog.create_namespace_if_not_exists(TABULAR_TARGET_WAREHOUSE)

for namespace in namespaces_to_register:
  print(f"Found namespace to register: {namespace}")
  tables_to_register = [table[1] for table in tabular_catalog.list_tables(namespace)]
  for table in tables_to_register:
    try:
      tabular_table_name = f"{namespace}.{table}"
      tabular_table = tabular_catalog.load_table(f"{tabular_table_name}")
      glue_table_name =  f"{TABULAR_TARGET_WAREHOUSE}.{namespace}__{table}"  
      print(f"  - Registering tabular table {tabular_table_name} -> {glue_table_name}")
      glue_catalog.register_table(glue_table_name, tabular_table.metadata_location)
      print(f"  - ✅ Successfully registered {tabular_table_name} -> {glue_table_name}")
    except Exception as e:
      print(f"\t❌ Failure while processing {tabular_table_name} - {e}")
  print('\n\n')

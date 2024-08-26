# Databricks notebook source
# MAGIC %pip install pyiceberg[glue,s3]==0.7.1 pyarrow
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

import os, time
from pprint import pprint
from pyiceberg.catalog import load_catalog

# get tabular catalog details
TABULAR_CREDENTIAL  = dbutils.secrets.get(scope="randy_pitcher_workspace_tabular", key="tabular_credential")
TABULAR_CATALOG_URI = 'https://api.tabular.io/ws' # unless you're a single tenant user, you don't need to change this

# get glue catalog details
AWS_REGION           = 'us-east-1'
GLUE_USER_SECRET_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_sandbox_glue_user_secret_key")
GLUE_USER_ACCESS_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_sandbox_glue_user_access_key")


# COMMAND ----------

def disarm_tabular_services_on_table(pyiceberg_table):
  with pyiceberg_table.transaction() as tx:
    tx.set_properties({
      'history.expire.max-snapshot-age-ms': 94608000000, # 3 year retention time should prevent snapshot cleanup
      'compaction.enabled':                 'false',     # Disable compaction
      'manifest-rewrite.enabled':           'false',     # Disable manifest rewriting
      'optimizer.enabled':                  'false',     # Disable automatic optimizations
      'lifecycle.enabled':                  'false',     # Disable data lifecycle functionality
      'fileloader.enabled':                 'false',     # Turn off the file loader
      'dependent-tables':                   '',          # Disable CDC by destroying dependent-table references
    })
  
def migrate_tabular_namespace_to_glue(tabular_namespace, tabular_catalog, glue_namespace, glue_catalog, should_disarm_tabular_tables=False):
  tabular_namespace_properties = tabular_catalog.load_namespace_properties(tabular_namespace)
  glue_catalog.create_namespace_if_not_exists(glue_namespace, {'location': tabular_namespace_properties['location']})

  print(f"Found namespace to register: {tabular_namespace}")
  tables_to_register = [table[1] for table in tabular_catalog.list_tables(tabular_namespace)]
  for table in tables_to_register:
    try:
      tabular_table_name = f"{tabular_namespace}.{table}"
      tabular_table = tabular_catalog.load_table(f"{tabular_table_name}")

      # 
      disarm_tabular_services_on_table(tabular_table)

      glue_table_name =  f"{glue_namespace}.{tabular_namespace}__{table}"  
      print(f"  - Registering tabular table {tabular_table_name} -> {glue_table_name}")
      glue_catalog.register_table(glue_table_name, tabular_table.metadata_location)
      print(f"  - ✅ Successfully registered {tabular_table_name} -> {glue_table_name}")
    except Exception as e:
      print(f"\t❌ Failure while processing {tabular_table_name} - {e}")


def migrate_tabular_warehouse_to_glue(tabular_warehouse_name, tabular_catalog, glue_catalog, should_disarm_tabular_tables=False):
  namespaces_to_register = [namespace[0] for namespace in tabular_catalog.list_namespaces() if namespace[0] not in ['system', 'examples']]

  for namespace in namespaces_to_register:
    migrate_tabular_namespace_to_glue(namespace, tabular_catalog, tabular_warehouse_name, glue_catalog, should_disarm_tabular_tables)
    print('\n')

# COMMAND ----------

def main():
  tabular_warehouse_name = 'rpw_aws_us_east_1' # replace this with your tabular warehouse name
  # load tabular catalog
  tabular_catalog_properties = {
      'uri':        TABULAR_CATALOG_URI,
      'credential': TABULAR_CREDENTIAL,
      'warehouse':  tabular_warehouse_name
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

  migrate_tabular_warehouse_to_glue(tabular_warehouse_name, tabular_catalog, glue_catalog)


# COMMAND ----------

main()

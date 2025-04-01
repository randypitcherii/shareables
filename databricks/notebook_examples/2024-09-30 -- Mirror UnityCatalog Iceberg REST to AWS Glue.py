# Databricks notebook source
# MAGIC %md
# MAGIC # Mirror UnityCatalog Iceberg Tables to AWS Glue
# MAGIC Hey! Let's get right to it.
# MAGIC
# MAGIC First, some things to know:
# MAGIC - you'll need a single node databricks cluster for this. For some reason, serverless compute doesn't seem to connect to glue correctly
# MAGIC - next, you'll need to enable uniform on any UC tables you wish to mirror as this all works by making Glue think you have iceberg tables.
# MAGIC
# MAGIC
# MAGIC Sound good? Alright, hit the install below and add your own config values. 
# MAGIC
# MAGIC Happy mirroring!

# COMMAND ----------

# MAGIC %pip install pyiceberg[glue,s3]==0.7.1
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add your own configs below ⬇️

# COMMAND ----------

# get UC iceberg catalog details
UC_CATALOG_TO_MIRROR = 'analytics_prod'
UC_CREDENTIAL  = dbutils.secrets.get(scope="randy_pitcher_workspace", key="databricks_pat")
UC_DATABRICKS_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
UC_CATALOG_URI = f'https://{UC_DATABRICKS_URL}/api/2.1/unity-catalog/iceberg'

# get glue catalog details
GLUE_TARGET_DATABASE = 'databricks_uc_mirror'
AWS_REGION           = 'us-east-1'
GLUE_USER_SECRET_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_analytics_glue_user_secret_key")
GLUE_USER_ACCESS_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_analytics_glue_user_access_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time to go to work 💪

# COMMAND ----------

from pyiceberg.catalog import load_catalog

def migrate_iceberg_namespace_to_glue(iceberg_namespace, iceberg_catalog, glue_namespace, glue_catalog):
  glue_catalog.create_namespace_if_not_exists(glue_namespace)

  print(f"Found namespace to register: {iceberg_namespace}")
  tables_to_register = [table[1] for table in iceberg_catalog.list_tables(iceberg_namespace)]
  for table in tables_to_register:
    try:
      iceberg_table_name = f"{iceberg_namespace}.{table}"
      iceberg_table = iceberg_catalog.load_table(f"{iceberg_table_name}")

      glue_table_name =  f"{glue_namespace}.{iceberg_namespace}__{table}"  
      print(f"  - Registering iceberg table {iceberg_table_name} -> {glue_table_name}")
      glue_catalog.register_table(glue_table_name, iceberg_table.metadata_location)
      print(f"      ✅ Successfully registered {iceberg_table_name} -> {glue_table_name}")

    except Exception as e:
      print(f"      ❌ Failure while processing {iceberg_table_name} - {e}")


def migrate_iceberg_warehouse_to_glue(iceberg_warehouse_name, iceberg_catalog, glue_catalog):
  namespaces_to_register = [namespace[0] for namespace in iceberg_catalog.list_namespaces() if namespace[0] not in ['system', 'examples']]

  for namespace in namespaces_to_register:
    migrate_iceberg_namespace_to_glue(namespace, iceberg_catalog, GLUE_TARGET_DATABASE, glue_catalog)
    print('\n')


def main():
  # load UC iceberg catalog
  uc_catalog_properties = {
      'type':      'rest',
      'uri':       UC_CATALOG_URI,
      'token':     UC_CREDENTIAL,
      'warehouse': UC_CATALOG_TO_MIRROR
  }
  uc_catalog = load_catalog(**uc_catalog_properties)

  # load glue catalog
  glue_catalog_properties = {
      "type": "glue",
      "client.access-key-id": GLUE_USER_ACCESS_KEY,
      "client.secret-access-key": GLUE_USER_SECRET_KEY,
      "client.region": AWS_REGION
  }
  glue_catalog = load_catalog(**glue_catalog_properties)

  migrate_iceberg_warehouse_to_glue(UC_CATALOG_TO_MIRROR, uc_catalog, glue_catalog)

# COMMAND ----------

main()

# Databricks notebook source
# MAGIC %md
# MAGIC # 🪞 Mirror Tabular tables to Databricks Unity Catalog 
# MAGIC The purpose of this notebook is to be a schedulable resource for mirroring Tabular Iceberg tables into the Databricks Unity Catalog.
# MAGIC
# MAGIC This allows Databricks compute to access Tabular tables. Zero data is copied.
# MAGIC
# MAGIC Before you get started, you'll need:
# MAGIC - a tabular credential with read access to the warehouses/databases/tables that you want to mirror
# MAGIC - permission to register tables in Unity Catalog
# MAGIC - the name of your tabular warehouse. 
# MAGIC
# MAGIC Let's get after it 💪
# MAGIC <br/>
# MAGIC
# MAGIC <p align="center">
# MAGIC   <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/2/24/Lava_lamps_%2816136876840%29.jpg/2560px-Lava_lamps_%2816136876840%29.jpg" width="500">
# MAGIC </p>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧊 First, lets install some dependencies for iceberg

# COMMAND ----------

# MAGIC %pip install pyiceberg pyarrow
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚙️ Next, provide your configs below

# COMMAND ----------

# You'll need a tabular credential. Member credential or service account will work fine
TABULAR_CREDENTIAL       = dbutils.secrets.get(scope="tabular_production", key="tabular_credential")
TABULAR_TARGET_WAREHOUSE = dbutils.secrets.get(scope="tabular_production", key="tabular_warehouse")
TABULAR_CATALOG_URI      = 'https://api.tabular.io/ws' # unless you're a single tenant user, you don't need to change this

# configure tabular assets to mirror to unity catalog
TABULAR_DATABASES_TO_MIRROR = [ # will list the contents of these db's and mirror everything
  'dim',
  'finance'
]

TABULAR_TABLES_TO_EXCLUDE = { x.lower() for x in [ # exclude these tables. Must provide '{database}.{table}'. Case is ignored
  'finance.billing_usage_metrics',
]}

# which unity catalog catalog do you want this to live in (it has to already exist)
UNITY_CATALOG_NAME = 'tabular_product_raw'


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's go to work 💪

# COMMAND ----------

import os, time
from concurrent.futures import ThreadPoolExecutor

from pyiceberg.catalog import load_catalog

def get_tabular_catalog():
  catalog_properties = {
      'uri':        TABULAR_CATALOG_URI,
      'credential': TABULAR_CREDENTIAL,
      'warehouse':  TABULAR_TARGET_WAREHOUSE
  }
  
  catalog = load_catalog(**catalog_properties)
  
  return catalog


def get_iceberg_tables_to_mirror(catalog, tabular_databases):
  tables_to_mirror = []
  for db in tabular_databases:
    for _, tablename in catalog.list_tables(db):
      full_table_name = f"{db}.{tablename}".lower()
      if full_table_name in TABULAR_TABLES_TO_EXCLUDE:
        print(f"⛔ Skipping tabular table due to exclusion rule: '{full_table_name}'")
      else:
        tables_to_mirror.append(catalog.load_table(full_table_name))
        print(f"Found tabular table: '{full_table_name}'")
  
  return tables_to_mirror
  
  
def mirror_iceberg_to_unity(spark, table, unity_target):
  catalog, database, table_name = unity_target.split('.')
  
  create_db_query = f"""
  CREATE DATABASE IF NOT EXISTS {catalog}.{database}
  """
  spark.sql(create_db_query)
  
  check_table_exists_query = f"""
  SHOW TABLES IN {catalog}.{database} LIKE '{table_name}'
  """
  table_exists = spark.sql(check_table_exists_query).count() > 0
  
  if table_exists:
    update_table_query = f"REFRESH TABLE {unity_target} METADATA_PATH '{table.metadata_location}';"
    print(f"\nUpdating existing table with command:{update_table_query}")
    spark.sql(update_table_query)
  else:
    create_table_query = f"""
    CREATE TABLE {unity_target}
      UNIFORM iceberg
      METADATA_PATH '{table.metadata_location}';
    """
    print(f"\nCreating new mirror table with command:{create_table_query}")
    spark.sql(create_table_query)

def mirror_table(table_to_mirror):
  unity_catalog_target = f'{UNITY_CATALOG_NAME}.{table_to_mirror.identifier[1]}.{table_to_mirror.identifier[2]}'
  try:    
    mirror_iceberg_to_unity(spark, table_to_mirror, unity_catalog_target)
    print(f"✅ Success for '{unity_catalog_target}'!\n")
  except Exception as e:
    msg = f"❌ Failure. Unity Catalog mirroring error for table '{unity_catalog_target}':\n{e}"
    return msg

def main():
  tabular_catalog = get_tabular_catalog()
  tables_to_mirror = get_iceberg_tables_to_mirror(tabular_catalog, TABULAR_DATABASES_TO_MIRROR)
  
  exception_messages = []
  with ThreadPoolExecutor() as executor:
    results = executor.map(mirror_table, tables_to_mirror)
    for result in results:
      if result:
        exception_messages.append(result)
        print(result)

  if exception_messages:
    msg = '\n\t'.join(exception_messages)
    raise Exception(f"Registration encountered {len(exception_messages)} errors.\n {msg} ")

# COMMAND ----------

main()

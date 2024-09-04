# Databricks notebook source
# MAGIC %md
# MAGIC # 🪞 Register HMS Iceberg Tables to UC
# MAGIC The purpose of this notebook is to be a schedulable resource for mirroring Iceberg tables into the Databricks Unity Catalog.
# MAGIC
# MAGIC This allows Databricks compute to access Iceberg tables. Zero data is copied.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧊 First, lets install some dependencies for iceberg

# COMMAND ----------

# MAGIC %pip install pyiceberg[hive] pyarrow
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚙️ Next, provide your configs below

# COMMAND ----------

# configure HMS connectivity
HMS_URI = 'thrift://hive.server.host:9083' # replace this with your HMS URI

# configure iceberg assets to mirror to unity catalog
# will list the contents of these db's and mirror everything
ICEBERG_DATABASES_TO_MIRROR = [ 
  'finance',
  'business',
  'sales',
  'operations'
]

# exclude these tables. Must provide '{database}.{table}'. Case is ignored
ICBERG_TABLES_TO_EXCLUDE = { x.lower() for x in [ 
  'finance.billing_usage_metrics',
]}

# which unity catalog catalog do you want this to live in (it has to already exist)
UNITY_CATALOG_NAME = 'hms_iceberg_lake'


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's go to work 💪

# COMMAND ----------

import os, time
from concurrent.futures import ThreadPoolExecutor

from pyiceberg.catalog import load_catalog

def get_iceberg_catalog():
  catalog_properties = {
      'uri': HMS_URI,
  }

  catalog = load_catalog(**catalog_properties)

  return catalog


def get_iceberg_tables_to_mirror(catalog, iceberg_databases):
  tables_to_mirror = []
  for db in iceberg_databases:
    for _, tablename in catalog.list_tables(db):
      full_table_name = f"{db}.{tablename}".lower()
      if full_table_name in ICBERG_TABLES_TO_EXCLUDE:
        print(f"⛔ Skipping iceberg table due to exclusion rule: '{full_table_name}'")
      else:
        tables_to_mirror.append(catalog.load_table(full_table_name))
        print(f"Found iceberg table: '{full_table_name}'")
  
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
  iceberg_catalog = get_iceberg_catalog()
  tables_to_mirror = get_iceberg_tables_to_mirror(iceberg_catalog, ICEBERG_DATABASES_TO_MIRROR)
  
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

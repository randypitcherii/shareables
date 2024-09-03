# Databricks notebook source
# MAGIC %md
# MAGIC ### ❄️ First, lets install some dependencies for Snowflake communication

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚙️ Next, provide your configs below

# COMMAND ----------

# You'll need a tabular credential. Member credential or service account will work fine
SNOWFLAKE_ACCOUNT = 'wvb28439.us-east-1'
SNOWFLAKE_USERNAME = dbutils.secrets.get(scope="randy_pitcher_workspace", key="snowflake_username")
SNOWFLAKE_PASSWORD = dbutils.secrets.get(scope="randy_pitcher_workspace", key="snowflake_password")
SNOWFLAKE_WAREHOUSE = 'REVENUE_ANALYTICS_WH'
SNOWFLAKE_DATABASE = 'REVENUE_ANALYTICS_DEV' # This is just the initial database for the connection

# configure snowflake assets to mirror to unity catalog
SNOWFLAKE_DATABASES_TO_MIRROR = ( # will list the iceberg contents of these dbs and mirror what it finds
  'REVENUE_ANALYTICS_DEV',
  'UNITY_CATALOG'
)

# which unity catalog catalog do you want this to live in (it has to already exist)
UNITY_CATALOG_TARGET = 'snowflake_managed_iceberg'


# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's go to work 💪

# COMMAND ----------

import os
import time
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor

def get_snowflake_client():
  snowflake_configs = {
    'account':   SNOWFLAKE_ACCOUNT,
    'user':      SNOWFLAKE_USERNAME,
    'password':  SNOWFLAKE_PASSWORD,
    'warehouse': SNOWFLAKE_WAREHOUSE,
    'database':  SNOWFLAKE_DATABASE,
  }
  
  client = snowflake.connector.connect(**snowflake_configs)
  
  return client


def get_iceberg_tables_to_mirror(snowflake_client):
  cursor = snowflake_client.cursor()
  cursor.execute(f"""
    select 
        table_catalog || '.' || table_schema || '.' || table_name as full_table_name
    from 
      revenue_analytics_dev.information_schema.tables
    where 
        is_iceberg = 'YES'
        and table_catalog in {SNOWFLAKE_DATABASES_TO_MIRROR}
  """)
  results = cursor.fetchall()
  tables_to_mirror = [r[0] for r in results]
  
  return tables_to_mirror


def get_iceberg_metadata_location(snowflake_client, snowflake_full_table_name):
  cursor = snowflake_client.cursor()
  cursor.execute(f"""
    with metadata as (
      select parse_json(system$get_iceberg_table_information('{snowflake_full_table_name}'))
    )

    select $1:"metadataLocation"::string from metadata;
  """)
  results = cursor.fetchall()
  metadata_location = results[0][0]
  
  return metadata_location


def mirror_iceberg_to_unity(spark, snowflake_table_metadata_path, unity_target):
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
    update_table_query = f"REFRESH TABLE {unity_target} METADATA_PATH '{snowflake_table_metadata_path}';"
    print(f"\nUpdating existing table with command:{update_table_query}")
    spark.sql(update_table_query)
  else:
    create_table_query = f"""
    CREATE TABLE {unity_target}
      UNIFORM iceberg
      METADATA_PATH '{snowflake_table_metadata_path}';
    """
    print(f"\nCreating new mirror table with command:{create_table_query}")
    spark.sql(create_table_query)
  

def mirror_table(snowflake_table_dict):
  snowflake_table_name = snowflake_table_dict['table_name']
  snowflake_table_metadata_location = snowflake_table_dict['metadata_location']
  sf_db, sf_schema, sf_tablename = snowflake_table_name.split('.')
  unity_catalog_target = f'{UNITY_CATALOG_TARGET}.{sf_db}__{sf_schema}.{sf_tablename}'

  try:    
    mirror_iceberg_to_unity(spark, snowflake_table_metadata_location, unity_catalog_target)
    print(f"✅ Success for '{unity_catalog_target}'!\n")
  except Exception as e:
    msg = f"❌ Failure. Unity Catalog mirroring error for table '{unity_catalog_target}':\n{e}"
    return msg


def main():
  spark.sql(f'create catalog if not exists {UNITY_CATALOG_TARGET}')
  snowflake_client = get_snowflake_client()

  tables_to_mirror = tables_to_mirror = [
    {
      'table_name': table_name, 
      'metadata_location': get_iceberg_metadata_location(sf, table_name)
    } 
    for table_name in get_iceberg_tables_to_mirror(sf)
  ]

  
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

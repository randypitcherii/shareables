# Databricks notebook source
# MAGIC %md
# MAGIC # Mirror UnityCatalog Iceberg Tables to another Iceberg REST Catalog (including Apache Polaris)
# MAGIC Hey! Let's get right to it.
# MAGIC
# MAGIC First, some things to know:
# MAGIC - you'll need a single node databricks cluster for this.
# MAGIC - next, you'll need to enable uniform on any UC tables you wish to mirror as this all works with iceberg metadata
# MAGIC
# MAGIC
# MAGIC Sound good? Alright, suplex the installs below and add your own config values after that. Then you're ready to go 🤘 
# MAGIC
# MAGIC Happy mirroring!

# COMMAND ----------

# MAGIC %pip install pyiceberg[s3]==0.7.1
# MAGIC %pip install --upgrade pyparsing
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add your own configs below ⬇️

# COMMAND ----------

# get UC iceberg catalog details
UC_CATALOG_TO_MIRROR = 'polaris_sharing'
UC_CREDENTIAL  = dbutils.secrets.get(scope="randy_pitcher_workspace", key="databricks_pat")
UC_DATABRICKS_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
UC_CATALOG_URI = f'https://{UC_DATABRICKS_URL}/api/2.1/unity-catalog/iceberg'

# get rest catalog details
REST_TARGET_WAREHOUSE = 'databricks_uc_mirror_s3'
REST_CATALOG_URI = 'https://wgb31150.us-east-1.snowflakecomputing.com/polaris/api/catalog'
REST_CATALOG_CREDENTIAL = dbutils.secrets.get(scope="randy_pitcher_workspace", key="polaris_credential")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time to go to work 💪

# COMMAND ----------

from pyiceberg.catalog import load_catalog

def migrate_uc_namespace_to_target_catalog(uc_namespace, uc_catalog, target_database, target_catalog):
  target_catalog.create_namespace_if_not_exists(target_database)

  print(f"Found namespace to register: {uc_namespace}")
  tables_to_register = [table[1] for table in uc_catalog.list_tables(uc_namespace)]
  for table in tables_to_register:
    try:
      iceberg_table_name = f"{uc_namespace}.{table}"
      iceberg_table = uc_catalog.load_table(f"{iceberg_table_name}")

      target_table_name =  f"{target_database}.{table}"  
      print(f"  - Registering iceberg table {iceberg_table_name} -> {target_table_name}")
      if target_catalog.table_exists(target_table_name):
        target_catalog.drop_table(target_table_name)
      target_catalog.register_table(target_table_name, iceberg_table.metadata_location)
      print(f"      ✅ Successfully registered {iceberg_table_name} -> {target_table_name}")

    except Exception as e:
      print(f"      ❌ Failure while processing {iceberg_table_name} - {e}")


def migrate_uc_catalog_to_target_catalog(uc_catalog, target_catalog):
  namespaces_to_register = [namespace[0] for namespace in uc_catalog.list_namespaces() if namespace[0] not in ['system', 'examples']]

  for namespace in namespaces_to_register:
    migrate_uc_namespace_to_target_catalog(namespace, uc_catalog, namespace, target_catalog)
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

  # load target catalog
  target_catalog_properties = {
      'type':           'rest',
      'uri':            REST_CATALOG_URI,
      'credential':     REST_CATALOG_CREDENTIAL,
      'warehouse':      REST_TARGET_WAREHOUSE,
      'scope':          'PRINCIPAL_ROLE:ALL' # this is required for polaris integration
  }
  target_catalog = load_catalog(**target_catalog_properties)

  migrate_uc_catalog_to_target_catalog(uc_catalog, target_catalog)

# COMMAND ----------

main()

# COMMAND ----------

polaris_properties = {
  'type':           'rest',
  'uri':            REST_CATALOG_URI,
  'credential':     REST_CATALOG_CREDENTIAL,
  'warehouse':      REST_TARGET_WAREHOUSE,
  'scope':          'PRINCIPAL_ROLE:ALL' # this is required for polaris integration
}
polaris = load_catalog(**polaris_properties)

# COMMAND ----------

polaris.list_namespaces()

polaris_table = polaris.load_table('raw.hello_polaris')
arrow_table = polaris_table.scan().to_arrow()

df = spark.createDataFrame(arrow_table.to_pandas())

display(df)

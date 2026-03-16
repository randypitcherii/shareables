# Databricks notebook source
# MAGIC %md
# MAGIC ### ⚙️ Provide your configs below

# COMMAND ----------

# provide a CSV path and/or a list of tables to refresh. 
# Format is expected to be no header with fully qualified UC table names (catalog.schema.table)
UC_TABLES_TO_REFRESH_CSV_PATH = './tables_to_refresh.csv' # default to none, but just provide a path and we'll figure it out
UC_TABLES_TO_REFRESH = [
  'analytics_dev.dbt_randy_pitcher.space_stuff'
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's go to work 💪

# COMMAND ----------

def get_uc_tables_to_refresh(tables_to_refresh_list, csv_path):
  tables_to_refresh = []

  if tables_to_refresh_list:
    tables_to_refresh += tables_to_refresh_list

  if csv_path:
    print(f'Loading tables to refresh from CSV path: {csv_path}')
    with open(csv_path, 'r') as f:
      tables_to_refresh += [line.strip() for line in f.read().splitlines()]

  return tables_to_refresh


def refresh_uc_table_uniform_metadata(spark, table_name):
  print('\n♻️ Refreshing table: {table_name}')
  try:
    spark.sql(f'MSCK REPAIR TABLE {table_name} SYNC METADATA')
    print(f'  ✅ {table_name} refreshed')
  except Exception as e:
    print(f'  ❌ Error refreshing table: {table_name}, {e}')



def main():
  tables_to_refresh = get_uc_tables_to_refresh(UC_TABLES_TO_REFRESH, UC_TABLES_TO_REFRESH_CSV_PATH)

  for table_name in tables_to_refresh:
    refresh_uc_table_uniform_metadata(spark, table_name)

# COMMAND ----------

main()

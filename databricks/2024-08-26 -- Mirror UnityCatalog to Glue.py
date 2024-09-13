# Databricks notebook source
# get glue catalog details
AWS_REGION           = 'us-east-1'
GLUE_USER_SECRET_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_analytics_glue_user_secret_key")
GLUE_USER_ACCESS_KEY = dbutils.secrets.get("randy_pitcher_workspace", "tabular_aws_analytics_glue_user_access_key")

# set configs
GLUE_TARGET_DATABASE = 'unity_catalog_mirror'
UC_NAMESPACES_TO_MIRROR = ['analytics_prod.mart']
SHOULD_PREFIX_TABLE_NAMES = False # if true, will prefix table names with the namespace


# COMMAND ----------

import boto3
from botocore.exceptions import ClientError
from pprint import pprint
import time

glue_client = boto3.client(
  'glue', 
  region_name=AWS_REGION, 
  aws_access_key_id=GLUE_USER_ACCESS_KEY, 
  aws_secret_access_key=GLUE_USER_SECRET_KEY
)



# COMMAND ----------

def create_glue_database_if_not_exists(database_name):
  try:
    glue_client.get_database(Name=database_name)
    print(f"ℹ️ Glue database '{database_name}' already exists.")
  except glue_client.exceptions.EntityNotFoundException:
    try:
      glue_client.create_database(
        DatabaseInput={
          'Name': database_name,
          'Description': f'Database created to mirror UnityCatalog delta lake tables to Glue'
        }
      )
      print(f"✅ Glue database '{database_name}' created successfully.")
    except ClientError as e:
      print(f"❌ Error creating database: {e}")
  except ClientError as e:
    print(f"❌ Error checking database existence: {e}")

def register_table_in_glue(uc_table, glue_database):
    if SHOULD_PREFIX_TABLE_NAMES:
        table_name = f"{uc_table.namespace[0]}_{uc_table.name}"
    else:
        table_name = uc_table.name

    # Get table location
    location_df = spark.sql(f"describe formatted {uc_table.catalog}.{uc_table.namespace[0]}.{uc_table.name}")
    location_row = location_df.filter("col_name='Location'").collect()
    
    if not location_row:
        print(f"❌ Error: Could not find location for table {uc_table.name}")
        return
    
    table_location = location_row[0].data_type

    # Get table schema
    schema = spark.table(f"{uc_table.catalog}.{uc_table.namespace[0]}.{uc_table.name}").schema
    columns = [{"Name": field.name, "Type": field.dataType.simpleString()} for field in schema.fields]

    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': table_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.delta.DeltaInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.delta.DeltaOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.delta.DeltaSerDe',
                'Parameters': {
                    'delta.compatibility.version': '2'
                }
            }
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'table_type': 'DELTA',
            'delta.minReaderVersion': '1',
            'delta.minWriterVersion': '2',
            'classification': 'delta',
            'has_encrypted_data': 'false',
            'parquet.compression': 'SNAPPY'
        }
    }

    try:
        # Check if the table already exists
        try:
            glue_client.get_table(DatabaseName=glue_database, Name=table_name)
            # Table exists, update it
            glue_client.update_table(DatabaseName=glue_database, TableInput=table_input)
            print(f"✅ Table '{table_name}' updated successfully in Glue database '{glue_database}'")
        except glue_client.exceptions.EntityNotFoundException:
            # Table doesn't exist, create it
            glue_client.create_table(DatabaseName=glue_database, TableInput=table_input)
            print(f"✅ Table '{table_name}' created successfully in Glue database '{glue_database}'")
    except ClientError as e:
        print(f"❌ Error registering/updating table '{table_name}': {e}")


def main():
  # Create the target Glue database
  create_glue_database_if_not_exists(GLUE_TARGET_DATABASE)

  # Register tables from each namespace
  for namespace in UC_NAMESPACES_TO_MIRROR:
    tables_to_register = [tb for tb in spark.catalog.listTables(namespace) if tb.tableType == "MANAGED"]
    for table in tables_to_register:
      print(f'\n🔎 found table {table.catalog}.{table.namespace[0]}.{table.name} to mirror')
      register_table_in_glue(table, GLUE_TARGET_DATABASE)

  print("\n💪 Table registration process completed. 🌞")


# COMMAND ----------

main()

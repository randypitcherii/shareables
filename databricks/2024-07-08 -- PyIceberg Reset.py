# Databricks notebook source
# MAGIC %pip install pyarrow "git+https://github.com/apache/iceberg-python.git#egg=pyiceberg" 
# MAGIC %pip install --upgrade pyparsing 

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os, time, pprint

import pyarrow as pa
from pyiceberg.catalog import load_catalog

# You'll need a tabular credential. Member credential or service account will work fine
TABULAR_CREDENTIAL       = dbutils.secrets.get(scope="randy_pitcher_workspace_tabular", key="tabular_credential")
TABULAR_TARGET_WAREHOUSE = 'rpw_aws_us_east_1' # replace this with your tabular warehouse name
TABULAR_CATALOG_URI      = 'https://api.tabular.io/ws' # unless you're a single tenant user, you don't need to change this

catalog_properties = {
    'uri':        TABULAR_CATALOG_URI,
    'credential': TABULAR_CREDENTIAL,
    'warehouse':  TABULAR_TARGET_WAREHOUSE
}
catalog = load_catalog(**catalog_properties)

# COMMAND ----------

def get_kafka_props_from_latest_snapshot_properties(snapshot_properties):
    kafka_props = []
    
    for key in snapshot_properties:
        if key.startswith('kafka'):
            kafka_props.append(key)
    
    return kafka_props


def print_latest_snapshot_properties(catalog, table_name: str):
    """
    Prints the properties of the latest snapshot of the given table.

    :param catalog: The catalog object
    :param table_name: Name of the table
    """
    # Load the table
    table = catalog.load_table(table_name)

    # Get the current snapshot
    latest_snapshot = table.current_snapshot()

    # Access the properties of the latest snapshot
    snapshot_properties = latest_snapshot.dict()['summary']
    kafka_props = get_kafka_props_from_latest_snapshot_properties(snapshot_properties)

    # Print the snapshot properties
    if kafka_props:
        print(f'Found kafka connect properties in latest snapshot:')
        for kafka_prop in kafka_props:
            print(f'\t{kafka_prop}:\t{snapshot_properties[kafka_prop]}')
    else:
        print('No kafka connect snapshot properties found. Full properties are:')
        pprint.pprint(snapshot_properties)

# COMMAND ----------

# get the table
table_name = 'default.solar_system'
table = catalog.load_table(table_name)

# modify the snapshot props
snapshot_props = table.current_snapshot().dict()['summary']
new_props = {'kafka-offset-to-reset': "{'0': '123', '1': '123'}"}

# get a schema-compliant but empty arrow DF to append with. Thanks, @Fokko!!
arrow_schema = table.schema().as_arrow()

# try to append 
table.append(arrow_schema.empty_table(), snapshot_properties=new_props)
